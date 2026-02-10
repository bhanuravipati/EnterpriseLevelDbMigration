"""
Error Fixer Agent - Analyzes sandbox errors and uses LLM to fix DDL.
Uses dependency graph as context and handles circular dependencies.
"""

import json
import re
from pathlib import Path
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_groq import ChatGroq

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase, MigrationStatus, SandboxResult
from src.tools.artifact_manager import get_artifact_manager
from src.config import get_settings


# System prompt for error analysis and fixing
ERROR_FIXER_SYSTEM_PROMPT = """You are an expert PostgreSQL Database Engineer. Your job is to analyze SQL execution errors and fix them.

You will receive:
1. The original SQL statement that failed
2. The exact error message from PostgreSQL
3. The object type (table, view, procedure, trigger)
4. The dependency graph showing FK relationships

## Common PostgreSQL Errors and Fixes:

### Duplicate Index Names (VERY COMMON!)
- ERROR: "relation idx_fk_customer_id already exists"
- CAUSE: Index names must be unique across the ENTIRE database
- FIX: Rename index to include table name as prefix
  - WRONG: CREATE INDEX "idx_fk_customer_id" ON "rental" (...)
  - CORRECT: CREATE INDEX "idx_rental_customer_id" ON "rental" (...)

### GROUP BY Errors
- ERROR: "column X must appear in the GROUP BY clause or be used in an aggregate function"
- FIX: Add the missing column to the GROUP BY clause
  - Example: If error mentions "m.first_name", add m.first_name to GROUP BY

### Table Name Typos
- ERROR: "relation X does not exist" where X is a typo
- FIX: Check for common typos in Sakila database (ALWAYS use singular names):
  - "staffs" → "staff"
  - "payments" → "payment"
  - "customers" → "customer"
  - "rentals" → "rental"
  - "inventories" → "inventory"

### Index Errors (IMPORTANT!)
- **POINT/GEOMETRY columns MUST use GIST index**, not btree:
  - ERROR: "data type point has no default operator class for access method btree"
  - WRONG: CREATE INDEX "idx_location" ON "address" ("location");
  - CORRECT: CREATE INDEX "idx_address_location" ON "address" USING GIST ("location");

### Circular Dependency Errors
- When staff → store and store → staff both have FKs
- SOLUTION: Remove ONE of the circular FK constraints from CREATE TABLE
- Add it later via ALTER TABLE after both tables exist

### Data Type Errors
- "type X does not exist" → Convert to valid PostgreSQL type
- UNSIGNED → Remove, use larger type if needed

### Syntax Errors
- Replace backticks with double quotes
- Remove ENGINE=InnoDB, CHARSET, COLLATE clauses
- Convert AUTO_INCREMENT to SERIAL/BIGSERIAL

### Function/Procedure Errors
- "function result type must be X because of OUT parameters" → Change RETURNS void to RETURNS INTEGER

### Aggregate Errors
- "ORDER BY in aggregate with DISTINCT" → Remove ORDER BY from inside DISTINCT aggregate

## Output Format
Return ONLY the corrected SQL statement.
Do NOT include explanations or markdown code blocks.
Just the raw SQL.
"""


class ErrorFixerAgent(BaseAgent):
    """
    Agent that analyzes sandbox errors and uses LLM to fix them.
    Uses dependency graph for context on circular dependencies.
    Produces: Updated DDL in state.transformed_ddl
    """
    
    def __init__(self):
        super().__init__(
            name="Error Fixer Agent",
            description="Analyzes sandbox errors and fixes SQL using LLM",
            use_complex_model=True,
            system_prompt=ERROR_FIXER_SYSTEM_PROMPT
        )
        self.artifact_manager = get_artifact_manager()
        
        # Initialize LLM using config (uses LLM_MODEL_COMPLEX from .env)
        settings = get_settings()
        self._llm = ChatGroq(
            api_key=settings.llm.groq_api_key,
            model=settings.llm.llm_model_complex,  # From .env: LLM_MODEL_COMPLEX
            temperature=settings.llm.temperature,
            max_tokens=settings.llm.max_tokens,
        )
        
        # Load dependency graph
        self.dependency_graph = self._load_dependency_graph()
    
    def _load_dependency_graph(self) -> dict:
        """Load dependency graph from artifacts."""
        try:
            dep_path = self.artifact_manager.artifacts_dir / "dependency_graph.json"
            if dep_path.exists():
                with open(dep_path) as f:
                    return json.load(f)
        except Exception as e:
            self.log(f"Could not load dependency graph: {e}", "warning")
        return {"nodes": [], "edges": [], "migration_order": []}
    
    def _get_circular_dependencies(self) -> list:
        """Find circular dependencies from the graph."""
        edges = self.dependency_graph.get("edges", [])
        circular_pairs = []
        
        # Find pairs where A→B and B→A both exist
        edge_set = {(e["from_id"], e["to_id"]) for e in edges if e["edge_type"] == "foreign_key"}
        
        for from_id, to_id in edge_set:
            if (to_id, from_id) in edge_set:
                # Found circular dependency
                pair = tuple(sorted([from_id, to_id]))
                if pair not in circular_pairs:
                    circular_pairs.append(pair)
        
        return circular_pairs
    
    def _get_fk_context(self, table_name: str) -> str:
        """Get FK relationships for a table from dependency graph."""
        edges = self.dependency_graph.get("edges", [])
        table_id = f"table:{table_name}"
        
        # Find tables this table depends on (FK references)
        depends_on = []
        for edge in edges:
            if edge["from_id"] == table_id and edge["edge_type"] == "foreign_key":
                ref_table = edge["to_id"].replace("table:", "")
                depends_on.append(ref_table)
        
        # Find tables that depend on this table
        depended_by = []
        for edge in edges:
            if edge["to_id"] == table_id and edge["edge_type"] == "foreign_key":
                from_table = edge["from_id"].replace("table:", "")
                depended_by.append(from_table)
        
        context = f"Table '{table_name}' FK dependencies:\n"
        context += f"  - References (FK to): {', '.join(depends_on) if depends_on else 'None'}\n"
        context += f"  - Referenced by: {', '.join(depended_by) if depended_by else 'None'}\n"
        
        return context
    
    def run(self, state: MigrationState) -> MigrationState:
        """Analyze and fix all sandbox errors."""
        
        # CRITICAL: Increment retry counter HERE (not in routing function)
        # LangGraph only persists state changes from nodes, not routing functions
        state.sandbox_retry_count = state.sandbox_retry_count + 1
        state.current_retry_count = state.sandbox_retry_count
        
        self.log(f"Error Fixer (retry {state.sandbox_retry_count}/3)...")
        
        # Get failed results
        failed_results = [r for r in state.sandbox_results if not r.executed]
        
        if not failed_results:
            self.log("No errors to fix!", "success")
            return state
        
        self.log(f"Found {len(failed_results)} failed objects to fix")
        
        # Find circular dependencies
        circular_deps = self._get_circular_dependencies()
        if circular_deps:
            self.log(f"Detected circular dependencies: {circular_deps}")
        
        fixes_applied = 0
        
        # All errors are now fixable - we send them all to LLM with context
        for result in failed_results:
            success = self._fix_error_with_context(result, state, circular_deps)
            if success:
                fixes_applied += 1
        
        self.log(f"Applied {fixes_applied} fixes using LLM", "success")
        
        # Update state
        state.current_phase = MigrationPhase.SCHEMA_TRANSFORMATION
        
        return state
    
    def _fix_error_with_context(self, result: SandboxResult, state: MigrationState, circular_deps: list) -> bool:
        """Fix error using LLM with dependency context."""
        self.log(f"Fixing {result.object_type}: {result.object_name}")
        
        # Find the original DDL
        original_ddl = None
        ddl_obj = None
        
        for ddl in state.transformed_ddl:
            if ddl.object_name == result.object_name:
                original_ddl = ddl.target_ddl
                ddl_obj = ddl
                break
        
        if not original_ddl:
            # Check procedures
            for proc in state.converted_procedures:
                if proc.name == result.object_name:
                    original_ddl = proc.target_code
                    ddl_obj = proc
                    break
        
        if not original_ddl:
            self.log(f"  ✗ Could not find original DDL for {result.object_name}", "warning")
            return False
        
        error_text = result.errors[0] if result.errors else "Unknown error"
        
        # Get FK context for tables
        fk_context = ""
        if result.object_type == "table":
            fk_context = self._get_fk_context(result.object_name)
        
        # Check if this is part of a circular dependency
        is_circular = False
        for pair in circular_deps:
            if f"table:{result.object_name}" in pair:
                is_circular = True
                break
        
        circular_note = ""
        if is_circular:
            circular_note = """
⚠️ CIRCULAR DEPENDENCY DETECTED!
This table is part of a circular FK relationship (e.g., staff→store and store→staff).
To fix: REMOVE the Foreign Key constraint that references the missing table.
The FK can be added later via ALTER TABLE after both tables exist.
"""
        
        # Build prompt for LLM
        prompt = f"""Fix this PostgreSQL DDL that failed with an error:

## Object Type: {result.object_type}
## Object Name: {result.object_name}

## Error Message:
{error_text[:800]}

{fk_context}
{circular_note}

## Original SQL:
{original_ddl}

Instructions:
1. If error mentions POINT data type and btree index, change to GIST index
2. If error is "relation X does not exist", REMOVE the FOREIGN KEY constraint referencing that table
3. Fix any syntax issues

Return ONLY the corrected SQL statement."""

        messages = [
            SystemMessage(content=ERROR_FIXER_SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ]
        
        try:
            # Use invoke_with_retry for automatic API key rotation on rate limits
            # Token tracking is handled automatically by invoke_with_retry
            response = self.invoke_with_retry(messages)
            
            fixed_ddl = self._clean_sql(response.content)
            
            # Check if LLM actually changed something
            if fixed_ddl != original_ddl:
                # Update the DDL in state
                if hasattr(ddl_obj, 'target_ddl'):
                    ddl_obj.target_ddl = fixed_ddl
                elif hasattr(ddl_obj, 'target_code'):
                    ddl_obj.target_code = fixed_ddl
                
                # Save updated DDL
                if result.object_type == "table":
                    self.artifact_manager.save_table_ddl(result.object_name, fixed_ddl)
                elif result.object_type == "view":
                    self.artifact_manager.save_sql(
                        fixed_ddl,
                        f"{result.object_name}.sql",
                        subdir="ddl/views",
                        header_comment=f"Fixed view: {result.object_name}"
                    )
                elif result.object_type in ["procedure", "function", "trigger"]:
                    self.artifact_manager.save_sql(
                        fixed_ddl,
                        f"{result.object_name}.sql",
                        subdir="procedures",
                        header_comment=f"Fixed {result.object_type}: {result.object_name}"
                    )
                
                self.log(f"  ✓ Fixed {result.object_name}")
                return True
            else:
                self.log(f"  - No changes made for {result.object_name}")
                return False
                
        except Exception as e:
            self.log(f"  ✗ LLM error: {str(e)[:100]}", "warning")
            return False
    
    def _clean_sql(self, sql: str) -> str:
        """Clean up LLM output."""
        sql = sql.strip()
        
        # Remove markdown code blocks
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        
        sql = sql.strip()
        
        # Ensure ends with semicolon
        if sql and not sql.endswith(";"):
            sql += ";"
        
        return sql


def error_fixer_node(state: dict) -> dict:
    """LangGraph node function for error fixing."""
    agent = ErrorFixerAgent()
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
