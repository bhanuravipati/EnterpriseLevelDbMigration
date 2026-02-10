"""
Schema Agent - Transforms MySQL DDL to PostgreSQL DDL using LLM.
Uses the 120b model for accurate SQL translation with metadata context.
"""

import json
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase, MigrationStatus, TransformedDDL
from src.tools.artifact_manager import get_artifact_manager
from src.config import get_settings


# System prompt for schema transformation - TWO-PASS FK AND INDEX APPROACH
SCHEMA_AGENT_SYSTEM_PROMPT = """You are an expert Database Migration Engineer specializing in MySQL to PostgreSQL migrations.

Your task is to convert MySQL CREATE TABLE statements to PostgreSQL syntax.

## ⚠️ CRITICAL: TWO-PASS APPROACH FOR FK AND INDEXES ⚠️

**DO NOT include ANY of the following in the CREATE TABLE statement:**
1. FOREIGN KEY constraints - Added later via ALTER TABLE
2. CREATE INDEX statements - Generated separately after tables

Only output the CREATE TABLE statement with:
- Column definitions
- PRIMARY KEY
- UNIQUE constraints
- CHECK constraints
- DEFAULT values

✅ CORRECT OUTPUT (TABLE ONLY, NO INDEXES):
CREATE TABLE "rental" (
    "rental_id" SERIAL PRIMARY KEY,
    "customer_id" SMALLINT NOT NULL,
    "staff_id" SMALLINT NOT NULL,
    "last_update" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

❌ WRONG (DO NOT INCLUDE INDEXES OR FKs):
CREATE TABLE "rental" (...);
CREATE INDEX "idx_rental_customer_id" ON "rental" (...);  -- DON'T INCLUDE!
FOREIGN KEY (...)  -- DON'T INCLUDE!

## Key Conversion Rules:

### Data Types
| MySQL | PostgreSQL |
|-------|------------|
| TINYINT | SMALLINT |
| TINYINT(1) | BOOLEAN |
| SMALLINT UNSIGNED | INTEGER |
| INT / INTEGER | INTEGER |
| INT UNSIGNED | BIGINT |
| BIGINT | BIGINT |
| BIGINT UNSIGNED | NUMERIC(20) |
| FLOAT | REAL |
| DOUBLE | DOUBLE PRECISION |
| DECIMAL(p,s) | NUMERIC(p,s) |
| VARCHAR(n) | VARCHAR(n) |
| CHAR(n) | CHAR(n) |
| TEXT / TINYTEXT / MEDIUMTEXT / LONGTEXT | TEXT |
| BLOB / TINYBLOB / MEDIUMBLOB / LONGBLOB | BYTEA |
| DATETIME | TIMESTAMP |
| TIMESTAMP | TIMESTAMP |
| DATE | DATE |
| TIME | TIME |
| YEAR | SMALLINT |
| JSON | JSONB |
| ENUM(...) | TEXT (with CHECK constraint) |
| SET(...) | TEXT[] |
| BIT(n) | BIT(n) |
| GEOMETRY / POINT | POINT |

### Syntax Differences
1. **AUTO_INCREMENT** → Use SERIAL (for INT) or BIGSERIAL (for BIGINT)
2. **Backticks** → Use double quotes for identifiers
3. **ENGINE=InnoDB** → Remove (PostgreSQL doesn't use this)
4. **CHARSET/COLLATE** → Remove (PostgreSQL handles encoding differently)
5. **UNSIGNED** → Remove and use larger type if needed
6. **ON UPDATE CURRENT_TIMESTAMP** → Requires trigger (note this)
7. **COMMENT** → Use COMMENT ON TABLE/COLUMN syntax separately

### What to Include in CREATE TABLE
- ✅ PRIMARY KEY - Include
- ✅ UNIQUE constraints - Include
- ✅ CHECK constraints - Include
- ✅ DEFAULT values - Include
- ❌ FOREIGN KEY - DO NOT include!
- ❌ CREATE INDEX - DO NOT include!

### Table Names (Sakila Database - CRITICAL!)
- ALL table names are SINGULAR, never plural!
- Use "payment" NOT "payments"
- Use "rental" NOT "rentals"
- Use "staff" NOT "staffs"

- Use "customer" NOT "customers"
- Use "inventory" NOT "inventories"

## Output Format
Return ONLY the PostgreSQL CREATE TABLE statement (WITHOUT foreign keys).
Do NOT include explanations, comments, or markdown code blocks.
Just the raw SQL statement ending with semicolon.
"""


class SchemaAgent(BaseAgent):
    """
    Agent responsible for transforming MySQL schema to PostgreSQL using LLM.
    Uses the 120b model for accurate and complex SQL translation.
    Produces: ddl/*.sql artifacts
    """
    
    def __init__(self):
        super().__init__(
            name="Schema Transformation Agent",
            description="Converts MySQL table DDL to PostgreSQL syntax using AI",
            use_complex_model=True,  # Uses 120b model
            system_prompt=SCHEMA_AGENT_SYSTEM_PROMPT
        )
        self.artifact_manager = get_artifact_manager()
        
        # Force use of the 120b model for this agent
        settings = get_settings()
        self._llm = ChatGroq(
            api_key=settings.llm.groq_api_key,
            model="openai/gpt-oss-120b",  # Explicitly use 120b
            temperature=0.1,  # Low temperature for consistent SQL
            max_tokens=4096,
        )
    
    def run(self, state: MigrationState) -> MigrationState:
        """Transform all table/view DDLs using LLM with blueprint context."""
        self.log("Starting LLM-based schema transformation with blueprints...")
        
        if not state.schema_metadata:
            self.log("No schema metadata found!", "error")
            return state
        
        try:
            schema = state.schema_metadata
            transformed_ddls: list[TransformedDDL] = []
            
            # Clear existing DDLs to avoid stale data
            self._clear_ddl_dir()
            
            # Load blueprints directory
            blueprints_dir = self.artifact_manager.artifacts_dir / "blueprints"
            
            # Process tables with blueprint context
            for table in schema.tables:
                self.log(f"Transforming table: {table.name}")
                
                # Load blueprint for this table
                blueprint = self._load_blueprint(blueprints_dir, table.name)
                
                # Build context from blueprint (richer context!)
                if blueprint:
                    metadata_context = self._build_blueprint_context(blueprint)
                else:
                    # Fallback to basic metadata
                    metadata_context = self._build_metadata_context(table)
                
                # Use LLM to generate PostgreSQL DDL
                pg_ddl = self._llm_convert_table(table.name, metadata_context)
                
                # Clean up output
                pg_ddl = self._clean_sql_output(pg_ddl)
                
                # Save SQL artifact
                file_path = self.artifact_manager.save_table_ddl(table.name, pg_ddl)
                
                # Create TransformedDDL record
                transformed = TransformedDDL(
                    object_name=table.name,
                    object_type="table",
                    source_ddl=metadata_context,
                    target_ddl=pg_ddl,
                    type_mappings=[{"method": "LLM+Blueprint", "model": "openai/gpt-oss-120b"}],
                    file_path=str(file_path),
                    status=MigrationStatus.PENDING,
                )
                transformed_ddls.append(transformed)
                
                self.log(f"  ✓ Saved to {file_path}")
            
            # Process views
            for view in schema.views:
                self.log(f"Transforming view: {view.name}")
                
                pg_ddl = self._llm_convert_view(view)
                pg_ddl = self._clean_sql_output(pg_ddl)
                
                file_path = self.artifact_manager.save_sql(
                    pg_ddl, 
                    f"{view.name}.sql", 
                    subdir="ddl/views",
                    header_comment=f"View: {view.name}"
                )
                
                transformed = TransformedDDL(
                    object_name=view.name,
                    object_type="view",
                    source_ddl=view.definition,
                    target_ddl=pg_ddl,
                    type_mappings=[{"method": "LLM", "model": "openai/gpt-oss-120b"}],
                    file_path=str(file_path),
                    status=MigrationStatus.PENDING,
                )
                transformed_ddls.append(transformed)
            
            # Generate ALTER TABLE statements for deferred (circular) FKs
            deferred_fks_sql = self._generate_deferred_fks(blueprints_dir)
            if deferred_fks_sql:
                self.log(f"Generated {len(deferred_fks_sql)} deferred FK statements")
                
                # Save deferred FKs as a single file
                all_deferred = "\n\n".join(deferred_fks_sql)
                deferred_path = self.artifact_manager.save_sql(
                    all_deferred,
                    "deferred_fks.sql",
                    subdir="ddl",
                    header_comment="Deferred Foreign Keys (circular dependencies)"
                )
                
                # Add as a special DDL entry
                transformed = TransformedDDL(
                    object_name="_deferred_fks",
                    object_type="constraint",  # Special type for deferred FKs
                    source_ddl="Circular FK dependencies",
                    target_ddl=all_deferred,
                    type_mappings=[{"method": "generated"}],
                    file_path=str(deferred_path),
                    status=MigrationStatus.PENDING,
                )
                transformed_ddls.append(transformed)
            
            # Generate CREATE INDEX statements (Two-Pass Index approach)
            indexes_sql = self._generate_indexes(blueprints_dir)
            if indexes_sql:
                self.log(f"Generated {len(indexes_sql)} index statements")
                
                # Save indexes as a single file
                all_indexes = "\n".join(indexes_sql)
                indexes_path = self.artifact_manager.save_sql(
                    all_indexes,
                    "indexes.sql",
                    subdir="ddl",
                    header_comment="Database Indexes (Two-Pass approach)"
                )
                
                # Add as a special DDL entry
                transformed = TransformedDDL(
                    object_name="_indexes",
                    object_type="index",  # Special type for indexes
                    source_ddl="All table indexes",
                    target_ddl=all_indexes,
                    type_mappings=[{"method": "generated"}],
                    file_path=str(indexes_path),
                    status=MigrationStatus.PENDING,
                )
                transformed_ddls.append(transformed)
            
            # Save summary
            ddl_summary = {
                "method": "LLM-only (openai/gpt-oss-120b)",
                "tables": len([d for d in transformed_ddls if d.object_type == "table"]),
                "views": len([d for d in transformed_ddls if d.object_type == "view"]),
                "deferred_fks": len(deferred_fks_sql) if deferred_fks_sql else 0,
                "indexes": len(indexes_sql) if indexes_sql else 0,
                "transformations": [d.model_dump() for d in transformed_ddls],
            }
            self.artifact_manager.save_json(ddl_summary, "transformed_ddl.json")
            
            # Update state
            state.transformed_ddl = transformed_ddls
            state.current_phase = MigrationPhase.SCHEMA_TRANSFORMATION
            state.artifact_paths["transformed_ddl"] = str(
                self.artifact_manager.artifacts_dir / "transformed_ddl.json"
            )
            
            self.log(f"Transformed {len(transformed_ddls)} objects using LLM", "success")
            
        except Exception as e:
            self.log(f"Schema transformation failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.SCHEMA_TRANSFORMATION,
                "error_type": "transformation_error",
                "error_message": str(e)
            })
        
        return state
    
    def _load_blueprint(self, blueprints_dir, table_name: str) -> dict | None:
        """Load blueprint JSON for a table."""
        import json
        try:
            blueprint_path = blueprints_dir / f"{table_name}.blueprint.json"
            if blueprint_path.exists():
                with open(blueprint_path) as f:
                    return json.load(f)
        except Exception as e:
            self.log(f"Could not load blueprint for {table_name}: {e}", "warning")
        return None
    
    def _build_blueprint_context(self, blueprint: dict) -> str:
        """Build rich context from blueprint (much more context than basic metadata)."""
        table_name = blueprint.get("table_name", "unknown")
        
        context = f"""## Table: {table_name}

### All Tables in Database (for reference):
{', '.join(blueprint.get('all_tables_in_database', []))}

### Columns:
"""
        for col in blueprint.get("schema", {}).get("columns", []):
            col_info = f"- `{col['name']}`: {col['mysql_type']}"
            if not col.get('nullable', True):
                col_info += " NOT NULL"
            if col.get('autoincrement'):
                col_info += " AUTO_INCREMENT"
            if col.get('default'):
                col_info += f" DEFAULT {col['default']}"
            context += col_info + "\n"
        
        pk = blueprint.get("schema", {}).get("primary_key", [])
        if pk:
            context += f"\n### Primary Key: {', '.join(pk)}\n"
        
        # Indexes with table-specific naming
        indexes = blueprint.get("indexes", [])
        if indexes:
            context += f"\n### Indexes (IMPORTANT: use idx_{table_name}_<column> format!):\n"
            for idx in indexes:
                idx_type = "UNIQUE " if idx.get('unique') else ""
                context += f"- {idx_type}INDEX `idx_{table_name}_{idx['columns'][0]}` ON ({', '.join(idx['columns'])})\n"
        
        # Foreign keys - CLEARLY mark which are deferred!
        fks = blueprint.get("foreign_keys", {})
        if fks.get("outgoing"):
            context += "\n### Foreign Keys:\n"
            for fk in fks["outgoing"]:
                is_deferred = fk.get('is_deferred', False)
                if is_deferred:
                    context += f"- ⚠️ DEFERRED (is_deferred: true) - SKIP THIS FK IN CREATE TABLE!\n"
                    context += f"  {fk['name']}: ({', '.join(fk['columns'])}) → {fk['references_table']}\n"
                else:
                    context += f"- {fk['name']}: ({', '.join(fk['columns'])}) → {fk['references_table']}\n"
        
        # Show deferred FKs separately for emphasis
        if fks.get("deferred"):
            context += "\n### ⚠️ DEFERRED FKs (DO NOT include in CREATE TABLE!):\n"
            for fk in fks["deferred"]:
                context += f"- {fk['name']}: Skip this - will be added via ALTER TABLE later\n"
        
        if fks.get("incoming"):
            context += "\n### Tables that depend on this table:\n"
            for incoming in fks["incoming"]:
                context += f"- {incoming['from_table']}\n"
        
        # Dependencies info
        deps = blueprint.get("dependencies", {})
        if deps.get("depends_on"):
            context += f"\n### This table depends on: {', '.join(deps['depends_on'])}\n"
        
        if deps.get("has_circular_fk"):
            context += "\n### ⚠️ WARNING: This table has CIRCULAR FK dependencies - some FKs are deferred!\n"
        
        # Related views (so LLM knows the table name is used)
        related_views = blueprint.get("related_views", [])
        if related_views:
            context += f"\n### Views that use this table ({len(related_views)}):\n"
            for v in related_views[:3]:  # Limit to 3 for token economy
                context += f"- {v['name']}\n"
        
        # Related triggers
        related_triggers = blueprint.get("related_triggers", [])
        if related_triggers:
            context += f"\n### Triggers on this table ({len(related_triggers)}):\n"
            for t in related_triggers:
                context += f"- {t['name']} ({t['timing']} {t['event']})\n"
        
        # Row count
        row_count = blueprint.get("schema", {}).get("row_count")
        if row_count:
            context += f"\n### Row Count: {row_count:,}\n"
        
        return context

    def _build_metadata_context(self, table) -> str:
        """Build rich metadata context for the LLM (fallback if no blueprint)."""
        context = f"""## Table: {table.name}

### Columns:
"""
        for col in table.columns:
            col_info = f"- `{col['name']}`: {col['type']}"
            if not col.get('nullable', True):
                col_info += " NOT NULL"
            if col.get('autoincrement'):
                col_info += " AUTO_INCREMENT"
            if col.get('default'):
                col_info += f" DEFAULT {col['default']}"
            context += col_info + "\n"
        
        if table.primary_key:
            context += f"\n### Primary Key: {', '.join(table.primary_key)}\n"
        
        if table.indexes:
            context += "\n### Indexes:\n"
            for idx in table.indexes:
                idx_type = "UNIQUE " if idx.get('unique') else ""
                context += f"- {idx_type}INDEX `{idx['name']}` ON ({', '.join(idx['columns'])})\n"
        
        if table.foreign_keys:
            context += "\n### Foreign Keys:\n"
            for fk in table.foreign_keys:
                context += f"- ({', '.join(fk['columns'])}) → {fk['referred_table']}({', '.join(fk['referred_columns'])})\n"
        
        if table.row_count:
            context += f"\n### Row Count: {table.row_count:,}\n"
        
        return context
    
    def _generate_deferred_fks(self, blueprints_dir) -> list:
        """Generate ALTER TABLE statements for ALL foreign keys (Two-Pass approach).
        
        In Two-Pass FK approach:
        - Pass 1: CREATE TABLE without ANY FKs (done by LLM)
        - Pass 2: ALTER TABLE ADD CONSTRAINT for ALL FKs (done here)
        """
        import json
        
        all_fk_statements = []
        
        try:
            # Scan all blueprint files for ALL FKs (not just deferred)
            for blueprint_file in blueprints_dir.glob("*.blueprint.json"):
                with open(blueprint_file) as f:
                    blueprint = json.load(f)
                
                table_name = blueprint.get("table_name")
                fks = blueprint.get("foreign_keys", {})
                outgoing = fks.get("outgoing", [])
                
                # Process ALL outgoing FKs
                for fk in outgoing:
                    # Generate ALTER TABLE statement for each FK
                    fk_name = fk.get("name", f"fk_{table_name}_{fk.get('references_table', 'unknown')}")
                    columns = fk.get("columns", [])
                    ref_table = fk.get("references_table")
                    ref_columns = fk.get("references_columns", [])
                    
                    if not columns or not ref_table or not ref_columns:
                        continue
                    
                    columns_sql = ", ".join(f'"{c}"' for c in columns)
                    ref_columns_sql = ", ".join(f'"{c}"' for c in ref_columns)
                    
                    alter_stmt = f'''ALTER TABLE "{table_name}" ADD CONSTRAINT "{fk_name}" 
    FOREIGN KEY ({columns_sql}) REFERENCES "{ref_table}" ({ref_columns_sql});'''
                    
                    all_fk_statements.append(alter_stmt)
                    self.log(f"  FK: {table_name}.{columns[0]} → {ref_table}")
        
        except Exception as e:
            self.log(f"Error generating FKs: {e}", "error")
        
        self.log(f"Generated {len(all_fk_statements)} foreign key constraints")
        return all_fk_statements
    
    def _generate_indexes(self, blueprints_dir) -> list:
        """Generate CREATE INDEX statements for ALL indexes (Two-Pass approach).
        
        In Two-Pass Index approach:
        - Pass 1: CREATE TABLE without ANY indexes (done by LLM)
        - Pass 2: CREATE INDEX for ALL indexes (done here)
        
        This ensures consistent index creation regardless of LLM behavior.
        """
        import json
        
        all_index_statements = []
        
        try:
            # Scan all blueprint files for ALL indexes
            for blueprint_file in blueprints_dir.glob("*.blueprint.json"):
                with open(blueprint_file) as f:
                    blueprint = json.load(f)
                
                table_name = blueprint.get("table_name")
                indexes = blueprint.get("indexes", [])
                columns_info = {c['name']: c for c in blueprint.get("columns", [])}
                
                for idx in indexes:
                    idx_name = idx.get("name", "")
                    idx_columns = idx.get("columns", [])
                    is_unique = idx.get("unique", False)
                    
                    if not idx_name or not idx_columns:
                        continue
                    
                    # Rename index to include table name if not already included
                    if not idx_name.startswith(f"idx_{table_name}"):
                        # Check if it's an old-style name like idx_fk_country_id
                        if idx_name.startswith("idx_fk_") or idx_name.startswith("idx_"):
                            # Extract the column part and add table name
                            col_part = idx_name.replace("idx_fk_", "").replace("idx_", "")
                            idx_name = f"idx_{table_name}_{col_part}"
                    
                    columns_sql = ", ".join(f'"{c}"' for c in idx_columns)
                    
                    # Check if any column is GEOMETRY/POINT type - needs GIST index
                    needs_gist = False
                    for col in idx_columns:
                        col_info = columns_info.get(col, {})
                        col_type = col_info.get("type", "").upper()
                        if col_type in ["GEOMETRY", "POINT", "NULL"]:  # NULL often means GEOMETRY
                            needs_gist = True
                            break
                    
                    # Generate CREATE INDEX statement
                    unique_str = "UNIQUE " if is_unique else ""
                    if needs_gist:
                        index_stmt = f'CREATE {unique_str}INDEX "{idx_name}" ON "{table_name}" USING GIST ({columns_sql});'
                    else:
                        index_stmt = f'CREATE {unique_str}INDEX "{idx_name}" ON "{table_name}" ({columns_sql});'
                    
                    all_index_statements.append(index_stmt)
                    self.log(f"  Index: {idx_name} on {table_name}({', '.join(idx_columns)})")
        
        except Exception as e:
            self.log(f"Error generating indexes: {e}", "error")
        
        self.log(f"Generated {len(all_index_statements)} indexes")
        return all_index_statements

    
    def _llm_convert_table(self, table_name: str, metadata_context: str) -> str:
        """Use LLM to convert table metadata to PostgreSQL DDL."""
        
        prompt = f"""Convert the following MySQL table metadata to a PostgreSQL CREATE TABLE statement:

{metadata_context}

Generate a complete, valid PostgreSQL CREATE TABLE statement for table "{table_name}".
Remember to:
1. Convert all data types appropriately
2. Convert AUTO_INCREMENT to SERIAL/BIGSERIAL
3. Use double quotes for identifiers 
4. Include all constraints (PK, FK, indexes)

Return ONLY the SQL statement, no explanations."""

        messages = [
            SystemMessage(content=SCHEMA_AGENT_SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ]
        
        # Use invoke_with_retry for automatic API key rotation on rate limits
        response = self.invoke_with_retry(messages)
        return self.extract_text_content(response)
    
    def _llm_convert_view(self, view) -> str:
        """Use LLM to convert MySQL view to PostgreSQL."""
        
        prompt = f"""Convert this MySQL VIEW to PostgreSQL 16:

View name: {view.name}
MySQL Definition:
{view.definition}

## CRITICAL CONVERSION RULES:

### 1. Schema References
- Do NOT use any schema prefix (no "sakila.", no "public.")
- Reference tables directly: FROM actor (not FROM sakila.actor)

### 2. Table Names (Sakila Database)
- ALL table names are SINGULAR: customer, payment, rental, staff, inventory

### 3. STRING_AGG with DISTINCT (CRITICAL!)
PostgreSQL CANNOT use DISTINCT inside STRING_AGG with ORDER BY!

❌ WRONG:
STRING_AGG(DISTINCT c.name, ', ' ORDER BY c.name)

✅ CORRECT OPTIONS:
-- Option 1: Use LATERAL subquery
LEFT JOIN LATERAL (
    SELECT STRING_AGG(name, ', ' ORDER BY name) AS names
    FROM ...
) sub ON true

-- Option 2: Remove DISTINCT, add to GROUP BY
STRING_AGG(c.name, ', ' ORDER BY c.name)

### 4. GROUP_CONCAT Conversion
- GROUP_CONCAT(x) → STRING_AGG(x, ',')
- GROUP_CONCAT(x SEPARATOR '; ') → STRING_AGG(x, '; ')
- GROUP_CONCAT(DISTINCT x) → STRING_AGG(DISTINCT x::text, ',')

### 5. CASE WHEN with Boolean
- Use = 1 or = TRUE for active column checks
- CASE WHEN cu.active = 1 THEN 'active' ELSE '' END

### 6. Type Casting
- Cast numerics to text if needed for string operations: x::text

Return ONLY the valid PostgreSQL SQL statement, no markdown, no explanations."""

        messages = [
            SystemMessage(content=SCHEMA_AGENT_SYSTEM_PROMPT),
            HumanMessage(content=prompt)
        ]
        
        # Use invoke_with_retry for automatic API key rotation on rate limits
        import re
        response = self.invoke_with_retry(messages)
        result = self.extract_text_content(response)
        # Post-process: remove any sakila. prefix that might still appear
        result = re.sub(r'\bsakila\.(\w+)', r'\1', result)
        return result
    
    def _clean_sql_output(self, sql: str) -> str:
        """Clean up LLM output to extract pure SQL."""
        # Remove markdown code blocks if present
        sql = sql.strip()
        
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        
        if sql.endswith("```"):
            sql = sql[:-3]
        
        # Remove any leading/trailing whitespace
        sql = sql.strip()
        
        # Ensure it ends with semicolon
        if not sql.endswith(";"):
            sql += ";"
        
        return sql
    
    def _clear_ddl_dir(self):
        """Clear the DDL directory."""
        try:
            ddl_dir = self.artifact_manager.artifacts_dir / "ddl"
            if ddl_dir.exists():
                # Remove all files in ddl and subdirectories
                import shutil
                # We can't just delete the dir because artifact_manager might expect it to exist?
                # Actually artifact_manager creates it on save.
                # safely remove contents
                for item in ddl_dir.iterdir():
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        shutil.rmtree(item)
                self.log("Cleared old DDL directory")
        except Exception as e:
            self.log(f"Could not clear DDL directory: {e}", "warning")


def schema_node(state: dict) -> dict:
    """LangGraph node function for schema transformation."""
    agent = SchemaAgent()
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
