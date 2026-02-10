"""
Introspection Agent - Extracts schema metadata from source MySQL database.
"""

from langchain_core.messages import HumanMessage

from src.agents.base_agent import BaseAgent, AgentResponse
from src.state import MigrationState, MigrationPhase, SchemaMetadata
from src.tools.mysql_introspection import MySQLIntrospector
from src.tools.artifact_manager import get_artifact_manager


class IntrospectionAgent(BaseAgent):
    """
    Agent responsible for extracting complete schema metadata from MySQL.
    Produces: schema_metadata.json artifact
    """
    
    def __init__(self):
        super().__init__(
            name="Introspection Agent",
            description="Extracts and catalogs all schema objects from the source MySQL database",
            use_complex_model=True,  # Use oss-120b for better accuracy
            system_prompt="""You are the Introspection Agent, responsible for extracting 
database schema metadata from MySQL. Your job is to:

1. Connect to the source MySQL database
2. Extract all tables with their columns, types, indexes, and constraints
3. Extract all views with their definitions
4. Extract all stored procedures and functions with their source code
5. Extract all triggers

Provide clear summaries of what you find and flag any potential issues."""
        )
        self.introspector = MySQLIntrospector()
        self.artifact_manager = get_artifact_manager()
    
    def run(self, state: MigrationState) -> MigrationState:
        """
        Execute introspection and update state.
        """
        self.log("Starting database introspection...")
        
        try:
            # Test connection first
            if not self.introspector.test_connection():
                state.errors.append({
                    "phase": MigrationPhase.INTROSPECTION,
                    "error_type": "connection_error",
                    "error_message": "Failed to connect to source MySQL database"
                })
                return state
            
            # Extract full schema
            self.log("Extracting tables...")
            schema = self.introspector.get_full_schema()
            
            self.log(f"Found {len(schema.tables)} tables")
            self.log(f"Found {len(schema.views)} views")
            self.log(f"Found {len(schema.procedures)} procedures/functions")
            self.log(f"Found {len(schema.triggers)} triggers")
            
            # Save artifact
            artifact_path = self.artifact_manager.save_schema_metadata(schema)
            self.log(f"Saved schema metadata to {artifact_path}", "success")
            
            # Update state
            state.schema_metadata = schema
            state.current_phase = MigrationPhase.INTROSPECTION
            state.artifact_paths["schema_metadata"] = str(artifact_path)
            
            # Generate summary using LLM
            summary = self._generate_summary(schema)
            self.log(summary, "info")
            
        except Exception as e:
            self.log(f"Introspection failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.INTROSPECTION,
                "error_type": "introspection_error",
                "error_message": str(e)
            })
        finally:
            self.introspector.close()
        
        return state
    
    def _generate_summary(self, schema: SchemaMetadata) -> str:
        """Generate a summary of the extracted schema."""
        total_rows = sum(t.row_count or 0 for t in schema.tables)
        total_columns = sum(len(t.columns) for t in schema.tables)
        total_indexes = sum(len(t.indexes) for t in schema.tables)
        total_fks = sum(len(t.foreign_keys) for t in schema.tables)
        
        return f"""
Schema Extraction Complete:
- Database: {schema.database_name}
- Tables: {len(schema.tables)} ({total_columns} columns, {total_rows:,} total rows)
- Indexes: {total_indexes}
- Foreign Keys: {total_fks}
- Views: {len(schema.views)}
- Procedures/Functions: {len(schema.procedures)}
- Triggers: {len(schema.triggers)}
"""


def introspection_node(state: dict) -> dict:
    """LangGraph node function for introspection."""
    agent = IntrospectionAgent()
    
    # Convert dict to MigrationState if needed
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
