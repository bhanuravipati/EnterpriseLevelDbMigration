"""
<<<<<<< HEAD
Data Migration Agent - Orchestrates data transfer from MySQL to PostgreSQL.
Uses streaming batch approach with progress tracking and error handling.
"""

import json
from pathlib import Path

from src.agents.base_agent import BaseAgent
from src.state import MigrationPhase
from src.tools.data_migrator import DataMigrator, DataMigrationResult


def data_migration_node(state: dict) -> dict:
    """
    LangGraph node for data migration.
    Transfers data from MySQL to PostgreSQL after schema migration.
    
    Args:
        state: Current migration state
        
    Returns:
        Updated state with data migration results
    """
    print("\n" + "=" * 60)
    print("📦 DATA MIGRATION AGENT")
    print("=" * 60)
    
    # Check if schema migration was successful
    sandbox_results = state.get("sandbox_results", [])
    validation_passed = state.get("validation_passed", False)
    
    if not validation_passed:
        print("⚠️ Skipping data migration - validation not passed")
        return {
            "current_phase": MigrationPhase.DATA_MIGRATION.value,
            "data_migration_complete": False,
            "data_migration_results": [],
            "tables_migrated": [],
        }
    
    # Get dependency graph for table ordering
    dependency_graph = state.get("dependency_graph")
    
    # Initialize migrator
    migrator = DataMigrator(batch_size=1000)
    
    try:
        # Run full migration
        result: DataMigrationResult = migrator.run_full_migration(
            dependency_graph=dependency_graph,
            continue_on_error=True
        )
        
        # Convert results to serializable format
        table_results = []
        tables_migrated = []
        
        for tr in result.table_results:
            table_results.append({
                "table_name": tr.table_name,
                "rows_migrated": tr.rows_migrated,
                "duration_ms": tr.duration_ms,
                "success": tr.success,
                "errors": tr.errors
            })
            if tr.success:
                tables_migrated.append(tr.table_name)
        
        # Create summary for state
        summary = {
            "total_rows": result.total_rows,
            "tables_migrated_count": result.tables_migrated,
            "tables_failed_count": result.tables_failed,
            "total_duration_ms": result.total_duration_ms,
            "success": result.success,
            "errors": result.errors
        }
        
        # Save results to artifact file for UI
        artifacts_dir = Path("./artifacts")
        artifacts_dir.mkdir(exist_ok=True)
        try:
            with open(artifacts_dir / "data_migration_results.json", "w") as f:
                json.dump({
                    "summary": summary,
                    "table_results": table_results
                }, f, indent=2)
            print(f"💾 Saved data migration results to artifacts/data_migration_results.json")
        except Exception as save_err:
            print(f"⚠️ Could not save data migration results: {save_err}")
        
        print(f"\n✅ Data migration {'completed successfully' if result.success else 'completed with errors'}")
        
        return {
            "current_phase": MigrationPhase.DATA_MIGRATION.value,
            "data_migration_complete": result.success,
            "data_migration_results": table_results,
            "data_migration_summary": summary,
            "tables_migrated": tables_migrated,
        }
        
    except Exception as e:
        print(f"❌ Data migration failed: {e}")
        return {
            "current_phase": MigrationPhase.DATA_MIGRATION.value,
            "data_migration_complete": False,
            "data_migration_results": [],
            "tables_migrated": [],
            "errors": [{"phase": "data_migration", "error": str(e)}]
        }
    finally:
        migrator.close()
=======
Data Migration Agent - Streams data from MySQL to PostgreSQL using batch processing.
Uses the sandbox database by default for safe testing before production deployment.
"""

import struct
import time
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase, MigrationStatus
from src.tools.artifact_manager import get_artifact_manager
from src.config import get_settings


def convert_wkb_to_point(wkb_bytes) -> str | None:
    """
    Convert MySQL WKB (Well-Known Binary) GEOMETRY to PostgreSQL POINT format.
    MySQL POINT WKB format: 4 bytes SRID + 1 byte order + 4 bytes type + 8 bytes X + 8 bytes Y
    """
    if wkb_bytes is None:
        return None
    
    try:
        # Handle bytes or memoryview
        if isinstance(wkb_bytes, memoryview):
            wkb_bytes = bytes(wkb_bytes)
        
        if len(wkb_bytes) < 21:  # Minimum size for POINT with SRID
            return None
        
        # Skip SRID (4 bytes) + byte order (1 byte) + type (4 bytes) = 9 bytes
        # Then read X and Y as doubles (8 bytes each)
        x = struct.unpack('<d', wkb_bytes[9:17])[0]
        y = struct.unpack('<d', wkb_bytes[17:25])[0]
        
        # PostgreSQL POINT format: (x, y)
        return f"({x}, {y})"
    except Exception as e:
        # If parsing fails, return NULL - this is safer than failing
        return None


# Type converters for MySQL → PostgreSQL
TYPE_CONVERTERS = {
    # Boolean handling (MySQL TINYINT(1) → PostgreSQL BOOLEAN)
    "tinyint(1)": lambda v: bool(v) if v is not None else None,
    "bit(1)": lambda v: bool(v) if v is not None else None,
    
    # Binary data (MySQL BLOB → PostgreSQL BYTEA)
    "blob": lambda v: bytes(v) if v is not None else None,
    "tinyblob": lambda v: bytes(v) if v is not None else None,
    "mediumblob": lambda v: bytes(v) if v is not None else None,
    "longblob": lambda v: bytes(v) if v is not None else None,
    
    # SET type (MySQL SET → PostgreSQL TEXT[])
    "set": lambda v: v.split(",") if v else [],
    
    # GEOMETRY/POINT types - convert WKB to PostgreSQL POINT format
    "geometry": convert_wkb_to_point,
    "point": convert_wkb_to_point,
    "null": convert_wkb_to_point,  # SQLAlchemy reports GEOMETRY as NULL type
    
    # Default: pass through (Python handles most conversions)
}

# Columns that require special handling (table.column -> converter)
SPECIAL_COLUMN_HANDLERS = {
    "address.location": convert_wkb_to_point,
    "staff.picture": lambda v: bytes(v) if v is not None else None,  # BLOB
}

>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b


class DataMigrationAgent(BaseAgent):
    """
<<<<<<< HEAD
    Agent for orchestrating data migration from MySQL to PostgreSQL.
    
    This agent:
    1. Uses the DataMigrator tool for streaming batch transfer
    2. Tracks progress per table
    3. Handles errors gracefully
    4. Reports statistics
    """
    
    def __init__(self):
        super().__init__(
            name="DataMigrationAgent",
            description="Transfers data from MySQL to PostgreSQL using streaming batches",
            use_complex_model=False,  # Doesn't need LLM for data transfer
        )
        self.migrator = DataMigrator()
    
    def run(self, state: dict) -> dict:
        """
        Execute data migration.
        
        Args:
            state: Current migration state
            
        Returns:
            Updated state with migration results
        """
        return data_migration_node(state)
=======
    Agent responsible for migrating data from MySQL to PostgreSQL.
    Uses streaming batch approach to be memory-efficient.
    
    By default, migrates to sandbox database for validation.
    Set use_sandbox=False to migrate directly to target (for production deploy).
    """
    
    def __init__(self, use_sandbox: bool = True):
        super().__init__(
            name="Data Migration Agent",
            description="Streams data from MySQL to PostgreSQL in batches",
            use_complex_model=False,  # No LLM needed for data migration
            system_prompt="You are the Data Migration Agent."
        )
        self.use_sandbox = use_sandbox
        self.artifact_manager = get_artifact_manager()
        
        settings = get_settings()
        
        # Source: MySQL
        self._source_engine: Engine | None = None
        self.source_conn_str = settings.db.source_connection_string
        
        # Target: Sandbox or Production
        if use_sandbox:
            self._target_engine: Engine | None = None
            self.target_conn_str = settings.db.sandbox_connection_string
            self.target_name = "sandbox"
        else:
            self._target_engine: Engine | None = None
            self.target_conn_str = settings.db.target_connection_string
            self.target_name = "target"
        
        # Configuration
        self.batch_size = 1000
        
        # Column type cache (table -> column -> type)
        self._column_types: dict[str, dict[str, str]] = {}
    
    @property
    def source_engine(self) -> Engine:
        """Get or create source MySQL engine."""
        if self._source_engine is None:
            self._source_engine = create_engine(self.source_conn_str)
        return self._source_engine
    
    @property
    def target_engine(self) -> Engine:
        """Get or create target PostgreSQL engine."""
        if self._target_engine is None:
            self._target_engine = create_engine(self.target_conn_str)
        return self._target_engine
    
    def run(self, state: MigrationState) -> MigrationState:
        """Execute data migration from MySQL to PostgreSQL."""
        self.log(f"Starting data migration to {self.target_name}...")
        
        try:
            # Get table order from dependency graph
            table_order = self._get_table_order(state)
            self.log(f"Migrating {len(table_order)} tables in dependency order")
            
            # Cache column types for transformations
            self._cache_column_types(state)
            
            # Phase 1: Disable FK constraints
            self.log("Disabling FK constraints...")
            self._disable_fk_constraints()
            
            # Phase 2: Migrate data table by table
            migration_results = []
            total_rows = 0
            
            for table_name in table_order:
                result = self._migrate_table(table_name)
                migration_results.append(result)
                total_rows += result.get("rows_migrated", 0)
                
                if result["success"]:
                    self.log(f"  ✓ {table_name}: {result['rows_migrated']:,} rows ({result['time_ms']:.1f}ms)")
                else:
                    self.log(f"  ✗ {table_name}: {result['error']}", "warning")
            
            # Phase 3: Re-enable FK constraints
            self.log("Re-enabling FK constraints...")
            self._enable_fk_constraints()
            
            # Phase 4: Reset sequences
            self.log("Resetting sequences...")
            sequences_reset = self._reset_sequences(table_order)
            self.log(f"  Reset {sequences_reset} sequences")
            
            # Phase 5: Validate row counts
            self.log("Validating row counts...")
            validation_results = self._validate_row_counts(table_order)
            
            passed = len([r for r in validation_results if r["match"]])
            failed = len([r for r in validation_results if not r["match"]])
            
            if failed == 0:
                self.log(f"✅ All {passed} tables validated successfully!", "success")
            else:
                self.log(f"⚠️ {passed} passed, {failed} failed validation", "warning")
            
            # Save results
            results_summary = {
                "target": self.target_name,
                "tables_migrated": len(migration_results),
                "total_rows": total_rows,
                "validation": validation_results,
                "migration_results": migration_results,
            }
            self.artifact_manager.save_json(results_summary, "data_migration_results.json")
            
            # Update state
            state.data_migration_complete = True
            state.tables_migrated = table_order
            state.current_phase = MigrationPhase.DATA_MIGRATION
            state.artifact_paths["data_migration"] = str(
                self.artifact_manager.artifacts_dir / "data_migration_results.json"
            )
            
            self.log(f"Data migration complete: {total_rows:,} rows in {len(table_order)} tables", "success")
            
        except Exception as e:
            self.log(f"Data migration failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.DATA_MIGRATION,
                "error_type": "data_migration_error",
                "error_message": str(e)
            })
        finally:
            self._close_connections()
        
        return state
    
    def _get_table_order(self, state: MigrationState) -> list[str]:
        """Get table order from dependency graph."""
        if state.dependency_graph and state.dependency_graph.migration_order:
            # Filter to just tables (not views, procedures, etc.)
            table_order = []
            for obj_id in state.dependency_graph.migration_order:
                if obj_id.startswith("table:"):
                    table_order.append(obj_id.replace("table:", ""))
            return table_order
        
        # Fallback: Get tables from schema metadata
        if state.schema_metadata:
            return [t.name for t in state.schema_metadata.tables]
        
        return []
    
    def _cache_column_types(self, state: MigrationState):
        """Cache column types for type conversion during migration."""
        if state.schema_metadata:
            for table in state.schema_metadata.tables:
                self._column_types[table.name] = {}
                for col in table.columns:
                    col_name = col.get("name", "")
                    col_type = col.get("type", "").lower()
                    self._column_types[table.name][col_name] = col_type
    
    def _disable_fk_constraints(self):
        """Disable foreign key constraint checking in PostgreSQL."""
        with self.target_engine.connect() as conn:
            conn.execute(text("SET session_replication_role = 'replica'"))
            conn.commit()
    
    def _enable_fk_constraints(self):
        """Re-enable foreign key constraint checking in PostgreSQL."""
        with self.target_engine.connect() as conn:
            conn.execute(text("SET session_replication_role = 'origin'"))
            conn.commit()
    
    def _migrate_table(self, table_name: str) -> dict:
        """Migrate a single table using batch streaming."""
        result = {
            "table": table_name,
            "success": False,
            "rows_migrated": 0,
            "time_ms": 0,
            "error": None
        }
        
        start_time = time.time()
        
        try:
            # Get column names from source
            with self.source_engine.connect() as src_conn:
                col_query = text(f"""
                    SELECT COLUMN_NAME 
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table
                    ORDER BY ORDINAL_POSITION
                """)
                col_result = src_conn.execute(col_query, {"table": table_name})
                columns = [row[0] for row in col_result]
            
            if not columns:
                result["error"] = "No columns found"
                return result
            
            # Clear existing data in target
            # IMPORTANT: Use DELETE instead of TRUNCATE CASCADE to avoid
            # cascading deletes that wipe out previously migrated tables.
            # With session_replication_role='replica', FK constraints are disabled,
            # so DELETE won't fail on FK violations.
            with self.target_engine.connect() as tgt_conn:
                tgt_conn.execute(text(f'DELETE FROM "{table_name}"'))
                tgt_conn.commit()
            
            # Stream data in batches
            offset = 0
            total_rows = 0
            
            # Build column list for SELECT (MySQL uses backticks)
            mysql_cols = ", ".join([f"`{c}`" for c in columns])
            
            while True:
                # Fetch batch from MySQL
                with self.source_engine.connect() as src_conn:
                    query = text(f"SELECT {mysql_cols} FROM `{table_name}` LIMIT :limit OFFSET :offset")
                    rows = src_conn.execute(query, {"limit": self.batch_size, "offset": offset}).fetchall()
                
                if not rows:
                    break
                
                # Transform and insert into PostgreSQL
                self._insert_batch(table_name, columns, rows)
                
                total_rows += len(rows)
                offset += self.batch_size
            
            result["success"] = True
            result["rows_migrated"] = total_rows
            
        except Exception as e:
            result["error"] = str(e)
        
        result["time_ms"] = (time.time() - start_time) * 1000
        return result
    
    def _insert_batch(self, table_name: str, columns: list[str], rows: list):
        """Insert a batch of rows into PostgreSQL."""
        if not rows:
            return
        
        # Build INSERT statement
        pg_cols = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join([f":{c}" for c in columns])
        insert_sql = f'INSERT INTO "{table_name}" ({pg_cols}) VALUES ({placeholders})'
        
        # Transform rows
        transformed_rows = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                
                # 1. Check for special column handler (table.column)
                special_key = f"{table_name}.{col}"
                if special_key in SPECIAL_COLUMN_HANDLERS:
                    value = SPECIAL_COLUMN_HANDLERS[special_key](value)
                else:
                    # 2. Check for type-based conversion
                    col_type = self._column_types.get(table_name, {}).get(col, "")
                    if col_type in TYPE_CONVERTERS:
                        value = TYPE_CONVERTERS[col_type](value)
                
                row_dict[col] = value
            transformed_rows.append(row_dict)
        
        # Bulk insert
        with self.target_engine.connect() as conn:
            conn.execute(text(insert_sql), transformed_rows)
            conn.commit()
    
    def _reset_sequences(self, table_order: list[str]) -> int:
        """Reset PostgreSQL sequences to match max values."""
        sequences_reset = 0
        
        with self.target_engine.connect() as conn:
            for table_name in table_order:
                try:
                    # Find sequences for this table
                    seq_query = text("""
                        SELECT column_name, pg_get_serial_sequence(:table, column_name) as seq_name
                        FROM information_schema.columns
                        WHERE table_name = :table
                        AND column_default LIKE 'nextval%'
                    """)
                    seq_result = conn.execute(seq_query, {"table": table_name})
                    
                    for row in seq_result:
                        col_name = row[0]
                        seq_name = row[1]
                        
                        if seq_name:
                            # Reset sequence to max value
                            reset_query = text(f"""
                                SELECT setval('{seq_name}', COALESCE((SELECT MAX("{col_name}") FROM "{table_name}"), 1))
                            """)
                            conn.execute(reset_query)
                            sequences_reset += 1
                    
                except Exception as e:
                    self.log(f"  Warning: Could not reset sequence for {table_name}: {e}", "warning")
            
            conn.commit()
        
        return sequences_reset
    
    def _validate_row_counts(self, table_order: list[str]) -> list[dict]:
        """Validate row counts match between source and target."""
        results = []
        
        for table_name in table_order:
            result = {
                "table": table_name,
                "source_count": 0,
                "target_count": 0,
                "match": False
            }
            
            try:
                # Get source count
                with self.source_engine.connect() as conn:
                    src_result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                    result["source_count"] = src_result.scalar() or 0
                
                # Get target count
                with self.target_engine.connect() as conn:
                    tgt_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                    result["target_count"] = tgt_result.scalar() or 0
                
                result["match"] = result["source_count"] == result["target_count"]
                
            except Exception as e:
                result["error"] = str(e)
            
            results.append(result)
        
        return results
    
    def _close_connections(self):
        """Close database connections."""
        if self._source_engine:
            self._source_engine.dispose()
            self._source_engine = None
        if self._target_engine:
            self._target_engine.dispose()
            self._target_engine = None


def data_migration_node(state: dict) -> dict:
    """LangGraph node function for data migration."""
    agent = DataMigrationAgent(use_sandbox=True)  # Always use sandbox in workflow
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
