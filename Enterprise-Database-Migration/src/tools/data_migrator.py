"""
Data Migrator - Streaming batch transfer from MySQL to PostgreSQL.
Handles data type transformations and bulk inserts with FK constraint management.
"""

import time
from io import StringIO
from typing import Any, Generator
from dataclasses import dataclass, field

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from langchain_core.tools import tool

from src.config import get_settings
from src.state import TableMetadata, DependencyGraph


@dataclass
class TableMigrationResult:
    """Result of migrating a single table."""
    table_name: str
    rows_migrated: int
    duration_ms: float
    success: bool
    errors: list[str] = field(default_factory=list)
    

@dataclass
class DataMigrationResult:
    """Result of the complete data migration."""
    total_rows: int
    tables_migrated: int
    tables_failed: int
    total_duration_ms: float
    success: bool
    table_results: list[TableMigrationResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class DataMigrator:
    """
    Handles streaming batch data transfer from MySQL to PostgreSQL.
    
    Features:
    - Batch streaming (default 1000 rows) for memory efficiency
    - Data type transformation (MySQL → PostgreSQL)
    - FK constraint management (disable during transfer)
    - Sequence resetting for SERIAL columns
    - Row count validation
    """
    
    def __init__(
        self, 
        source_connection: str | None = None,
        target_connection: str | None = None,
        batch_size: int = 1000
    ):
        settings = get_settings()
        self._source_connection = source_connection or settings.db.source_connection_string
        self._target_connection = target_connection or settings.db.target_connection_string
        self._source_engine: Engine | None = None
        self._target_engine: Engine | None = None
        self.batch_size = batch_size
        
    @property
    def source_engine(self) -> Engine:
        """Get or create source MySQL engine."""
        if self._source_engine is None:
            self._source_engine = create_engine(self._source_connection)
        return self._source_engine
    
    @property
    def target_engine(self) -> Engine:
        """Get or create target PostgreSQL engine."""
        if self._target_engine is None:
            self._target_engine = create_engine(self._target_connection)
        return self._target_engine
    
    def test_connections(self) -> tuple[bool, bool]:
        """Test both database connections."""
        source_ok = False
        target_ok = False
        
        try:
            with self.source_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                source_ok = True
        except Exception as e:
            print(f"❌ Source connection failed: {e}")
            
        try:
            with self.target_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                target_ok = True
        except Exception as e:
            print(f"❌ Target connection failed: {e}")
            
        return source_ok, target_ok
    
    def disable_foreign_keys(self) -> bool:
        """Disable FK constraints in PostgreSQL for bulk loading."""
        try:
            with self.target_engine.connect() as conn:
                # This disables FK checking for the session
                conn.execute(text("SET session_replication_role = 'replica'"))
                conn.commit()
            print("✅ Foreign key constraints disabled")
            return True
        except Exception as e:
            print(f"❌ Failed to disable FK constraints: {e}")
            return False
    
    def enable_foreign_keys(self) -> bool:
        """Re-enable FK constraints in PostgreSQL."""
        try:
            with self.target_engine.connect() as conn:
                conn.execute(text("SET session_replication_role = 'origin'"))
                conn.commit()
            print("✅ Foreign key constraints re-enabled")
            return True
        except Exception as e:
            print(f"❌ Failed to enable FK constraints: {e}")
            return False
    
    def get_migration_order(self, dependency_graph: DependencyGraph | None = None) -> list[str]:
        """
        Get table migration order from dependency graph.
        Falls back to alphabetical if no graph provided.
        """
        if dependency_graph and dependency_graph.migration_order:
            return dependency_graph.migration_order
        
        # Fallback: get all tables from source
        inspector = inspect(self.source_engine)
        return sorted(inspector.get_table_names())
    
    def get_table_columns(self, table_name: str) -> list[dict[str, Any]]:
        """Get column metadata for a table from source database."""
        inspector = inspect(self.source_engine)
        columns = inspector.get_columns(table_name)
        return columns
    
    def get_source_row_count(self, table_name: str) -> int:
        """Get row count from source table."""
        try:
            with self.source_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                return result.scalar() or 0
        except Exception:
            return 0
    
    def get_target_row_count(self, table_name: str) -> int:
        """Get row count from target table."""
        try:
            with self.target_engine.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                return result.scalar() or 0
        except Exception:
            return 0
    
    def stream_source_data(
        self, 
        table_name: str, 
        batch_size: int | None = None
    ) -> Generator[list[dict], None, None]:
        """
        Stream data from source table in batches.
        
        Yields:
            List of row dictionaries for each batch
        """
        batch_size = batch_size or self.batch_size
        offset = 0
        
        with self.source_engine.connect() as conn:
            while True:
                query = text(f"SELECT * FROM `{table_name}` LIMIT :limit OFFSET :offset")
                result = conn.execute(query, {"limit": batch_size, "offset": offset})
                
                rows = [dict(row._mapping) for row in result]
                
                if not rows:
                    break
                    
                yield rows
                offset += batch_size
    
    def transform_value(self, value: Any, mysql_type: str) -> Any:
        """
        Transform a MySQL value to PostgreSQL-compatible format.
        
        Args:
            value: The value to transform
            mysql_type: MySQL column type string
            
        Returns:
            Transformed value
        """
        if value is None:
            return None
        
        mysql_type_upper = str(mysql_type).upper()
        
        # Handle invalid MySQL dates
        if "DATE" in mysql_type_upper or "TIMESTAMP" in mysql_type_upper:
            str_value = str(value)
            if str_value.startswith("0000-00-00"):
                return None
        
        # Handle TINYINT(1) as boolean
        if "TINYINT(1)" in mysql_type_upper:
            return bool(value)
        
        # Handle binary data
        if "BLOB" in mysql_type_upper or "BINARY" in mysql_type_upper:
            if isinstance(value, bytes):
                return value  # psycopg2 handles bytes correctly
            return value
        
        # Handle BIT type
        if mysql_type_upper.startswith("BIT"):
            if isinstance(value, bytes):
                return int.from_bytes(value, byteorder='big')
            return value
        
        # Handle SET type (convert to array-like string or keep as-is)
        if mysql_type_upper.startswith("SET"):
            if isinstance(value, str):
                return value  # Keep comma-separated string
            return value
        
        # Most types work as-is
        return value
    
    def transform_row(
        self, 
        row: dict[str, Any], 
        columns: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Transform a complete row for PostgreSQL compatibility."""
        column_types = {col["name"]: str(col["type"]) for col in columns}
        
        transformed = {}
        for col_name, value in row.items():
            mysql_type = column_types.get(col_name, "")
            transformed[col_name] = self.transform_value(value, mysql_type)
        
        return transformed
    
    def bulk_insert(
        self, 
        table_name: str, 
        rows: list[dict[str, Any]], 
        columns: list[str]
    ) -> tuple[bool, str | None]:
        """
        Bulk insert rows into PostgreSQL table.
        
        Uses executemany for reliability.
        
        Args:
            table_name: Target table name
            rows: List of row dictionaries
            columns: List of column names
            
        Returns:
            Tuple of (success, error_message)
        """
        if not rows:
            return True, None
        
        try:
            # Build INSERT statement with proper quoting
            col_list = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(f":{c}" for c in columns)
            sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})'
            
            with self.target_engine.connect() as conn:
                conn.execute(text(sql), rows)
                conn.commit()
            
            return True, None
            
        except Exception as e:
            return False, str(e)
    
    def migrate_table(
        self, 
        table_name: str,
        columns: list[dict[str, Any]] | None = None
    ) -> TableMigrationResult:
        """
        Migrate a single table from MySQL to PostgreSQL.
        
        Args:
            table_name: Name of the table to migrate
            columns: Optional column metadata (fetched if not provided)
            
        Returns:
            TableMigrationResult with migration statistics
        """
        start_time = time.time()
        total_rows = 0
        errors = []
        
        print(f"📥 Migrating table: {table_name}")
        
        # Get column metadata if not provided
        if columns is None:
            columns = self.get_table_columns(table_name)
        
        column_names = [col["name"] for col in columns]
        
        try:
            # Stream and insert in batches
            for batch_num, batch in enumerate(self.stream_source_data(table_name)):
                # Transform each row
                transformed_rows = [
                    self.transform_row(row, columns) 
                    for row in batch
                ]
                
                # Bulk insert
                success, error = self.bulk_insert(table_name, transformed_rows, column_names)
                
                if not success:
                    errors.append(f"Batch {batch_num}: {error}")
                    # Continue with other batches
                else:
                    total_rows += len(transformed_rows)
                
                # Progress logging every 10 batches
                if (batch_num + 1) % 10 == 0:
                    print(f"  ... {total_rows:,} rows migrated")
            
            duration_ms = (time.time() - start_time) * 1000
            success = len(errors) == 0
            
            if success:
                print(f"✅ {table_name}: {total_rows:,} rows in {duration_ms:.0f}ms")
            else:
                print(f"⚠️ {table_name}: {total_rows:,} rows with {len(errors)} errors")
            
            return TableMigrationResult(
                table_name=table_name,
                rows_migrated=total_rows,
                duration_ms=duration_ms,
                success=success,
                errors=errors
            )
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            print(f"❌ {table_name}: Failed - {e}")
            return TableMigrationResult(
                table_name=table_name,
                rows_migrated=total_rows,
                duration_ms=duration_ms,
                success=False,
                errors=[str(e)]
            )
    
    def reset_sequences(self) -> list[str]:
        """
        Reset PostgreSQL sequences for SERIAL columns to max value.
        
        Returns:
            List of reset sequence names
        """
        reset_sequences = []
        
        try:
            with self.target_engine.connect() as conn:
                # Find all sequences
                result = conn.execute(text("""
                    SELECT 
                        c.table_name,
                        c.column_name,
                        pg_get_serial_sequence(c.table_name, c.column_name) as seq_name
                    FROM information_schema.columns c
                    WHERE c.table_schema = 'public'
                    AND c.column_default LIKE 'nextval%'
                """))
                
                for row in result:
                    table_name = row.table_name
                    column_name = row.column_name
                    seq_name = row.seq_name
                    
                    if seq_name:
                        # Reset sequence to max value
                        reset_sql = text(f"""
                            SELECT setval('{seq_name}', 
                                COALESCE((SELECT MAX("{column_name}") FROM "{table_name}"), 1))
                        """)
                        conn.execute(reset_sql)
                        reset_sequences.append(seq_name)
                
                conn.commit()
                
            print(f"✅ Reset {len(reset_sequences)} sequences")
            
        except Exception as e:
            print(f"⚠️ Error resetting sequences: {e}")
        
        return reset_sequences
    
    def validate_row_counts(self, tables: list[str]) -> list[dict[str, Any]]:
        """
        Validate row counts match between source and target.
        
        Args:
            tables: List of table names to validate
            
        Returns:
            List of validation results
        """
        results = []
        
        for table in tables:
            source_count = self.get_source_row_count(table)
            target_count = self.get_target_row_count(table)
            
            match = source_count == target_count
            
            results.append({
                "table": table,
                "source_count": source_count,
                "target_count": target_count,
                "match": match,
                "difference": abs(source_count - target_count)
            })
            
            status = "✅" if match else "❌"
            print(f"{status} {table}: {source_count} → {target_count}")
        
        return results
    
    def run_full_migration(
        self, 
        dependency_graph: DependencyGraph | None = None,
        continue_on_error: bool = True
    ) -> DataMigrationResult:
        """
        Run complete data migration from MySQL to PostgreSQL.
        
        Args:
            dependency_graph: Optional dependency graph for table ordering
            continue_on_error: If True, continue migrating other tables on failure
            
        Returns:
            DataMigrationResult with complete migration statistics
        """
        start_time = time.time()
        table_results = []
        errors = []
        
        print("=" * 60)
        print("🚀 Starting Data Migration: MySQL → PostgreSQL")
        print("=" * 60)
        
        # Test connections
        source_ok, target_ok = self.test_connections()
        if not source_ok or not target_ok:
            return DataMigrationResult(
                total_rows=0,
                tables_migrated=0,
                tables_failed=0,
                total_duration_ms=0,
                success=False,
                errors=["Database connection failed"]
            )
        
        # Get migration order
        tables = self.get_migration_order(dependency_graph)
        print(f"📋 Tables to migrate: {len(tables)}")
        
        # Disable FK constraints
        if not self.disable_foreign_keys():
            errors.append("Failed to disable FK constraints")
        
        # Migrate each table
        for table in tables:
            result = self.migrate_table(table)
            table_results.append(result)
            
            if not result.success:
                errors.extend(result.errors)
                if not continue_on_error:
                    break
        
        # Re-enable FK constraints
        if not self.enable_foreign_keys():
            errors.append("Failed to re-enable FK constraints")
        
        # Reset sequences
        self.reset_sequences()
        
        # Validate row counts
        print("\n📊 Validating row counts...")
        self.validate_row_counts(tables)
        
        # Calculate totals
        total_rows = sum(r.rows_migrated for r in table_results)
        tables_migrated = sum(1 for r in table_results if r.success)
        tables_failed = sum(1 for r in table_results if not r.success)
        total_duration_ms = (time.time() - start_time) * 1000
        
        print("\n" + "=" * 60)
        print(f"✅ Migration Complete!")
        print(f"   Tables: {tables_migrated}/{len(tables)} succeeded")
        print(f"   Rows: {total_rows:,} migrated")
        print(f"   Duration: {total_duration_ms/1000:.1f}s")
        print("=" * 60)
        
        return DataMigrationResult(
            total_rows=total_rows,
            tables_migrated=tables_migrated,
            tables_failed=tables_failed,
            total_duration_ms=total_duration_ms,
            success=tables_failed == 0,
            table_results=table_results,
            errors=errors
        )
    
    def close(self):
        """Close database connections."""
        if self._source_engine:
            self._source_engine.dispose()
        if self._target_engine:
            self._target_engine.dispose()


# ============================================================================
# LangChain Tools for Agent Use
# ============================================================================

@tool
def migrate_data_batch(
    table_name: str,
    batch_size: int = 1000
) -> str:
    """
    Migrate data for a single table from MySQL to PostgreSQL.
    
    Args:
        table_name: Name of the table to migrate
        batch_size: Number of rows per batch (default 1000)
        
    Returns:
        Migration result summary
    """
    migrator = DataMigrator(batch_size=batch_size)
    
    try:
        result = migrator.migrate_table(table_name)
        
        if result.success:
            return f"✅ Migrated {result.rows_migrated:,} rows from {table_name} in {result.duration_ms:.0f}ms"
        else:
            return f"❌ Failed to migrate {table_name}: {'; '.join(result.errors)}"
    finally:
        migrator.close()


@tool
def run_full_data_migration(continue_on_error: bool = True) -> str:
    """
    Run complete data migration from MySQL to PostgreSQL.
    Migrates all tables in dependency order with FK constraint management.
    
    Args:
        continue_on_error: If True, continue with other tables if one fails
        
    Returns:
        Migration summary
    """
    migrator = DataMigrator()
    
    try:
        result = migrator.run_full_migration(continue_on_error=continue_on_error)
        
        summary = f"""
Data Migration Complete:
- Tables: {result.tables_migrated}/{result.tables_migrated + result.tables_failed}
- Rows: {result.total_rows:,}
- Duration: {result.total_duration_ms/1000:.1f}s
- Status: {'✅ Success' if result.success else '❌ Failed'}
"""
        if result.errors:
            summary += f"\nErrors:\n" + "\n".join(f"  - {e}" for e in result.errors[:5])
        
        return summary
    finally:
        migrator.close()


@tool
def validate_data_migration() -> str:
    """
    Validate that row counts match between MySQL source and PostgreSQL target.
    
    Returns:
        Validation summary
    """
    migrator = DataMigrator()
    
    try:
        tables = migrator.get_migration_order()
        results = migrator.validate_row_counts(tables)
        
        matched = sum(1 for r in results if r["match"])
        mismatched = [r for r in results if not r["match"]]
        
        summary = f"Validation: {matched}/{len(results)} tables match\n"
        
        if mismatched:
            summary += "\nMismatched tables:\n"
            for r in mismatched:
                summary += f"  - {r['table']}: {r['source_count']} → {r['target_count']} (diff: {r['difference']})\n"
        
        return summary
    finally:
        migrator.close()
