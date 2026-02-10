"""
MySQL Introspection Tools - Extract schema metadata from MySQL database.
"""

from typing import Any

from langchain_core.tools import tool
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from src.config import get_settings
from src.state import (
    TableMetadata,
    ViewMetadata,
    ProcedureMetadata,
    TriggerMetadata,
    SchemaMetadata,
)


class MySQLIntrospector:
    """Introspects MySQL database to extract schema metadata."""
    
    def __init__(self, connection_string: str | None = None):
        settings = get_settings()
        self.connection_string = connection_string or settings.db.source_connection_string
        self._engine: Engine | None = None
    
    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(self.connection_string)
        return self._engine
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def get_database_name(self) -> str:
        """Get the current database name."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT DATABASE()"))
            return result.scalar() or ""
    
    def get_tables(self) -> list[TableMetadata]:
        """Extract all table metadata."""
        inspector = inspect(self.engine)
        tables = []
        
        for table_name in inspector.get_table_names():
            # Get columns
            columns = []
            for col in inspector.get_columns(table_name):
                columns.append({
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "default": str(col.get("default")) if col.get("default") else None,
                    "autoincrement": col.get("autoincrement", False),
                })
            
            # Get primary key
            pk = inspector.get_pk_constraint(table_name)
            primary_key = pk.get("constrained_columns", []) if pk else []
            
            # Get indexes
            indexes = []
            for idx in inspector.get_indexes(table_name):
                indexes.append({
                    "name": idx["name"],
                    "columns": idx["column_names"],
                    "unique": idx.get("unique", False),
                })
            
            # Get foreign keys
            foreign_keys = []
            for fk in inspector.get_foreign_keys(table_name):
                foreign_keys.append({
                    "name": fk.get("name"),
                    "columns": fk["constrained_columns"],
                    "referred_table": fk["referred_table"],
                    "referred_columns": fk["referred_columns"],
                })
            
            # Get row count
            row_count = self._get_row_count(table_name)
            
            tables.append(TableMetadata(
                name=table_name,
                columns=columns,
                primary_key=primary_key,
                indexes=indexes,
                foreign_keys=foreign_keys,
                row_count=row_count,
            ))
        
        return tables
    
    def _get_row_count(self, table_name: str) -> int:
        """Get approximate row count for a table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                return result.scalar() or 0
        except Exception:
            return 0
    
    def get_views(self) -> list[ViewMetadata]:
        """Extract all view metadata."""
        views = []
        
        with self.engine.connect() as conn:
            # Get view names
            result = conn.execute(text("""
                SELECT TABLE_NAME, VIEW_DEFINITION 
                FROM information_schema.VIEWS 
                WHERE TABLE_SCHEMA = DATABASE()
            """))
            
            for row in result:
                view_name = row[0]
                definition = row[1] or ""
                
                # Get view columns
                col_result = conn.execute(text(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :view_name
                """), {"view_name": view_name})
                
                columns = []
                for col_row in col_result:
                    columns.append({
                        "name": col_row[0],
                        "type": col_row[1],
                        "nullable": col_row[2] == "YES",
                    })
                
                views.append(ViewMetadata(
                    name=view_name,
                    definition=definition,
                    columns=columns,
                ))
        
        return views
    
    def get_procedures(self) -> list[ProcedureMetadata]:
        """Extract all stored procedures and functions."""
        procedures = []
        
        with self.engine.connect() as conn:
            # Get procedures
            result = conn.execute(text("""
                SELECT ROUTINE_NAME, ROUTINE_TYPE, DTD_IDENTIFIER
                FROM information_schema.ROUTINES
                WHERE ROUTINE_SCHEMA = DATABASE()
            """))
            
            for row in result:
                proc_name = row[0]
                proc_type = row[1].lower()  # PROCEDURE or FUNCTION
                return_type = row[2]
                
                # Get procedure source
                try:
                    show_result = conn.execute(text(f"SHOW CREATE {row[1]} `{proc_name}`"))
                    show_row = show_result.fetchone()
                    source_code = show_row[2] if show_row and len(show_row) > 2 else ""
                except Exception:
                    source_code = ""
                
                # Get parameters
                param_result = conn.execute(text("""
                    SELECT PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE
                    FROM information_schema.PARAMETERS
                    WHERE SPECIFIC_SCHEMA = DATABASE() AND SPECIFIC_NAME = :proc_name
                    ORDER BY ORDINAL_POSITION
                """), {"proc_name": proc_name})
                
                parameters = []
                for param_row in param_result:
                    if param_row[0]:  # Skip return value (NULL name)
                        parameters.append({
                            "name": param_row[0],
                            "type": param_row[1],
                            "mode": param_row[2],  # IN, OUT, INOUT
                        })
                
                procedures.append(ProcedureMetadata(
                    name=proc_name,
                    type=proc_type,
                    parameters=parameters,
                    return_type=return_type,
                    source_code=source_code,
                ))
        
        return procedures
    
    def get_triggers(self) -> list[TriggerMetadata]:
        """Extract all trigger metadata."""
        triggers = []
        
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, 
                       EVENT_MANIPULATION, ACTION_STATEMENT
                FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = DATABASE()
            """))
            
            for row in result:
                triggers.append(TriggerMetadata(
                    name=row[0],
                    table_name=row[1],
                    timing=row[2],  # BEFORE, AFTER
                    event=row[3],   # INSERT, UPDATE, DELETE
                    source_code=row[4] or "",
                ))
        
        return triggers
    
    def get_full_schema(self) -> SchemaMetadata:
        """Extract complete schema metadata."""
        return SchemaMetadata(
            database_name=self.get_database_name(),
            database_type="mysql",
            tables=self.get_tables(),
            views=self.get_views(),
            procedures=self.get_procedures(),
            triggers=self.get_triggers(),
        )
    
    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


# LangChain Tools for agent use

@tool
def introspect_mysql_tables() -> str:
    """
    Extract all table metadata from the source MySQL database.
    Returns a JSON string with table names, columns, indexes, and foreign keys.
    """
    introspector = MySQLIntrospector()
    try:
        tables = introspector.get_tables()
        return f"Found {len(tables)} tables: {[t.name for t in tables]}"
    finally:
        introspector.close()


@tool
def introspect_mysql_views() -> str:
    """
    Extract all view definitions from the source MySQL database.
    Returns view names and their SQL definitions.
    """
    introspector = MySQLIntrospector()
    try:
        views = introspector.get_views()
        return f"Found {len(views)} views: {[v.name for v in views]}"
    finally:
        introspector.close()


@tool
def introspect_mysql_procedures() -> str:
    """
    Extract all stored procedures and functions from the source MySQL database.
    Returns procedure names, parameters, and source code.
    """
    introspector = MySQLIntrospector()
    try:
        procedures = introspector.get_procedures()
        return f"Found {len(procedures)} procedures/functions: {[p.name for p in procedures]}"
    finally:
        introspector.close()


@tool
def introspect_mysql_triggers() -> str:
    """
    Extract all triggers from the source MySQL database.
    Returns trigger names, associated tables, and trigger code.
    """
    introspector = MySQLIntrospector()
    try:
        triggers = introspector.get_triggers()
        return f"Found {len(triggers)} triggers: {[t.name for t in triggers]}"
    finally:
        introspector.close()


@tool
def get_full_mysql_schema() -> str:
    """
    Extract the complete schema from the source MySQL database.
    Includes tables, views, procedures, and triggers.
    """
    introspector = MySQLIntrospector()
    try:
        schema = introspector.get_full_schema()
        summary = (
            f"Database: {schema.database_name}\n"
            f"Tables: {len(schema.tables)}\n"
            f"Views: {len(schema.views)}\n"
            f"Procedures: {len(schema.procedures)}\n"
            f"Triggers: {len(schema.triggers)}"
        )
        return summary
    finally:
        introspector.close()
