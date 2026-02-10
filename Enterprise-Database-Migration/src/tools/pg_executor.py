"""
PostgreSQL Executor - Execute DDL and queries on PostgreSQL databases.
Supports both target and sandbox databases.
"""

from typing import Any
from contextlib import contextmanager

from langchain_core.tools import tool
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import get_settings


class PostgreSQLExecutor:
    """Executes SQL on PostgreSQL databases with transaction support."""
    
    def __init__(self, connection_string: str | None = None, use_sandbox: bool = False):
        settings = get_settings()
        
        if connection_string:
            self.connection_string = connection_string
        elif use_sandbox:
            self.connection_string = settings.db.sandbox_connection_string
        else:
            self.connection_string = settings.db.target_connection_string
        
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
    
    def execute_ddl(self, ddl: str, auto_commit: bool = True) -> dict[str, Any]:
        """
        Execute a DDL statement.
        
        Returns:
            dict with keys: success, message, execution_time_ms, error
        """
        import time
        
        start_time = time.time()
        result = {
            "success": False,
            "message": "",
            "execution_time_ms": 0,
            "error": None
        }
        
        try:
            with self.engine.connect() as conn:
                if auto_commit:
                    conn.execute(text("COMMIT"))  # End any existing transaction
                
                conn.execute(text(ddl))
                
                if auto_commit:
                    conn.commit()
                
                result["success"] = True
                result["message"] = "DDL executed successfully"
                
        except Exception as e:
            result["error"] = str(e)
            result["message"] = f"DDL execution failed: {str(e)}"
        
        result["execution_time_ms"] = (time.time() - start_time) * 1000
        return result
    
    def execute_query(self, query: str) -> dict[str, Any]:
        """
        Execute a query and return results.
        
        Returns:
            dict with keys: success, rows, columns, row_count, error
        """
        result = {
            "success": False,
            "rows": [],
            "columns": [],
            "row_count": 0,
            "error": None
        }
        
        try:
            with self.engine.connect() as conn:
                cursor_result = conn.execute(text(query))
                
                if cursor_result.returns_rows:
                    result["columns"] = list(cursor_result.keys())
                    result["rows"] = [dict(row._mapping) for row in cursor_result]
                    result["row_count"] = len(result["rows"])
                else:
                    result["row_count"] = cursor_result.rowcount
                
                result["success"] = True
                
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            result = self.execute_query(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
            if result["success"] and result["rows"]:
                return result["rows"][0].get("cnt", 0)
        except Exception:
            pass
        return 0
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = :table_name
            )
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return result.scalar() or False
        except Exception:
            return False
    
    def drop_all_objects(self) -> dict[str, Any]:
        """
        Drop all objects in the database (for sandbox reset).
        USE WITH CAUTION!
        """
        result = {"success": False, "dropped": [], "errors": []}
        
        try:
            with self.engine.connect() as conn:
                # 1. Drop all views first (they depend on tables)
                views_result = conn.execute(text("""
                    SELECT table_name FROM information_schema.views 
                    WHERE table_schema = 'public'
                """))
                
                for row in views_result:
                    view_name = row[0]
                    try:
                        conn.execute(text(f'DROP VIEW IF EXISTS "{view_name}" CASCADE'))
                        result["dropped"].append(f"view:{view_name}")
                    except Exception as e:
                        result["errors"].append(f"view:{view_name}: {str(e)}")
                
                # 2. Drop all tables (CASCADE will drop triggers on them)
                tables_result = conn.execute(text("""
                    SELECT tablename FROM pg_tables 
                    WHERE schemaname = 'public'
                """))
                
                for row in tables_result:
                    table_name = row[0]
                    try:
                        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                        result["dropped"].append(f"table:{table_name}")
                    except Exception as e:
                        result["errors"].append(f"table:{table_name}: {str(e)}")
                
                # 3. Drop all sequences
                seqs_result = conn.execute(text("""
                    SELECT sequence_name FROM information_schema.sequences 
                    WHERE sequence_schema = 'public'
                """))
                
                for row in seqs_result:
                    seq_name = row[0]
                    try:
                        conn.execute(text(f'DROP SEQUENCE IF EXISTS "{seq_name}" CASCADE'))
                        result["dropped"].append(f"sequence:{seq_name}")
                    except Exception as e:
                        result["errors"].append(f"sequence:{seq_name}: {str(e)}")
                
                # 4. Drop all functions
                funcs_result = conn.execute(text("""
                    SELECT proname, oidvectortypes(proargtypes) 
                    FROM pg_proc 
                    INNER JOIN pg_namespace ns ON (pg_proc.pronamespace = ns.oid)
                    WHERE ns.nspname = 'public'
                """))
                
                for row in funcs_result:
                    func_name = row[0]
                    args = row[1]
                    try:
                        conn.execute(text(f'DROP FUNCTION IF EXISTS "{func_name}"({args}) CASCADE'))
                        result["dropped"].append(f"function:{func_name}")
                    except Exception as e:
                        result["errors"].append(f"function:{func_name}: {str(e)}")
                
                # 5. Drop all types
                types_result = conn.execute(text("""
                    SELECT typname FROM pg_type 
                    INNER JOIN pg_namespace ns ON (pg_type.typnamespace = ns.oid)
                    WHERE ns.nspname = 'public' AND typtype = 'e'
                """))
                
                for row in types_result:
                    type_name = row[0]
                    try:
                        conn.execute(text(f'DROP TYPE IF EXISTS "{type_name}" CASCADE'))
                        result["dropped"].append(f"type:{type_name}")
                    except Exception as e:
                        result["errors"].append(f"type:{type_name}: {str(e)}")
                
                conn.commit()
                result["success"] = len(result["errors"]) == 0
                
        except Exception as e:
            result["errors"].append(f"general: {str(e)}")
        
        return result
    
    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


class SandboxExecutor(PostgreSQLExecutor):
    """Executor specifically for sandbox database with reset capabilities."""
    
    def __init__(self):
        super().__init__(use_sandbox=True)
    
    def reset(self) -> dict[str, Any]:
        """Reset sandbox by dropping all objects."""
        return self.drop_all_objects()
    
    def test_ddl(self, ddl: str) -> dict[str, Any]:
        """
        Test DDL in sandbox.
        Returns execution result with timing.
        """
        return self.execute_ddl(ddl, auto_commit=True)


# LangChain Tools

@tool
def execute_postgres_ddl(ddl: str, use_sandbox: bool = True) -> str:
    """
    Execute DDL statement on PostgreSQL.
    By default uses sandbox for safety.
    
    Args:
        ddl: The DDL statement to execute
        use_sandbox: If True, execute in sandbox; if False, execute in target
        
    Returns:
        Execution result with timing
    """
    executor = PostgreSQLExecutor(use_sandbox=use_sandbox)
    try:
        result = executor.execute_ddl(ddl)
        if result["success"]:
            return f"✓ Success ({result['execution_time_ms']:.1f}ms)"
        else:
            return f"✗ Failed: {result['error']}"
    finally:
        executor.close()


@tool
def test_ddl_in_sandbox(ddl: str) -> str:
    """
    Test DDL statement in the sandbox database.
    Safe for testing transformations before applying to target.
    
    Args:
        ddl: The DDL statement to test
        
    Returns:
        Test result with execution details
    """
    executor = SandboxExecutor()
    try:
        result = executor.test_ddl(ddl)
        if result["success"]:
            return f"✓ DDL valid ({result['execution_time_ms']:.1f}ms)"
        else:
            return f"✗ DDL invalid: {result['error']}"
    finally:
        executor.close()


@tool
def reset_sandbox() -> str:
    """
    Reset the sandbox database by dropping all objects.
    Use before running a new test migration.
    
    Returns:
        Summary of dropped objects
    """
    executor = SandboxExecutor()
    try:
        result = executor.reset()
        if result["success"]:
            return f"✓ Sandbox reset. Dropped {len(result['dropped'])} objects."
        else:
            return f"Partial reset. Dropped {len(result['dropped'])}, errors: {len(result['errors'])}"
    finally:
        executor.close()


@tool
def check_table_exists(table_name: str, use_sandbox: bool = False) -> str:
    """
    Check if a table exists in PostgreSQL.
    
    Args:
        table_name: Name of the table to check
        use_sandbox: Check in sandbox or target database
        
    Returns:
        Whether the table exists
    """
    executor = PostgreSQLExecutor(use_sandbox=use_sandbox)
    try:
        exists = executor.table_exists(table_name)
        return f"Table '{table_name}': {'exists' if exists else 'does not exist'}"
    finally:
        executor.close()
