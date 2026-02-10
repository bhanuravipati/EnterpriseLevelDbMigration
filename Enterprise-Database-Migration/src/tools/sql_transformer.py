"""
SQL Transformer - Convert MySQL DDL/SQL to PostgreSQL using sqlglot.
"""

from typing import Any

import sqlglot
from sqlglot import exp
from langchain_core.tools import tool


# MySQL to PostgreSQL type mappings
TYPE_MAPPINGS: dict[str, str] = {
    # Integer types
    "TINYINT": "SMALLINT",
    "TINYINT UNSIGNED": "SMALLINT",
    "SMALLINT UNSIGNED": "INTEGER",
    "MEDIUMINT": "INTEGER",
    "MEDIUMINT UNSIGNED": "INTEGER",
    "INT": "INTEGER",
    "INT UNSIGNED": "BIGINT",
    "INTEGER": "INTEGER",
    "INTEGER UNSIGNED": "BIGINT",
    "BIGINT": "BIGINT",
    "BIGINT UNSIGNED": "NUMERIC(20)",
    
    # Floating point
    "FLOAT": "REAL",
    "DOUBLE": "DOUBLE PRECISION",
    "DOUBLE PRECISION": "DOUBLE PRECISION",
    "DECIMAL": "NUMERIC",
    
    # String types
    "TINYTEXT": "TEXT",
    "TEXT": "TEXT",
    "MEDIUMTEXT": "TEXT",
    "LONGTEXT": "TEXT",
    "TINYBLOB": "BYTEA",
    "BLOB": "BYTEA",
    "MEDIUMBLOB": "BYTEA",
    "LONGBLOB": "BYTEA",
    "BINARY": "BYTEA",
    "VARBINARY": "BYTEA",
    
    # Date/Time types
    "DATETIME": "TIMESTAMP",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "TIME": "TIME",
    "YEAR": "SMALLINT",
    
    # Boolean
    "TINYINT(1)": "BOOLEAN",
    "BIT": "BOOLEAN",
    "BIT(1)": "BOOLEAN",
    
    # JSON
    "JSON": "JSONB",
    
    # Spatial (basic mapping)
    "GEOMETRY": "GEOMETRY",
    "POINT": "POINT",
}

# Function mappings
FUNCTION_MAPPINGS: dict[str, str] = {
    "NOW()": "CURRENT_TIMESTAMP",
    "CURDATE()": "CURRENT_DATE",
    "CURTIME()": "CURRENT_TIME",
    "UUID()": "gen_random_uuid()",
    "IFNULL": "COALESCE",
    "IF": "CASE",
    "CONCAT_WS": "CONCAT_WS",
    "GROUP_CONCAT": "STRING_AGG",
    "SUBSTRING_INDEX": None,  # Needs custom handling
}


class SQLTransformer:
    """Transforms MySQL SQL to PostgreSQL SQL."""
    
    def __init__(self):
        self.type_mappings = TYPE_MAPPINGS.copy()
        self.conversion_notes: list[dict[str, str]] = []
    
    def transform_ddl(self, mysql_ddl: str) -> tuple[str, list[dict[str, str]]]:
        """
        Transform MySQL DDL to PostgreSQL DDL.
        Returns the transformed DDL and a list of conversion notes.
        """
        self.conversion_notes = []
        
        try:
            # Parse and transpile using sqlglot
            result = sqlglot.transpile(
                mysql_ddl,
                read="mysql",
                write="postgres",
                pretty=True
            )
            
            if result:
                pg_ddl = result[0]
                
                # Post-process for additional transformations
                pg_ddl = self._post_process_ddl(pg_ddl, mysql_ddl)
                
                return pg_ddl, self.conversion_notes
            
            return mysql_ddl, [{"type": "error", "message": "Failed to transpile"}]
            
        except Exception as e:
            return mysql_ddl, [{"type": "error", "message": str(e)}]
    
    def _post_process_ddl(self, pg_ddl: str, original: str) -> str:
        """Apply additional transformations not handled by sqlglot."""
        
        # Handle AUTO_INCREMENT -> SERIAL
        if "AUTO_INCREMENT" in original.upper():
            self.conversion_notes.append({
                "source": "AUTO_INCREMENT",
                "target": "SERIAL/BIGSERIAL",
                "reason": "PostgreSQL uses SERIAL types for auto-increment"
            })
        
        # Handle UNSIGNED (remove if still present)
        if "UNSIGNED" in pg_ddl.upper():
            pg_ddl = pg_ddl.replace(" UNSIGNED", "").replace(" unsigned", "")
            self.conversion_notes.append({
                "source": "UNSIGNED",
                "target": "(removed)",
                "reason": "PostgreSQL doesn't support UNSIGNED, use larger type if needed"
            })
        
        # Handle ON UPDATE CURRENT_TIMESTAMP
        if "ON UPDATE CURRENT_TIMESTAMP" in original.upper():
            self.conversion_notes.append({
                "source": "ON UPDATE CURRENT_TIMESTAMP",
                "target": "Trigger function",
                "reason": "PostgreSQL requires a trigger for auto-update timestamps"
            })
        
        # Handle ENGINE= clauses
        if "ENGINE=" in pg_ddl.upper():
            import re
            pg_ddl = re.sub(r'\s*ENGINE\s*=\s*\w+', '', pg_ddl, flags=re.IGNORECASE)
        
        # Handle CHARSET/COLLATE
        import re
        pg_ddl = re.sub(r'\s*(DEFAULT\s+)?(CHARACTER\s+SET|CHARSET)\s*=?\s*\w+', '', pg_ddl, flags=re.IGNORECASE)
        pg_ddl = re.sub(r'\s*COLLATE\s*=?\s*\w+', '', pg_ddl, flags=re.IGNORECASE)
        
        return pg_ddl.strip()
    
    def transform_query(self, mysql_query: str) -> tuple[str, list[dict[str, str]]]:
        """
        Transform a MySQL query to PostgreSQL.
        Returns the transformed query and conversion notes.
        """
        self.conversion_notes = []
        
        try:
            result = sqlglot.transpile(
                mysql_query,
                read="mysql",
                write="postgres",
                pretty=True
            )
            
            if result:
                return result[0], self.conversion_notes
            
            return mysql_query, [{"type": "error", "message": "Failed to transpile"}]
            
        except Exception as e:
            return mysql_query, [{"type": "error", "message": str(e)}]
    
    def map_type(self, mysql_type: str) -> str:
        """Map a MySQL data type to PostgreSQL."""
        mysql_type_upper = mysql_type.upper().strip()
        
        # Check exact match
        if mysql_type_upper in self.type_mappings:
            return self.type_mappings[mysql_type_upper]
        
        # Check with size parameters (e.g., VARCHAR(255))
        base_type = mysql_type_upper.split("(")[0].strip()
        if base_type in self.type_mappings:
            # For types that keep size
            if base_type in ("VARCHAR", "CHAR", "DECIMAL", "NUMERIC"):
                size_part = mysql_type[len(base_type):]
                return self.type_mappings.get(base_type, base_type) + size_part
            return self.type_mappings[base_type]
        
        # Handle ENUM specially
        if mysql_type_upper.startswith("ENUM"):
            return "TEXT"  # Simplified; ideally create custom type
        
        # Handle SET specially
        if mysql_type_upper.startswith("SET"):
            return "TEXT[]"
        
        # Default: return as-is
        return mysql_type
    
    def get_type_mapping_note(self, source_type: str, target_type: str) -> dict[str, str]:
        """Create a note explaining a type mapping."""
        return {
            "source": source_type,
            "target": target_type,
            "reason": self._get_mapping_reason(source_type, target_type)
        }
    
    def _get_mapping_reason(self, source: str, target: str) -> str:
        """Get explanation for a type mapping."""
        reasons = {
            ("TINYINT", "SMALLINT"): "PostgreSQL minimum integer is SMALLINT",
            ("DATETIME", "TIMESTAMP"): "DATETIME maps to TIMESTAMP in PostgreSQL",
            ("BLOB", "BYTEA"): "PostgreSQL uses BYTEA for binary data",
            ("JSON", "JSONB"): "JSONB is preferred in PostgreSQL for JSON data",
            ("ENUM", "TEXT"): "PostgreSQL ENUMs require custom type creation",
        }
        return reasons.get((source.upper(), target.upper()), "Standard type conversion")


# LangChain Tools

@tool
def transform_mysql_to_postgres(mysql_sql: str) -> str:
    """
    Transform MySQL SQL (DDL or query) to PostgreSQL syntax.
    Uses sqlglot for accurate transpilation.
    
    Args:
        mysql_sql: The MySQL SQL statement to transform
        
    Returns:
        The PostgreSQL equivalent SQL
    """
    transformer = SQLTransformer()
    pg_sql, notes = transformer.transform_ddl(mysql_sql)
    
    result = f"PostgreSQL:\n{pg_sql}\n"
    if notes:
        result += f"\nConversion notes:\n"
        for note in notes:
            result += f"- {note.get('source', '')} → {note.get('target', '')}: {note.get('reason', '')}\n"
    
    return result


@tool
def map_mysql_type_to_postgres(mysql_type: str) -> str:
    """
    Map a MySQL data type to its PostgreSQL equivalent.
    
    Args:
        mysql_type: The MySQL data type (e.g., 'TINYINT', 'VARCHAR(255)')
        
    Returns:
        The equivalent PostgreSQL data type
    """
    transformer = SQLTransformer()
    pg_type = transformer.map_type(mysql_type)
    return f"{mysql_type} → {pg_type}"


@tool
def validate_postgres_syntax(postgres_sql: str) -> str:
    """
    Validate PostgreSQL SQL syntax using sqlglot parser.
    
    Args:
        postgres_sql: The PostgreSQL SQL to validate
        
    Returns:
        Validation result with any errors
    """
    try:
        sqlglot.parse(postgres_sql, read="postgres")
        return "Syntax is valid"
    except Exception as e:
        return f"Syntax error: {str(e)}"
