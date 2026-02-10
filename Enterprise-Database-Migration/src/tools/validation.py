"""
Validation Tools - Verify data integrity between source and target databases.
"""

import hashlib
from typing import Any

from langchain_core.tools import tool
from sqlalchemy import create_engine, text

from src.config import get_settings


class DataValidator:
    """Validates data integrity between source and target databases."""
    
    def __init__(self):
        settings = get_settings()
        self.source_engine = create_engine(settings.db.source_connection_string)
        self.target_engine = create_engine(settings.db.target_connection_string)
    
    def validate_row_count(self, table_name: str) -> dict[str, Any]:
        """Compare row counts between source and target."""
        result = {
            "table": table_name,
            "validation_type": "row_count",
            "source_count": 0,
            "target_count": 0,
            "status": "fail",
            "details": ""
        }
        
        try:
            # Get source count (MySQL uses backticks)
            with self.source_engine.connect() as conn:
                source_result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                result["source_count"] = source_result.scalar() or 0
            
            # Get target count (PostgreSQL uses quotes)
            with self.target_engine.connect() as conn:
                target_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                result["target_count"] = target_result.scalar() or 0
            
            if result["source_count"] == result["target_count"]:
                result["status"] = "pass"
                result["details"] = f"Row counts match: {result['source_count']}"
            else:
                result["details"] = f"Mismatch: source={result['source_count']}, target={result['target_count']}"
                
        except Exception as e:
            result["details"] = f"Error: {str(e)}"
        
        return result
    
    def validate_checksum(self, table_name: str, columns: list[str] | None = None) -> dict[str, Any]:
        """
        Compare checksums between source and target.
        Uses MD5 of concatenated column values.
        """
        result = {
            "table": table_name,
            "validation_type": "checksum",
            "source_checksum": "",
            "target_checksum": "",
            "status": "fail",
            "details": ""
        }
        
        try:
            # If no columns specified, get all columns from source
            if not columns:
                with self.source_engine.connect() as conn:
                    col_result = conn.execute(text(f"""
                        SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table
                        ORDER BY ORDINAL_POSITION
                    """), {"table": table_name})
                    columns = [row[0] for row in col_result]
            
            if not columns:
                result["details"] = "No columns found"
                return result
            
            # Build checksum query for MySQL
            mysql_cols = ", ".join([f"COALESCE(`{c}`, '')" for c in columns])
            mysql_query = f"SELECT MD5(GROUP_CONCAT(CONCAT({mysql_cols}) ORDER BY 1)) FROM `{table_name}`"
            
            # Build checksum query for PostgreSQL
            pg_cols = " || ".join([f"COALESCE(\"{c}\"::text, '')" for c in columns])
            pg_query = f"SELECT MD5(STRING_AGG({pg_cols}, '' ORDER BY 1)) FROM \"{table_name}\""
            
            with self.source_engine.connect() as conn:
                source_result = conn.execute(text(mysql_query))
                result["source_checksum"] = source_result.scalar() or ""
            
            with self.target_engine.connect() as conn:
                target_result = conn.execute(text(pg_query))
                result["target_checksum"] = target_result.scalar() or ""
            
            if result["source_checksum"] == result["target_checksum"]:
                result["status"] = "pass"
                result["details"] = "Checksums match"
            else:
                result["details"] = "Checksum mismatch - data differs"
                
        except Exception as e:
            result["details"] = f"Error: {str(e)}"
        
        return result
    
    def validate_sample(
        self, 
        table_name: str, 
        sample_size: int = 10,
        key_column: str | None = None
    ) -> dict[str, Any]:
        """
        Compare sample rows between source and target.
        """
        result = {
            "table": table_name,
            "validation_type": "sample",
            "sample_size": sample_size,
            "matches": 0,
            "mismatches": 0,
            "status": "fail",
            "details": []
        }
        
        try:
            # Get sample from source
            mysql_query = f"SELECT * FROM `{table_name}` LIMIT {sample_size}"
            with self.source_engine.connect() as conn:
                source_result = conn.execute(text(mysql_query))
                source_rows = [dict(row._mapping) for row in source_result]
            
            if not source_rows:
                result["status"] = "pass"
                result["details"] = ["No rows to compare"]
                return result
            
            # Determine key column
            if not key_column:
                key_column = list(source_rows[0].keys())[0]
            
            # Compare each row
            for source_row in source_rows:
                key_value = source_row[key_column]
                
                # Get corresponding row from target
                pg_query = f'SELECT * FROM "{table_name}" WHERE "{key_column}" = :key_val'
                with self.target_engine.connect() as conn:
                    target_result = conn.execute(text(pg_query), {"key_val": key_value})
                    target_row = target_result.fetchone()
                
                if target_row:
                    target_dict = dict(target_row._mapping)
                    # Compare values (with type coercion)
                    matches = True
                    for col, source_val in source_row.items():
                        target_val = target_dict.get(col)
                        if str(source_val) != str(target_val):
                            matches = False
                            result["details"].append(
                                f"Row {key_value}: {col} differs ('{source_val}' vs '{target_val}')"
                            )
                    
                    if matches:
                        result["matches"] += 1
                    else:
                        result["mismatches"] += 1
                else:
                    result["mismatches"] += 1
                    result["details"].append(f"Row {key_value}: not found in target")
            
            if result["mismatches"] == 0:
                result["status"] = "pass"
            
        except Exception as e:
            result["details"].append(f"Error: {str(e)}")
        
        return result
    
    def validate_foreign_keys(self, table_name: str) -> dict[str, Any]:
        """Validate that all foreign key constraints are satisfied in target."""
        result = {
            "table": table_name,
            "validation_type": "foreign_key",
            "status": "pass",
            "violations": 0,
            "details": []
        }
        
        try:
            # Get foreign key constraints from target
            fk_query = """
                SELECT
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = :table_name
            """
            
            with self.target_engine.connect() as conn:
                fk_result = conn.execute(text(fk_query), {"table_name": table_name})
                foreign_keys = list(fk_result)
                
                for fk in foreign_keys:
                    constraint_name, column, foreign_table, foreign_column = fk
                    
                    # Check for violations
                    violation_query = f'''
                        SELECT COUNT(*) FROM "{table_name}" t
                        WHERE t."{column}" IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM "{foreign_table}" f 
                            WHERE f."{foreign_column}" = t."{column}"
                        )
                    '''
                    
                    violation_result = conn.execute(text(violation_query))
                    violation_count = violation_result.scalar() or 0
                    
                    if violation_count > 0:
                        result["status"] = "fail"
                        result["violations"] += violation_count
                        result["details"].append(
                            f"FK {constraint_name}: {violation_count} violations"
                        )
                
        except Exception as e:
            result["details"].append(f"Error: {str(e)}")
        
        return result
    
    def close(self):
        """Close database connections."""
        self.source_engine.dispose()
        self.target_engine.dispose()


# LangChain Tools

@tool
def validate_table_row_count(table_name: str) -> str:
    """
    Validate that row counts match between source MySQL and target PostgreSQL.
    
    Args:
        table_name: Name of the table to validate
        
    Returns:
        Validation result with counts
    """
    validator = DataValidator()
    try:
        result = validator.validate_row_count(table_name)
        if result["status"] == "pass":
            return f"✓ {table_name}: {result['source_count']} rows match"
        else:
            return f"✗ {table_name}: {result['details']}"
    finally:
        validator.close()


@tool
def validate_table_checksum(table_name: str) -> str:
    """
    Validate data integrity using checksums between source and target.
    
    Args:
        table_name: Name of the table to validate
        
    Returns:
        Checksum validation result
    """
    validator = DataValidator()
    try:
        result = validator.validate_checksum(table_name)
        if result["status"] == "pass":
            return f"✓ {table_name}: Checksums match"
        else:
            return f"✗ {table_name}: {result['details']}"
    finally:
        validator.close()


@tool
def validate_sample_data(table_name: str, sample_size: int = 10) -> str:
    """
    Validate sample rows match between source and target.
    
    Args:
        table_name: Name of the table to validate
        sample_size: Number of rows to sample
        
    Returns:
        Sample validation result
    """
    validator = DataValidator()
    try:
        result = validator.validate_sample(table_name, sample_size)
        if result["status"] == "pass":
            return f"✓ {table_name}: {result['matches']}/{sample_size} samples match"
        else:
            details = "; ".join(result["details"][:3])
            return f"✗ {table_name}: {result['mismatches']} mismatches - {details}"
    finally:
        validator.close()
