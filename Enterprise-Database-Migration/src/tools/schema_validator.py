"""
Schema Validator - Compares source MySQL schema with target PostgreSQL schema.
Validates that the schema conversion preserved all structural elements.
"""

from typing import Any
from dataclasses import dataclass, field
from src.tools.pg_executor import SandboxExecutor


# MySQL to PostgreSQL type mapping for validation
TYPE_MAPPINGS = {
    # Integer types
    "tinyint": ["smallint", "boolean", "integer"],
    "smallint": ["smallint", "integer"],
    "mediumint": ["integer"],
    "int": ["integer", "bigint", "serial"],
    "integer": ["integer", "bigint", "serial"],
    "bigint": ["bigint", "bigserial", "numeric"],
    
    # Decimal types
    "decimal": ["numeric", "decimal"],
    "numeric": ["numeric", "decimal"],
    "float": ["real", "double precision"],
    "double": ["double precision", "real"],
    
    # String types
    "char": ["character", "char", "varchar", "text"],
    "varchar": ["character varying", "varchar", "text"],
    "tinytext": ["text"],
    "text": ["text"],
    "mediumtext": ["text"],
    "longtext": ["text"],
    
    # Binary types
    "binary": ["bytea"],
    "varbinary": ["bytea"],
    "tinyblob": ["bytea"],
    "blob": ["bytea"],
    "mediumblob": ["bytea"],
    "longblob": ["bytea"],
    
    # Date/time types
    "date": ["date"],
    "time": ["time", "time without time zone", "time with time zone"],
    "datetime": ["timestamp", "timestamp without time zone", "timestamp with time zone"],
    "timestamp": ["timestamp", "timestamp without time zone", "timestamp with time zone"],
    "year": ["smallint", "integer"],
    
    # Other types
    "json": ["json", "jsonb"],
    "enum": ["text", "character varying", "varchar"],  # With CHECK constraint
    "set": ["text[]", "text", "character varying[]", "array"],  # Array in PostgreSQL
    "bit": ["bit", "bit varying"],
    "geometry": ["geometry", "point", "bytea"],  # Spatial types
    "point": ["point"],
    "null": ["point", "bytea", "text", "geometry"],  # Unknown types maps to any
}


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""
    severity: str  # 'critical', 'warning', 'info'
    category: str  # 'table', 'column', 'type', 'pk', 'fk', 'index'
    table_name: str
    message: str
    source_value: Any = None
    target_value: Any = None


@dataclass
class SchemaComparisonResult:
    """Result of schema comparison."""
    passed: bool = True
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    issues: list[ValidationIssue] = field(default_factory=list)
    
    def add_issue(self, issue: ValidationIssue):
        self.issues.append(issue)
        self.failed_checks += 1
        if issue.severity == 'critical':
            self.passed = False
    
    def add_pass(self):
        self.passed_checks += 1
        self.total_checks += 1


class SchemaValidator:
    """
    Validates PostgreSQL schema against source MySQL schema.
    Compares tables, columns, types, PKs, FKs, and indexes.
    """
    
    def __init__(self):
        self.executor = SandboxExecutor()
        self.pg_schema: dict = {}
    
    def close(self):
        """Close database connection."""
        self.executor.close()
    
    def introspect_postgres(self) -> dict:
        """Introspect PostgreSQL sandbox schema."""
        schema = {
            "tables": {},
            "foreign_keys": [],
            "indexes": []
        }
        
        # Get all tables
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        tables_result = self.executor.execute_query(tables_query)
        
        for row in tables_result.get("rows", []):
            table_name = row['table_name']
            schema["tables"][table_name] = {
                "columns": {},
                "primary_key": [],
                "foreign_keys": [],
                "indexes": []
            }
            
            # Get columns for this table
            columns_query = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{table_name}'
            ORDER BY ordinal_position;
            """
            cols_result = self.executor.execute_query(columns_query)
            
            for col in cols_result.get("rows", []):
                col_name = col['column_name']
                schema["tables"][table_name]["columns"][col_name] = {
                    "data_type": col['data_type'],
                    "nullable": col['is_nullable'] == 'YES',
                    "default": col['column_default'],
                    "char_length": col['character_maximum_length'],
                    "precision": col['numeric_precision'],
                    "scale": col['numeric_scale']
                }
            
            # Get primary key
            pk_query = f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public' 
                AND tc.table_name = '{table_name}'
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
            """
            pk_result = self.executor.execute_query(pk_query)
            schema["tables"][table_name]["primary_key"] = [row['column_name'] for row in pk_result.get("rows", [])]
            
            # Get foreign keys
            fk_query = f"""
            SELECT
                kcu.column_name,
                ccu.table_name AS references_table,
                ccu.column_name AS references_column,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.table_schema = 'public'
                AND tc.table_name = '{table_name}'
                AND tc.constraint_type = 'FOREIGN KEY';
            """
            fk_result = self.executor.execute_query(fk_query)
            for fk in fk_result.get("rows", []):
                schema["tables"][table_name]["foreign_keys"].append({
                    "column": fk['column_name'],
                    "references_table": fk['references_table'],
                    "references_column": fk['references_column'],
                    "constraint_name": fk['constraint_name']
                })
            
            # Get indexes
            idx_query = f"""
            SELECT
                i.relname AS index_name,
                a.attname AS column_name,
                am.amname AS index_type,
                ix.indisunique AS is_unique
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON i.relam = am.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relkind = 'r' 
                AND t.relname = '{table_name}'
                AND i.relname NOT LIKE '%_pkey'
            ORDER BY i.relname, a.attnum;
            """
            idx_result = self.executor.execute_query(idx_query)
            for idx in idx_result.get("rows", []):
                schema["tables"][table_name]["indexes"].append({
                    "name": idx['index_name'],
                    "column": idx['column_name'],
                    "type": idx['index_type'],
                    "unique": idx['is_unique']
                })
        
        self.pg_schema = schema
        return schema
    
    def validate(self, source_metadata) -> SchemaComparisonResult:
        """
        Validate PostgreSQL schema against source MySQL metadata.
        
        Args:
            source_metadata: The source MySQL schema metadata from introspection
            
        Returns:
            SchemaComparisonResult with all validation results
        """
        result = SchemaComparisonResult()
        
        # Introspect PostgreSQL
        self.introspect_postgres()
        
        source_tables = {t.name: t for t in source_metadata.tables}
        target_tables = self.pg_schema.get("tables", {})
        
        # 1. Table validation
        self._validate_tables(source_tables, target_tables, result)
        
        # 2. Column and type validation (per table)
        for table_name, source_table in source_tables.items():
            if table_name in target_tables:
                self._validate_columns(source_table, target_tables[table_name], result)
                self._validate_column_types(source_table, target_tables[table_name], result)
                self._validate_primary_key(source_table, target_tables[table_name], result)
                self._validate_foreign_keys(source_table, target_tables[table_name], result)
                self._validate_indexes(source_table, target_tables[table_name], result)
        
        result.total_checks = result.passed_checks + result.failed_checks
        return result
    
    def _validate_tables(self, source: dict, target: dict, result: SchemaComparisonResult):
        """Validate table count and existence."""
        source_names = set(source.keys())
        target_names = set(target.keys())
        
        # Check for missing tables
        missing = source_names - target_names
        for table in missing:
            result.add_issue(ValidationIssue(
                severity="critical",
                category="table",
                table_name=table,
                message=f"Table '{table}' missing in target PostgreSQL",
                source_value=table,
                target_value=None
            ))
        
        # Check for extra tables (info only)
        extra = target_names - source_names
        for table in extra:
            result.add_issue(ValidationIssue(
                severity="info",
                category="table",
                table_name=table,
                message=f"Extra table '{table}' in target (not in source)",
                source_value=None,
                target_value=table
            ))
        
        # Count matching tables
        matching = source_names & target_names
        for _ in matching:
            result.add_pass()
    
    def _validate_columns(self, source_table, target_table: dict, result: SchemaComparisonResult):
        """Validate column count and names."""
        table_name = source_table.name
        source_cols = {c['name'] for c in source_table.columns}
        target_cols = set(target_table.get("columns", {}).keys())
        
        # Check for missing columns
        missing = source_cols - target_cols
        for col in missing:
            result.add_issue(ValidationIssue(
                severity="critical",
                category="column",
                table_name=table_name,
                message=f"Column '{col}' missing in table '{table_name}'",
                source_value=col,
                target_value=None
            ))
        
        # Count matching columns
        matching = source_cols & target_cols
        for _ in matching:
            result.add_pass()
    
    def _validate_column_types(self, source_table, target_table: dict, result: SchemaComparisonResult):
        """Validate column type mappings."""
        table_name = source_table.name
        target_cols = target_table.get("columns", {})
        
        for col in source_table.columns:
            col_name = col['name']
            if col_name not in target_cols:
                continue  # Already reported in column validation
            
            source_type = col['type'].lower().split('(')[0].strip()  # Get base type
            target_type = target_cols[col_name]['data_type'].lower()
            
            # Skip validation for NULL/unknown source types (e.g., MySQL GEOMETRY not detected)
            if source_type in ['null', 'none', '']:
                result.add_pass()  # Accept any target type for unknown source
                continue
            
            # Check if mapping is valid
            valid_targets = TYPE_MAPPINGS.get(source_type, [])
            
            # Also allow exact match, partial match, or ARRAY type for SET
            is_valid = (
                target_type in valid_targets or
                source_type == target_type or
                any(vt in target_type for vt in valid_targets) or
                (source_type == 'set' and 'array' in target_type.lower())  # SET → ARRAY
            )
            
            if is_valid:
                result.add_pass()
            else:
                result.add_issue(ValidationIssue(
                    severity="warning",
                    category="type",
                    table_name=table_name,
                    message=f"Column '{col_name}': type mapping may be incorrect",
                    source_value=f"{source_type} (MySQL)",
                    target_value=f"{target_type} (PostgreSQL)"
                ))
    
    def _validate_primary_key(self, source_table, target_table: dict, result: SchemaComparisonResult):
        """Validate primary key columns match."""
        table_name = source_table.name
        source_pk = set(source_table.primary_key) if source_table.primary_key else set()
        target_pk = set(target_table.get("primary_key", []))
        
        if source_pk == target_pk:
            result.add_pass()
        else:
            result.add_issue(ValidationIssue(
                severity="critical",
                category="pk",
                table_name=table_name,
                message=f"Primary key mismatch in table '{table_name}'",
                source_value=list(source_pk),
                target_value=list(target_pk)
            ))
    
    def _validate_foreign_keys(self, source_table, target_table: dict, result: SchemaComparisonResult):
        """Validate foreign key mappings (full mapping, not just count)."""
        table_name = source_table.name
        
        # Build source FK set: (column, ref_table, ref_column)
        source_fks = set()
        for fk in source_table.foreign_keys:
            cols = fk.get('columns', [])
            ref_table = fk.get('referred_table', '')
            ref_cols = fk.get('referred_columns', [])
            for i, col in enumerate(cols):
                ref_col = ref_cols[i] if i < len(ref_cols) else ref_cols[0] if ref_cols else ''
                source_fks.add((col, ref_table.lower(), ref_col))
        
        # Build target FK set
        target_fks = set()
        for fk in target_table.get("foreign_keys", []):
            target_fks.add((
                fk['column'],
                fk['references_table'].lower(),
                fk['references_column']
            ))
        
        # Check for missing FKs
        missing_fks = source_fks - target_fks
        for fk in missing_fks:
            result.add_issue(ValidationIssue(
                severity="critical",
                category="fk",
                table_name=table_name,
                message=f"FK missing: {table_name}.{fk[0]} → {fk[1]}.{fk[2]}",
                source_value=f"{fk[0]} → {fk[1]}.{fk[2]}",
                target_value=None
            ))
        
        # Count matching FKs
        matching = source_fks & target_fks
        for _ in matching:
            result.add_pass()
    
    def _validate_indexes(self, source_table, target_table: dict, result: SchemaComparisonResult):
        """Validate indexes exist (not strict matching, just count)."""
        table_name = source_table.name
        
        # Count source indexes (excluding PK)
        source_idx_count = len(source_table.indexes) if hasattr(source_table, 'indexes') else 0
        target_idx_count = len(target_table.get("indexes", []))
        
        # Just check if target has indexes (not strict matching)
        if target_idx_count >= source_idx_count * 0.5:  # At least 50% of indexes
            result.add_pass()
        else:
            result.add_issue(ValidationIssue(
                severity="warning",  
                category="index",
                table_name=table_name,
                message=f"Index count differs: source={source_idx_count}, target={target_idx_count}",
                source_value=source_idx_count,
                target_value=target_idx_count
            ))
