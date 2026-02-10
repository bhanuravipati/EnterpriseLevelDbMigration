"""
Tests for SQL transformation tools.
"""

import pytest
from src.tools.sql_transformer import SQLTransformer


class TestSQLTransformer:
    """Tests for SQLTransformer class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = SQLTransformer()
    
    def test_map_tinyint_to_smallint(self):
        """Test TINYINT mapping."""
        result = self.transformer.map_type("TINYINT")
        assert result == "SMALLINT"
    
    def test_map_datetime_to_timestamp(self):
        """Test DATETIME mapping."""
        result = self.transformer.map_type("DATETIME")
        assert result == "TIMESTAMP"
    
    def test_map_blob_to_bytea(self):
        """Test BLOB mapping."""
        result = self.transformer.map_type("BLOB")
        assert result == "BYTEA"
    
    def test_map_json_to_jsonb(self):
        """Test JSON mapping."""
        result = self.transformer.map_type("JSON")
        assert result == "JSONB"
    
    def test_map_varchar_preserves_length(self):
        """Test VARCHAR keeps length specification."""
        result = self.transformer.map_type("VARCHAR(255)")
        assert "255" in result
    
    def test_transform_simple_create_table(self):
        """Test simple CREATE TABLE transformation."""
        mysql_ddl = """
        CREATE TABLE test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        pg_ddl, notes = self.transformer.transform_ddl(mysql_ddl)
        
        # Should produce valid PostgreSQL
        assert "CREATE TABLE" in pg_ddl
        assert "AUTO_INCREMENT" not in pg_ddl
    
    def test_transform_removes_engine_clause(self):
        """Test ENGINE clause is removed."""
        mysql_ddl = "CREATE TABLE test (id INT) ENGINE=InnoDB;"
        pg_ddl, notes = self.transformer.transform_ddl(mysql_ddl)
        
        assert "ENGINE" not in pg_ddl
    
    def test_transform_handles_unsigned(self, sample_mysql_ddl):
        """Test UNSIGNED is handled."""
        pg_ddl, notes = self.transformer.transform_ddl(sample_mysql_ddl)
        
        # UNSIGNED should be removed or type should be adjusted
        assert "unsigned" not in pg_ddl.lower() or any("UNSIGNED" in n.get("source", "") for n in notes)
