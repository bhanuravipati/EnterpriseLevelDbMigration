"""
Unit tests for the DataMigrator tool.
Tests data type transformations, batch processing, and helper functions.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from src.tools.data_migrator import DataMigrator, TableMigrationResult, DataMigrationResult


class TestDataTypeTransformations:
    """Test MySQL to PostgreSQL data type transformations."""
    
    def setup_method(self):
        """Create a migrator instance for testing."""
        # Don't connect to actual databases
        with patch.object(DataMigrator, 'source_engine', new_callable=Mock):
            with patch.object(DataMigrator, 'target_engine', new_callable=Mock):
                self.migrator = DataMigrator.__new__(DataMigrator)
                self.migrator.batch_size = 1000
    
    def test_transform_value_none(self):
        """Test that None values pass through unchanged."""
        result = self.migrator.transform_value(None, "VARCHAR(255)")
        assert result is None
    
    def test_transform_value_tinyint_boolean_true(self):
        """Test TINYINT(1) to boolean conversion for true value."""
        result = self.migrator.transform_value(1, "TINYINT(1)")
        assert result is True
    
    def test_transform_value_tinyint_boolean_false(self):
        """Test TINYINT(1) to boolean conversion for false value."""
        result = self.migrator.transform_value(0, "TINYINT(1)")
        assert result is False
    
    def test_transform_value_invalid_mysql_date(self):
        """Test that invalid MySQL dates (0000-00-00) become None."""
        result = self.migrator.transform_value("0000-00-00 00:00:00", "DATETIME")
        assert result is None
    
    def test_transform_value_valid_date(self):
        """Test that valid dates pass through."""
        valid_date = "2024-01-15 10:30:00"
        result = self.migrator.transform_value(valid_date, "DATETIME")
        assert result == valid_date
    
    def test_transform_value_blob_bytes(self):
        """Test that binary data passes through correctly."""
        binary_data = b'\x00\x01\x02\x03'
        result = self.migrator.transform_value(binary_data, "BLOB")
        assert result == binary_data
    
    def test_transform_value_bit_bytes(self):
        """Test BIT type conversion from bytes to int."""
        bit_value = b'\x01'  # 1 as a byte
        result = self.migrator.transform_value(bit_value, "BIT(1)")
        assert result == 1
    
    def test_transform_value_varchar(self):
        """Test that VARCHAR values pass through unchanged."""
        text = "Hello World"
        result = self.migrator.transform_value(text, "VARCHAR(255)")
        assert result == text
    
    def test_transform_value_integer(self):
        """Test that integers pass through unchanged."""
        result = self.migrator.transform_value(42, "INT")
        assert result == 42
    
    def test_transform_value_decimal(self):
        """Test that decimal values pass through unchanged."""
        from decimal import Decimal
        value = Decimal("123.45")
        result = self.migrator.transform_value(value, "DECIMAL(10,2)")
        assert result == value


class TestRowTransformation:
    """Test complete row transformation."""
    
    def setup_method(self):
        """Create a migrator instance for testing."""
        with patch.object(DataMigrator, 'source_engine', new_callable=Mock):
            with patch.object(DataMigrator, 'target_engine', new_callable=Mock):
                self.migrator = DataMigrator.__new__(DataMigrator)
                self.migrator.batch_size = 1000
    
    def test_transform_row_basic(self):
        """Test basic row transformation with multiple column types."""
        row = {
            "id": 1,
            "name": "Test User",
            "is_active": 1,
            "created_at": "2024-01-15 10:30:00"
        }
        columns = [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR(255)"},
            {"name": "is_active", "type": "TINYINT(1)"},
            {"name": "created_at", "type": "DATETIME"}
        ]
        
        result = self.migrator.transform_row(row, columns)
        
        assert result["id"] == 1
        assert result["name"] == "Test User"
        assert result["is_active"] is True
        assert result["created_at"] == "2024-01-15 10:30:00"
    
    def test_transform_row_with_nulls(self):
        """Test row transformation with NULL values."""
        row = {
            "id": 1,
            "name": None,
            "email": None
        }
        columns = [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR(255)"},
            {"name": "email", "type": "VARCHAR(255)"}
        ]
        
        result = self.migrator.transform_row(row, columns)
        
        assert result["id"] == 1
        assert result["name"] is None
        assert result["email"] is None


class TestMigrationResults:
    """Test migration result data classes."""
    
    def test_table_migration_result_success(self):
        """Test TableMigrationResult for successful migration."""
        result = TableMigrationResult(
            table_name="users",
            rows_migrated=1000,
            duration_ms=500.5,
            success=True
        )
        
        assert result.table_name == "users"
        assert result.rows_migrated == 1000
        assert result.duration_ms == 500.5
        assert result.success is True
        assert result.errors == []
    
    def test_table_migration_result_with_errors(self):
        """Test TableMigrationResult with errors."""
        result = TableMigrationResult(
            table_name="orders",
            rows_migrated=500,
            duration_ms=1000.0,
            success=False,
            errors=["FK constraint violation", "Data truncation"]
        )
        
        assert result.success is False
        assert len(result.errors) == 2
    
    def test_data_migration_result(self):
        """Test DataMigrationResult aggregation."""
        table_results = [
            TableMigrationResult("users", 100, 50.0, True),
            TableMigrationResult("orders", 200, 100.0, True)
        ]
        
        result = DataMigrationResult(
            total_rows=300,
            tables_migrated=2,
            tables_failed=0,
            total_duration_ms=150.0,
            success=True,
            table_results=table_results
        )
        
        assert result.total_rows == 300
        assert result.tables_migrated == 2
        assert result.tables_failed == 0
        assert result.success is True
        assert len(result.table_results) == 2


class TestMigrationOrder:
    """Test migration order determination."""
    
    def test_get_migration_order_with_dependency_graph(self):
        """Test that migration order uses dependency graph when available."""
        with patch.object(DataMigrator, 'source_engine', new_callable=Mock):
            with patch.object(DataMigrator, 'target_engine', new_callable=Mock):
                migrator = DataMigrator.__new__(DataMigrator)
                migrator._source_engine = Mock()
                migrator._target_engine = Mock()
        
        # Create mock dependency graph
        mock_graph = Mock()
        mock_graph.migration_order = ["countries", "cities", "addresses"]
        
        order = migrator.get_migration_order(mock_graph)
        
        assert order == ["countries", "cities", "addresses"]
    
    def test_get_migration_order_fallback_alphabetical(self):
        """Test that migration order falls back to alphabetical when no graph."""
        with patch.object(DataMigrator, 'source_engine', new_callable=Mock) as mock_engine:
            with patch.object(DataMigrator, 'target_engine', new_callable=Mock):
                migrator = DataMigrator.__new__(DataMigrator)
                migrator._source_engine = mock_engine
                migrator._target_engine = Mock()
                
                # Mock inspector
                mock_inspector = Mock()
                mock_inspector.get_table_names.return_value = ["zebra", "alpha", "beta"]
                
                with patch('src.tools.data_migrator.inspect', return_value=mock_inspector):
                    order = migrator.get_migration_order(None)
        
        assert order == ["alpha", "beta", "zebra"]  # Sorted alphabetically


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
