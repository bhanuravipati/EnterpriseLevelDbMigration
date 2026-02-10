"""
Tests for artifact manager.
"""

import json
import pytest
from pathlib import Path

from src.tools.artifact_manager import ArtifactManager


class TestArtifactManager:
    """Tests for ArtifactManager class."""
    
    def test_save_and_load_json(self, artifacts_dir):
        """Test JSON save and load."""
        manager = ArtifactManager(artifacts_dir)
        
        data = {"name": "test", "value": 123}
        path = manager.save_json(data, "test.json")
        
        assert path.exists()
        
        loaded = manager.load_json("test.json")
        assert loaded["name"] == "test"
        assert loaded["value"] == 123
    
    def test_save_and_load_yaml(self, artifacts_dir):
        """Test YAML save and load."""
        manager = ArtifactManager(artifacts_dir)
        
        data = {"phases": [{"name": "phase1", "steps": ["a", "b"]}]}
        path = manager.save_yaml(data, "plan.yaml")
        
        assert path.exists()
        
        loaded = manager.load_yaml("plan.yaml")
        assert loaded["phases"][0]["name"] == "phase1"
    
    def test_save_sql(self, artifacts_dir):
        """Test SQL save."""
        manager = ArtifactManager(artifacts_dir)
        
        sql = "CREATE TABLE test (id INT);"
        path = manager.save_sql(sql, "test.sql", header_comment="Test table")
        
        assert path.exists()
        
        content = path.read_text()
        assert "CREATE TABLE" in content
        assert "Test table" in content
    
    def test_save_markdown(self, artifacts_dir):
        """Test Markdown save."""
        manager = ArtifactManager(artifacts_dir)
        
        content = "# Test Report\n\nThis is a test."
        path = manager.save_markdown(content, "report.md")
        
        assert path.exists()
        assert "# Test Report" in path.read_text()
    
    def test_save_with_subdirectory(self, artifacts_dir):
        """Test saving with subdirectory."""
        manager = ArtifactManager(artifacts_dir)
        
        path = manager.save_sql("SELECT 1;", "query.sql", subdir="ddl/tables")
        
        assert path.exists()
        assert "ddl" in str(path)
        assert "tables" in str(path)
    
    def test_list_artifacts(self, artifacts_dir):
        """Test listing artifacts."""
        manager = ArtifactManager(artifacts_dir)
        
        manager.save_json({"a": 1}, "test1.json")
        manager.save_json({"b": 2}, "test2.json")
        
        files = manager.list_artifacts(pattern="*.json")
        assert len(files) >= 2
