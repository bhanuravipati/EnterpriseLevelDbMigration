"""
Artifact Manager - Handles reading/writing of migration artifacts.
Supports JSON, YAML, SQL, Markdown, and Diff formats.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from src.config import get_settings


class ArtifactManager:
    """Manages migration artifacts in various formats."""
    
    def __init__(self, artifacts_dir: Path | None = None):
        settings = get_settings()
        self.artifacts_dir = artifacts_dir or settings.app.artifacts_dir
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_path(self, filename: str, subdir: str | None = None) -> Path:
        """Get full path for an artifact file."""
        if subdir:
            path = self.artifacts_dir / subdir
            path.mkdir(parents=True, exist_ok=True)
            return path / filename
        return self.artifacts_dir / filename
    
    # JSON Operations
    def save_json(
        self, 
        data: dict | list | BaseModel, 
        filename: str, 
        subdir: str | None = None
    ) -> Path:
        """Save data as JSON file."""
        path = self._get_path(filename, subdir)
        
        if isinstance(data, BaseModel):
            content = data.model_dump(mode="json")
        else:
            content = data
        
        # Add metadata
        if isinstance(content, dict):
            content["_artifact_metadata"] = {
                "created_at": datetime.now().isoformat(),
                "version": "1.0"
            }
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, default=str)
        
        return path
    
    def load_json(self, filename: str, subdir: str | None = None) -> dict | list:
        """Load data from JSON file."""
        path = self._get_path(filename, subdir)
        
        if not path.exists():
            raise FileNotFoundError(f"Artifact not found: {path}")
        
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    # YAML Operations
    def save_yaml(
        self, 
        data: dict | list | BaseModel, 
        filename: str,
        subdir: str | None = None
    ) -> Path:
        """Save data as YAML file."""
        path = self._get_path(filename, subdir)
        
        if isinstance(data, BaseModel):
            content = data.model_dump(mode="json")
        else:
            content = data
        
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(content, f, default_flow_style=False, sort_keys=False)
        
        return path
    
    def load_yaml(self, filename: str, subdir: str | None = None) -> dict | list:
        """Load data from YAML file."""
        path = self._get_path(filename, subdir)
        
        if not path.exists():
            raise FileNotFoundError(f"Artifact not found: {path}")
        
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    
    # SQL Operations
    def save_sql(
        self, 
        sql: str, 
        filename: str, 
        subdir: str | None = None,
        header_comment: str | None = None
    ) -> Path:
        """Save SQL to file with optional header comment."""
        path = self._get_path(filename, subdir)
        
        content = ""
        if header_comment:
            content = f"-- {header_comment}\n"
            content += f"-- Generated: {datetime.now().isoformat()}\n\n"
        
        content += sql
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        
        return path
    
    def load_sql(self, filename: str, subdir: str | None = None) -> str:
        """Load SQL from file."""
        path = self._get_path(filename, subdir)
        
        if not path.exists():
            raise FileNotFoundError(f"Artifact not found: {path}")
        
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    
    # Markdown Operations
    def save_markdown(
        self, 
        content: str, 
        filename: str,
        subdir: str | None = None
    ) -> Path:
        """Save content as Markdown file."""
        path = self._get_path(filename, subdir)
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        
        return path
    
    def load_markdown(self, filename: str, subdir: str | None = None) -> str:
        """Load content from Markdown file."""
        path = self._get_path(filename, subdir)
        
        if not path.exists():
            raise FileNotFoundError(f"Artifact not found: {path}")
        
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    
    # Diff Operations
    def save_diff(
        self, 
        source: str, 
        target: str, 
        filename: str,
        source_label: str = "source",
        target_label: str = "target",
        subdir: str | None = None
    ) -> Path:
        """Save a diff between source and target."""
        import difflib
        
        path = self._get_path(filename, subdir)
        
        source_lines = source.splitlines(keepends=True)
        target_lines = target.splitlines(keepends=True)
        
        diff = difflib.unified_diff(
            source_lines,
            target_lines,
            fromfile=source_label,
            tofile=target_label
        )
        
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(diff)
        
        return path
    
    # List artifacts
    def list_artifacts(self, subdir: str | None = None, pattern: str = "*") -> list[Path]:
        """List all artifacts matching pattern."""
        search_dir = self.artifacts_dir / subdir if subdir else self.artifacts_dir
        return list(search_dir.glob(pattern))
    
    # Schema metadata shortcuts
    def save_schema_metadata(self, data: dict | BaseModel) -> Path:
        """Save schema metadata artifact."""
        return self.save_json(data, "schema_metadata.json")
    
    def load_schema_metadata(self) -> dict:
        """Load schema metadata artifact."""
        return self.load_json("schema_metadata.json")
    
    def save_dependency_graph(self, data: dict | BaseModel) -> Path:
        """Save dependency graph artifact."""
        return self.save_json(data, "dependency_graph.json")
    
    def load_dependency_graph(self) -> dict:
        """Load dependency graph artifact."""
        return self.load_json("dependency_graph.json")
    
    def save_migration_plan(self, data: dict | BaseModel) -> Path:
        """Save migration plan artifact."""
        return self.save_yaml(data, "migration_plan.yaml")
    
    def load_migration_plan(self) -> dict:
        """Load migration plan artifact."""
        return self.load_yaml("migration_plan.yaml")
    
    def save_table_ddl(self, table_name: str, ddl: str) -> Path:
        """Save table DDL artifact."""
        filename = f"{table_name}.sql"
        return self.save_sql(
            ddl, 
            filename, 
            subdir="ddl/tables",
            header_comment=f"Table: {table_name} - MySQL to PostgreSQL"
        )
    
    def save_procedure_sql(self, proc_name: str, sql: str) -> Path:
        """Save procedure SQL artifact."""
        filename = f"{proc_name}.sql"
        return self.save_sql(
            sql,
            filename,
            subdir="procedures",
            header_comment=f"Procedure: {proc_name} - Converted to PL/pgSQL"
        )
    
    def save_sandbox_results(self, data: dict | BaseModel) -> Path:
        """Save sandbox test results."""
        return self.save_json(data, "sandbox_results.json")
    
    def save_validation_report(self, data: dict | BaseModel) -> Path:
        """Save validation report."""
        return self.save_json(data, "validation_report.json")
    
    def save_benchmark_report(self, content: str) -> Path:
        """Save benchmark report as Markdown."""
        return self.save_markdown(content, "benchmark_report.md")
    
    def save_migration_report(self, content: str) -> Path:
        """Save final migration report as Markdown."""
        settings = get_settings()
        reports_dir = settings.app.reports_dir
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        path = reports_dir / "migration_report.md"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        
        return path


# Singleton instance
_artifact_manager: ArtifactManager | None = None


def get_artifact_manager() -> ArtifactManager:
    """Get or create the artifact manager instance."""
    global _artifact_manager
    if _artifact_manager is None:
        _artifact_manager = ArtifactManager()
    return _artifact_manager
