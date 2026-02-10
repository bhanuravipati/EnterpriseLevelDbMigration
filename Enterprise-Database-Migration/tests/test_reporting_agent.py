"""
Reporting Agent Tester - Test report generation without running full migration.
Uses existing artifacts to generate a report.
"""

import sys
from pathlib import Path

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import json
from datetime import datetime


def load_state_from_artifacts():
    """Load migration state from artifact files."""
    from src.state import (
        MigrationState, SchemaMetadata, DependencyGraph, 
        TransformedDDL, ConvertedProcedure, SandboxResult, 
        ValidationResult, MigrationStatus, MigrationPhase
    )
    
    artifacts_dir = Path("./artifacts")
    state = MigrationState()
    state.started_at = datetime.now()
    
    # Load schema metadata
    schema_path = artifacts_dir / "schema_metadata.json"
    if schema_path.exists():
        print(f"📂 Loading schema metadata from {schema_path}")
        with open(schema_path) as f:
            data = json.load(f)
        # Remove artifact metadata if present
        data = {k: v for k, v in data.items() if not k.startswith("_")}
        state.schema_metadata = SchemaMetadata(**data)
        print(f"   Found {len(state.schema_metadata.tables)} tables")
    
    # Load dependency graph
    dep_path = artifacts_dir / "dependency_graph.json"
    if dep_path.exists():
        print(f"📂 Loading dependency graph from {dep_path}")
        with open(dep_path) as f:
            data = json.load(f)
        data = {k: v for k, v in data.items() if not k.startswith("_")}
        state.dependency_graph = DependencyGraph(**data)
    
    # Load sandbox results
    sandbox_path = artifacts_dir / "sandbox_results.json"
    if sandbox_path.exists():
        print(f"📂 Loading sandbox results from {sandbox_path}")
        with open(sandbox_path) as f:
            data = json.load(f)
        results = data.get("results", [])
        for r in results:
            state.sandbox_results.append(SandboxResult(
                object_name=r.get("object_name", ""),
                object_type=r.get("object_type", ""),
                executed=r.get("executed", False),
                execution_time_ms=r.get("time_ms", 0),
                errors=[r.get("error", "")] if r.get("error") else []
            ))
        print(f"   Found {len(state.sandbox_results)} results")
    
    # Load validation results
    validation_path = artifacts_dir / "validation_report.json"
    if validation_path.exists():
        print(f"📂 Loading validation results from {validation_path}")
        with open(validation_path) as f:
            data = json.load(f)
        results = data.get("results", [])
        for r in results:
            state.validation_results.append(ValidationResult(
                validation_type=r.get("validation_type", ""),
                object_name=r.get("object_name", ""),
                source_value=str(r.get("source_value", "")),
                target_value=str(r.get("target_value", "")),
                status=r.get("status", "fail"),
                details=r.get("details", "")
            ))
        state.validation_passed = data.get("overall_status") == "pass"
        print(f"   Found {len(state.validation_results)} results, passed={state.validation_passed}")
    
    # Load DDL artifacts (scan directory)
    ddl_dir = artifacts_dir / "ddl" / "tables"
    if ddl_dir.exists():
        print(f"📂 Loading DDL from {ddl_dir}")
        for sql_file in ddl_dir.glob("*.sql"):
            table_name = sql_file.stem
            state.transformed_ddl.append(TransformedDDL(
                object_name=table_name,
                object_type="table",
                source_ddl="",
                target_ddl=sql_file.read_text(encoding="utf-8"),
                status=MigrationStatus.SUCCESS,
                file_path=str(sql_file)
            ))
        print(f"   Found {len(state.transformed_ddl)} DDL files")
    
    # Load procedures
    proc_dir = artifacts_dir / "procedures"
    if proc_dir.exists():
        print(f"📂 Loading procedures from {proc_dir}")
        for sql_file in proc_dir.glob("*.sql"):
            proc_name = sql_file.stem
            state.converted_procedures.append(ConvertedProcedure(
                name=proc_name,
                procedure_type="function",
                source_code="",
                target_code=sql_file.read_text(encoding="utf-8"),
                status=MigrationStatus.SUCCESS,
                file_path=str(sql_file)
            ))
        print(f"   Found {len(state.converted_procedures)} procedures")
    
    return state


def test_reporting_agent():
    """Test the reporting agent with existing artifacts."""
    print("=" * 60)
    print("REPORTING AGENT TESTER")
    print("=" * 60)
    
    # Load state from artifacts
    print("\n📥 Loading state from artifacts...")
    state = load_state_from_artifacts()
    
    # Run reporting agent
    print("\n📝 Running reporting agent...")
    from src.agents.reporting_agent import ReportingAgent
    
    agent = ReportingAgent()
    
    try:
        updated_state = agent.run(state)
        print(f"\n✅ Report generated successfully!")
        print(f"   Report path: {updated_state.artifact_paths.get('migration_report', 'N/A')}")
        
        # Show report preview
        report_path = Path("./reports/migration_report.md")
        if report_path.exists():
            content = report_path.read_text(encoding="utf-8")
            print("\n" + "=" * 60)
            print("REPORT PREVIEW (first 2000 chars)")
            print("=" * 60)
            print(content[:2000])
            print("\n... [truncated]")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Report generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_reporting_agent()
    print("\n" + "=" * 60)
    if success:
        print("✅ Reporting agent test PASSED!")
    else:
        print("❌ Reporting agent test FAILED!")
    print("=" * 60)
