"""
Test Script for Schema Validation
Uses existing artifacts to test schema comparison without running full migration.
"""

import json
import sys
import os
from pathlib import Path
from dataclasses import dataclass, field

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@dataclass
class SimpleTable:
    """Simple table wrapper for testing."""
    name: str
    columns: list = field(default_factory=list)
    primary_key: list = field(default_factory=list)
    foreign_keys: list = field(default_factory=list)
    indexes: list = field(default_factory=list)
    row_count: int = 0


@dataclass
class SimpleSchema:
    """Simple schema wrapper for testing."""
    tables: list = field(default_factory=list)


def load_schema_metadata():
    """Load existing schema metadata from artifacts."""
    metadata_path = project_root / "artifacts" / "schema_metadata.json"
    
    if not metadata_path.exists():
        print(f"❌ Error: {metadata_path} not found!")
        print("Please run a migration first to generate this file.")
        return None
    
    with open(metadata_path) as f:
        data = json.load(f)
    
    # Parse into simple wrapper
    tables = []
    for t in data.get("tables", []):
        tables.append(SimpleTable(
            name=t["name"],
            columns=t.get("columns", []),
            primary_key=t.get("primary_key", []),
            foreign_keys=t.get("foreign_keys", []),
            indexes=t.get("indexes", []),
            row_count=t.get("row_count", 0)
        ))
    
    return SimpleSchema(tables=tables)


def test_schema_validation():
    """Test the schema validation without running full migration."""
    print("=" * 60)
    print("SCHEMA VALIDATION TEST")
    print("=" * 60)
    
    # Load source metadata
    print("\n📂 Loading source schema metadata from artifacts...")
    source_metadata = load_schema_metadata()
    
    if not source_metadata:
        return False
    
    print(f"   Found {len(source_metadata.tables)} tables in source MySQL")
    for t in source_metadata.tables[:5]:
        print(f"   - {t.name}: {len(t.columns)} columns")
    if len(source_metadata.tables) > 5:
        print(f"   ... and {len(source_metadata.tables) - 5} more")
    
    # Import and run schema validation
    from src.tools.schema_validator import SchemaValidator
    
    print("\n🔍 Running schema validation...")
    validator = SchemaValidator()
    
    try:
        # Introspect PostgreSQL
        print("   Introspecting PostgreSQL sandbox...")
        pg_schema = validator.introspect_postgres()
        pg_tables = list(pg_schema.get("tables", {}).keys())
        print(f"   Found {len(pg_tables)} tables in PostgreSQL sandbox")
        
        if len(pg_tables) == 0:
            print("   ⚠️ No tables found in PostgreSQL sandbox!")
            print("   Make sure the sandbox has tables from a previous run.")
            return False
        
        # Run validation
        print("   Comparing schemas...")
        result = validator.validate(source_metadata)
        
        # Print results
        print("\n" + "=" * 60)
        print("VALIDATION RESULTS")
        print("=" * 60)
        
        print(f"\n📊 Summary:")
        print(f"   Total checks: {result.total_checks}")
        print(f"   Passed: {result.passed_checks}")
        print(f"   Failed: {result.failed_checks}")
        print(f"   Overall: {'✅ PASSED' if result.passed else '❌ FAILED'}")
        
        # Group issues by category
        if result.issues:
            print(f"\n⚠️ Issues Found ({len(result.issues)}):")
            
            categories = {}
            for issue in result.issues:
                cat = issue.category
                if cat not in categories:
                    categories[cat] = []
                categories[cat].append(issue)
            
            for cat, issues in categories.items():
                print(f"\n   [{cat.upper()}]")
                for issue in issues[:5]:  # Show first 5 per category
                    icon = "🔴" if issue.severity == "critical" else "🟡" if issue.severity == "warning" else "🔵"
                    print(f"   {icon} {issue.message}")
                    if issue.source_value and issue.target_value:
                        print(f"      Source: {issue.source_value}")
                        print(f"      Target: {issue.target_value}")
                if len(issues) > 5:
                    print(f"   ... and {len(issues) - 5} more")
        else:
            print("\n✅ No issues found!")
        
        # Save results to file
        results_file = project_root / "artifacts" / "schema_validation_test_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                "passed": result.passed,
                "total_checks": result.total_checks,
                "passed_checks": result.passed_checks,
                "failed_checks": result.failed_checks,
                "issues": [
                    {
                        "severity": i.severity,
                        "category": i.category,
                        "table": i.table_name,
                        "message": i.message,
                        "source": str(i.source_value),
                        "target": str(i.target_value)
                    }
                    for i in result.issues
                ]
            }, f, indent=2)
        print(f"\n💾 Results saved to: {results_file}")
        
        return result.passed
        
    finally:
        validator.close()


if __name__ == "__main__":
    success = test_schema_validation()
    print("\n" + "=" * 60)
    if success:
        print("✅ Schema validation test PASSED!")
    else:
        print("❌ Schema validation test FAILED - check issues above")
    print("=" * 60)

