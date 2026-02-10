"""
Validation Agent - Validates schema and data integrity after migration.
"""

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase, ValidationResult
from src.tools.artifact_manager import get_artifact_manager
from src.tools.schema_validator import SchemaValidator, SchemaComparisonResult


class ValidationAgent(BaseAgent):
    """
    Agent responsible for validating migration results.
    
    Runs two types of validation:
    1. Schema Validation - Compares source MySQL schema with target PostgreSQL schema
    2. Data Validation - Compares row counts and data (after data migration)
    
    Produces: validation_report.json, schema_validation_report.json artifacts
    """
    
    def __init__(self):
        super().__init__(
            name="Validation Agent",
            description="Validates schema and data integrity between source and target databases",
            use_complex_model=True,
            system_prompt="""You are the Validation Agent. Your job is to:

1. Validate schema conversion (tables, columns, types, PKs, FKs, indexes)
2. Compare row counts between source and target (after data migration)
3. Report any discrepancies found"""
        )
        self.artifact_manager = get_artifact_manager()
    
    def run(self, state: MigrationState) -> MigrationState:
        """Run all validations."""
        self.log("Starting validation...")
        
        if not state.schema_metadata:
            self.log("No schema metadata found!", "error")
            return state
        
        validation_results: list[ValidationResult] = []
        schema_validation_passed = False
        
        try:
            # ========== PHASE 1: Schema Validation ==========
            self.log("=" * 50)
            self.log("PHASE 1: Schema Validation (MySQL vs PostgreSQL)")
            self.log("=" * 50)
            
            schema_result = self._run_schema_validation(state)
            schema_validation_passed = schema_result.passed
            
            # Convert schema validation issues to ValidationResults
            for issue in schema_result.issues:
                validation_results.append(ValidationResult(
                    validation_type=f"schema_{issue.category}",
                    object_name=issue.table_name,
                    source_value=str(issue.source_value) if issue.source_value else "",
                    target_value=str(issue.target_value) if issue.target_value else "",
                    status="fail" if issue.severity in ["critical", "warning"] else "pass",
                    details=issue.message,
                ))
            
            # Add pass results for schema checks
            for _ in range(schema_result.passed_checks):
                validation_results.append(ValidationResult(
                    validation_type="schema_check",
                    object_name="schema",
                    status="pass",
                    details="Schema element validated successfully"
                ))
            
            self.log(f"Schema validation: {schema_result.passed_checks} passed, {schema_result.failed_checks} failed")
            
            # ========== PHASE 2: Data Validation (Skip if no data migrated) ==========
            # Note: Data validation is only meaningful after data migration
            # For now, we skip it since we're only validating schema
            self.log("=" * 50)
            self.log("PHASE 2: Data Validation (Skipped - no data migrated yet)")
            self.log("=" * 50)
            self.log("Data validation will run after data migration is complete")
            
            # Calculate overall status
            passed = len([r for r in validation_results if r.status == "pass"])
            failed = len([r for r in validation_results if r.status == "fail"])
            total = len(validation_results)
            
            validation_passed = schema_validation_passed  # Only schema validation for now
            
            # Save schema validation report
            schema_report = {
                "validation_type": "schema_comparison",
                "overall_status": "pass" if schema_validation_passed else "fail",
                "total_checks": schema_result.total_checks,
                "passed_checks": schema_result.passed_checks,
                "failed_checks": schema_result.failed_checks,
                "issues": [
                    {
                        "severity": issue.severity,
                        "category": issue.category,
                        "table": issue.table_name,
                        "message": issue.message,
                        "source": str(issue.source_value),
                        "target": str(issue.target_value)
                    }
                    for issue in schema_result.issues
                ]
            }
            self.artifact_manager.save_json(schema_report, "schema_validation_report.json")
            
            # Save overall validation report
            results_summary = {
                "total_checks": total,
                "passed": passed,
                "failed": failed,
                "pass_rate": f"{(passed/total*100):.1f}%" if total > 0 else "N/A",
                "overall_status": "pass" if validation_passed else "fail",
                "schema_validation": {
                    "status": "pass" if schema_validation_passed else "fail",
                    "checks": schema_result.total_checks,
                    "passed": schema_result.passed_checks,
                    "failed": schema_result.failed_checks,
                    "critical_issues": len([i for i in schema_result.issues if i.severity == "critical"])
                },
                "data_validation": {
                    "status": "skipped",
                    "reason": "Run after data migration"
                },
                "results": [r.model_dump() for r in validation_results],
            }
            artifact_path = self.artifact_manager.save_validation_report(results_summary)
            
            # Update state
            state.validation_results = validation_results
            state.validation_passed = validation_passed
            state.current_phase = MigrationPhase.VALIDATION
            state.artifact_paths["validation_report"] = str(artifact_path)
            
            if validation_passed:
                self.log(f"✅ Validation PASSED: {passed}/{total} checks passed", "success")
            else:
                self.log(f"❌ Validation FAILED: {failed} critical issues found", "error")
            
        except Exception as e:
            self.log(f"Validation failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.VALIDATION,
                "error_type": "validation_error",
                "error_message": str(e)
            })
        
        return state
    
    def _run_schema_validation(self, state: MigrationState) -> SchemaComparisonResult:
        """Run comprehensive schema validation."""
        validator = SchemaValidator()
        
        try:
            self.log("Introspecting PostgreSQL sandbox schema...")
            validator.introspect_postgres()
            
            pg_tables = list(validator.pg_schema.get("tables", {}).keys())
            self.log(f"Found {len(pg_tables)} tables in PostgreSQL")
            
            self.log("Comparing with source MySQL schema...")
            result = validator.validate(state.schema_metadata)
            
            # Log summary by category
            categories = {}
            for issue in result.issues:
                cat = issue.category
                if cat not in categories:
                    categories[cat] = {"critical": 0, "warning": 0, "info": 0}
                categories[cat][issue.severity] += 1
            
            for cat, counts in categories.items():
                if counts["critical"] > 0:
                    self.log(f"  {cat}: {counts['critical']} critical, {counts['warning']} warnings", "warning")
                elif counts["warning"] > 0:
                    self.log(f"  {cat}: {counts['warning']} warnings", "warning")
            
            return result
            
        finally:
            validator.close()


def validation_node(state: dict) -> dict:
    """LangGraph node function for validation."""
    agent = ValidationAgent()
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()

