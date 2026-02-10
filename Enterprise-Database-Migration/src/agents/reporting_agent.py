"""
Reporting Agent - Generates comprehensive final migration report.
"""

import json
from datetime import datetime
from pathlib import Path

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase, MigrationStatus
from src.tools.artifact_manager import get_artifact_manager


class ReportingAgent(BaseAgent):
    """
    Agent responsible for generating the final migration report.
    Produces: migration_report.md artifact with comprehensive details.
    """
    
    def __init__(self):
        super().__init__(
            name="Reporting Agent",
            description="Generates comprehensive migration reports",
            use_complex_model=True,
            system_prompt="""You are the Reporting Agent. Generate clear, comprehensive 
migration reports that summarize the entire migration process."""
        )
        self.artifact_manager = get_artifact_manager()
    
    def run(self, state: MigrationState) -> MigrationState:
        """Generate final migration report."""
        self.log("Generating migration report...")
        
        try:
            report = self._generate_report(state)
            
            # Save report
            artifact_path = self.artifact_manager.save_migration_report(report)
            
            # Also save as JSON summary
            summary = self._generate_summary(state)
            self.artifact_manager.save_json(summary, "final_report.json")
            
            # Update state
            state.current_phase = MigrationPhase.REPORTING
            state.overall_status = MigrationStatus.SUCCESS if self._is_success(state) else MigrationStatus.FAILED
            state.completed_at = datetime.now()
            state.artifact_paths["migration_report"] = str(artifact_path)
            
            self.log(f"Report saved to {artifact_path}", "success")
            
        except Exception as e:
            self.log(f"Report generation failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.REPORTING,
                "error_type": "reporting_error",
                "error_message": str(e)
            })
        
        return state
    
    def _load_data_migration_results(self) -> dict:
        """Load data migration results from artifact file."""
        try:
            path = Path("./artifacts/data_migration_results.json")
            if path.exists():
                with open(path) as f:
                    return json.load(f)
        except Exception:
            pass
        return {}
    
    def _load_token_usage(self) -> dict:
        """Load token usage from artifact file."""
        try:
            path = Path("./artifacts/token_usage.json")
            if path.exists():
                with open(path) as f:
                    return json.load(f)
        except Exception:
            pass
        return {}
    
    def _generate_report(self, state: MigrationState) -> str:
        """Generate markdown migration report."""
        schema = state.schema_metadata
        data_results = self._load_data_migration_results()
        token_usage = self._load_token_usage()
        
        # Calculate statistics
        tables_count = len(schema.tables) if schema else 0
        procs_count = len(schema.procedures) if schema else 0
        views_count = len(schema.views) if schema else 0
        triggers_count = len(schema.triggers) if schema else 0
        
        tables_migrated = len(state.transformed_ddl) if state.transformed_ddl else 0
        procs_migrated = len(state.converted_procedures) if state.converted_procedures else 0
        
        sandbox_passed = len([r for r in state.sandbox_results if r.executed]) if state.sandbox_results else 0
        sandbox_total = len(state.sandbox_results) if state.sandbox_results else 0
        
        validation_passed = len([r for r in state.validation_results if r.status == "pass"]) if state.validation_results else 0
        validation_total = len(state.validation_results) if state.validation_results else 0
        
        # Data migration stats
        data_tables = data_results.get("tables_migrated", 0)
        data_rows = data_results.get("total_rows", 0)
        data_validation = data_results.get("validation", [])
        data_passed = len([v for v in data_validation if v.get("match", False)])
        
        duration = ""
        if state.completed_at and state.started_at:
            delta = state.completed_at - state.started_at
            minutes = int(delta.total_seconds() // 60)
            seconds = int(delta.total_seconds() % 60)
            duration = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
        
        # Overall status
        overall_success = self._is_success(state) and data_passed == len(data_validation)
        status_emoji = "✅" if overall_success else "⚠️"
        status_text = "SUCCESS" if overall_success else "COMPLETED WITH ISSUES"
        
        report = f"""# 📊 Database Migration Report

> **Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
> **Duration:** {duration}  
> **Overall Status:** {status_emoji} **{status_text}**

---

## 📋 Executive Summary

This report documents the complete migration from **MySQL** to **PostgreSQL**.

### Source Database Overview

| Metric | Count |
|--------|-------|
| Database Name | `{schema.database_name if schema else "N/A"}` |
| Tables | {tables_count} |
| Views | {views_count} |
| Stored Procedures/Functions | {procs_count} |
| Triggers | {triggers_count} |

### Migration Results

| Phase | Status | Details |
|-------|--------|---------|
| Schema Transformation | {"✅ Complete" if tables_migrated > 0 else "❌ Failed"} | {tables_migrated} DDL objects generated |
| Logic Conversion | {"✅ Complete" if procs_migrated > 0 else "⏭️ Skipped"} | {procs_migrated} procedures/functions |
| Sandbox Testing | {"✅ Passed" if sandbox_passed == sandbox_total else "⚠️ Issues"} | {sandbox_passed}/{sandbox_total} tests passed |
| Schema Validation | {"✅ Passed" if validation_passed == validation_total else "⚠️ Issues"} | {validation_passed}/{validation_total} checks |
| Data Migration | {"✅ Complete" if data_passed == len(data_validation) else "⚠️ Issues"} | {data_rows:,} rows in {data_tables} tables |
| Data Validation | {"✅ All Match" if data_passed == len(data_validation) else f"⚠️ {len(data_validation) - data_passed} mismatches"} | {data_passed}/{len(data_validation)} tables validated |

---

## 🗄️ Schema Migration Details

### Tables Converted
"""
        if state.transformed_ddl:
            report += "\n| Table | Type | Status | Type Mappings |\n"
            report += "|-------|------|--------|---------------|\n"
            for ddl in state.transformed_ddl:
                status = "✅" if ddl.status == MigrationStatus.SUCCESS else "❌"
                notes = ""
                if ddl.type_mappings:
                    notes = "; ".join([f"{m.get('source','')}->{m.get('target','')}" for m in ddl.type_mappings[:3]])
                report += f"| `{ddl.object_name}` | {ddl.object_type} | {status} | {notes[:50]} |\n"
        else:
            report += "\n_No table transformation data recorded._\n"

        report += """
### Stored Procedures & Functions
"""
        if state.converted_procedures:
            report += "\n| Name | Type | Status | Notes |\n"
            report += "|------|------|--------|-------|\n"
            for proc in state.converted_procedures:
                status = "✅" if proc.status == MigrationStatus.SUCCESS else "❌"
                notes = proc.conversion_notes[:50] if proc.conversion_notes else "N/A"
                report += f"| `{proc.name}` | {proc.procedure_type} | {status} | {notes} |\n"
        else:
            report += "\n_No stored procedure conversion data recorded._\n"

        report += """

---

## 📦 Data Migration Details

"""
        if data_results:
            report += f"""**Target Database:** {data_results.get("target", "sandbox").upper()}

### Row Counts by Table

| Table | Rows Migrated | Source | Target | Status |
|-------|---------------|--------|--------|--------|
"""
            migration_results = data_results.get("migration_results", [])
            validation_map = {v["table"]: v for v in data_validation}
            
            for result in migration_results:
                table = result.get("table", "")
                rows = result.get("rows_migrated", 0)
                val = validation_map.get(table, {})
                src = val.get("source_count", 0)
                tgt = val.get("target_count", 0)
                match = val.get("match", False)
                status = "✅" if match else "❌"
                report += f"| `{table}` | {rows:,} | {src:,} | {tgt:,} | {status} |\n"
            
            report += f"\n**Total Rows Migrated:** {data_rows:,}\n"
        else:
            report += "_Data migration results not available._\n"

        report += """

---

## 🧪 Sandbox Testing Results

"""
        if state.sandbox_results:
            report += f"**Summary:** {sandbox_passed}/{sandbox_total} tests passed\n\n"
            
            # Group by object type
            passed_objects = [r for r in state.sandbox_results if r.executed]
            failed_objects = [r for r in state.sandbox_results if not r.executed]
            
            if failed_objects:
                report += "### ❌ Failed Tests\n\n"
                for obj in failed_objects[:10]:
                    error_msg = obj.errors[0][:100] if obj.errors else 'Unknown error'
                    report += f"- `{obj.object_name}` ({obj.object_type}): {error_msg}...\n"
                if len(failed_objects) > 10:
                    report += f"- _...and {len(failed_objects) - 10} more_\n"
            
            report += "\n### ✅ Passed Tests\n\n"
            report += f"All {len(passed_objects)} objects executed successfully in sandbox.\n"
        else:
            report += "_No sandbox testing results available._\n"

        report += """

---

## ✅ Schema Validation Results

"""
        if state.validation_results:
            report += f"**Summary:** {validation_passed}/{validation_total} checks passed\n\n"
            
            failed_validations = [r for r in state.validation_results if r.status != "pass"]
            if failed_validations:
                report += "### Issues Found\n\n"
                for val in failed_validations[:15]:
                    report += f"- **{val.object_name}**: {val.details[:80] if val.details else 'Validation failed'}\n"
                if len(failed_validations) > 15:
                    report += f"- _...and {len(failed_validations) - 15} more_\n"
            else:
                report += "✅ **All schema validation checks passed!**\n"
        else:
            report += "_No validation results available._\n"

        report += """

---

## 📈 Token Usage

"""
        if token_usage:
            total_tokens = token_usage.get("total_tokens", 0)
            total_calls = token_usage.get("total_calls", 0)
            by_agent = token_usage.get("by_agent", {})
            by_model = token_usage.get("by_model", {})
            
            report += f"**Total Tokens Used:** {total_tokens:,}\n"
            report += f"**Total LLM Calls:** {total_calls}\n\n"
            
            if by_agent:
                report += "### Usage by Agent\n\n"
                report += "| Agent | Tokens |\n|-------|--------|\n"
                for agent, tokens in sorted(by_agent.items(), key=lambda x: -x[1]):
                    report += f"| {agent} | {tokens:,} |\n"
            
            if by_model:
                report += "\n### Usage by Model\n\n"
                report += "| Model | Tokens |\n|-------|--------|\n"
                for model, tokens in sorted(by_model.items(), key=lambda x: -x[1]):
                    report += f"| {model} | {tokens:,} |\n"
        else:
            report += "_Token usage data not available._\n"

        report += """

---

## ⚠️ Errors & Warnings

"""
        if state.errors:
            report += "| Phase | Error |\n|-------|-------|\n"
            for error in state.errors:
                phase = str(error.get('phase', 'Unknown'))
                msg = str(error.get('error_message', 'No message'))[:100]
                report += f"| {phase} | {msg} |\n"
        else:
            report += "✅ **No errors reported during migration.**\n"

        report += """

---

## 📁 Generated Artifacts

| Artifact | Path |
|----------|------|
"""
        for name, path in state.artifact_paths.items():
            report += f"| {name} | `{path}` |\n"
        
        report += """

---

## 📝 Recommendations

"""
        recommendations = []
        
        if sandbox_total > 0 and sandbox_passed < sandbox_total:
            recommendations.append(f"⚠️ **{sandbox_total - sandbox_passed} objects failed sandbox testing** - Review and fix manually before deploying to production")
        
        if validation_total > 0 and validation_passed < validation_total:
            recommendations.append(f"⚠️ **{validation_total - validation_passed} schema validation checks failed** - Investigate and resolve discrepancies")
        
        if data_validation and data_passed < len(data_validation):
            recommendations.append(f"⚠️ **{len(data_validation) - data_passed} tables have row count mismatches** - Verify data integrity")
        
        if not state.errors and sandbox_passed == sandbox_total and validation_passed == validation_total:
            recommendations.append("✅ Migration completed successfully with no issues!")
            recommendations.append("📊 Consider running performance benchmarks on production queries")
            recommendations.append("🔒 Review application connection strings before cutover")
            recommendations.append("📋 Test application functionality with the migrated database")
        
        if token_usage.get("total_tokens", 0) > 0:
            recommendations.append(f"💰 Total LLM token usage: {token_usage.get('total_tokens', 0):,} tokens")
        
        for rec in recommendations:
            report += f"- {rec}\n"
        
        report += """

---

*Report generated by AI-Assisted Database Migration System*
"""
        
        return report
    
    def _generate_summary(self, state: MigrationState) -> dict:
        """Generate JSON summary."""
        data_results = self._load_data_migration_results()
        
        return {
            "status": "success" if self._is_success(state) else "failed",
            "started_at": state.started_at.isoformat() if state.started_at else None,
            "completed_at": state.completed_at.isoformat() if state.completed_at else None,
            "tables_migrated": len(state.transformed_ddl) if state.transformed_ddl else 0,
            "procedures_migrated": len(state.converted_procedures) if state.converted_procedures else 0,
            "data_rows_migrated": data_results.get("total_rows", 0),
            "validation_passed": state.validation_passed,
            "errors_count": len(state.errors),
            "artifact_paths": state.artifact_paths,
        }
    
    def _is_success(self, state: MigrationState) -> bool:
        """Determine if migration was successful."""
        # No critical errors
        if state.errors:
            return False
        
        # Validation passed
        if not state.validation_passed:
            return False
        
        return True


def reporting_node(state: dict) -> dict:
    """LangGraph node function for reporting."""
    agent = ReportingAgent()
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
