"""
Sandbox Agent - Tests DDL in isolated PostgreSQL sandbox before production.
Executes DDL in proper dependency order to avoid FK constraint failures.
"""

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase, MigrationStatus, SandboxResult
from src.tools.artifact_manager import get_artifact_manager
from src.tools.pg_executor import SandboxExecutor


class SandboxAgent(BaseAgent):
    """
    Agent responsible for testing DDL in sandbox environment.
    Executes tables in dependency order (based on foreign keys).
    Produces: sandbox_results.json artifact
    """
    
    def __init__(self):
        super().__init__(
            name="Sandbox Testing Agent",
            description="Tests all DDL in isolated sandbox before applying to target",
            use_complex_model=True,  # Use oss-120b
            system_prompt="""You are the Sandbox Testing Agent. Your job is to:

1. Reset the sandbox database
2. Execute each DDL statement in DEPENDENCY ORDER
3. Capture any errors that occur
4. Report which objects succeeded or failed
5. Provide error context for failed objects"""
        )
        self.artifact_manager = get_artifact_manager()
        self.executor = SandboxExecutor()
    
    def run(self, state: MigrationState) -> MigrationState:
        """Test all DDL in sandbox with proper dependency ordering."""
        self.log("Starting sandbox testing...")
        
        try:
            # Reset sandbox first
            self.log("Resetting sandbox...")
            reset_result = self.executor.reset()
            self.log(f"Sandbox reset: {len(reset_result.get('dropped', []))} objects dropped")
            
            sandbox_results: list[SandboxResult] = []
            
            # Get dependency order from state
            dependency_order = []
            if state.dependency_graph and state.dependency_graph.migration_order:
                dependency_order = state.dependency_graph.migration_order
                self.log(f"Using dependency order: {len(dependency_order)} objects")
            
            # Separate DDL by type
            tables = [d for d in state.transformed_ddl if d.object_type == "table"]
            indexes = [d for d in state.transformed_ddl if d.object_type == "index"]
            views = [d for d in state.transformed_ddl if d.object_type == "view"]
            deferred_fks = [d for d in state.transformed_ddl if d.object_type == "constraint"]
            
            # Sort tables by dependency order
            ordered_tables = self._sort_by_dependency(tables, dependency_order)
            
            # 1. Execute tables first (in dependency order)
            self.log(f"Executing {len(ordered_tables)} tables in dependency order...")
            for ddl in ordered_tables:
                result = self._execute_and_record(ddl, state, sandbox_results)
            
            # 2. Execute indexes AFTER all tables exist (but before FKs)
            if indexes:
                self.log(f"Executing {len(indexes)} index definitions...")
                for ddl in indexes:
                    result = self._execute_and_record(ddl, state, sandbox_results)
            
            # 3. Execute deferred FKs (circular dependencies) AFTER all tables exist
            if deferred_fks:
                self.log(f"Executing {len(deferred_fks)} deferred FK constraints...")
                for ddl in deferred_fks:
                    result = self._execute_and_record(ddl, state, sandbox_results)
            
            # 4. Execute views (after all tables and FKs exist)
            self.log(f"Executing {len(views)} views...")
            for ddl in views:
                # Fix schema references in views
                fixed_ddl = self._fix_view_schema_references(ddl.target_ddl)
                ddl.target_ddl = fixed_ddl
                result = self._execute_and_record(ddl, state, sandbox_results)
            
            # Test procedures last
            self.log(f"Executing {len(state.converted_procedures)} procedures/functions...")
            for proc in state.converted_procedures:
                self.log(f"Testing {proc.procedure_type}: {proc.name}")
                
                result = self.executor.execute_ddl(proc.target_code)
                
                sandbox_result = SandboxResult(
                    object_name=proc.name,
                    object_type=proc.procedure_type,
                    executed=result["success"],
                    execution_time_ms=result["execution_time_ms"],
                    errors=[result["error"]] if result["error"] else [],
                )
                
                if result["success"]:
                    proc.status = MigrationStatus.SUCCESS
                    self.log(f"  ✓ Success")
                else:
                    proc.status = MigrationStatus.FAILED
                    self.log(f"  ✗ Failed: {result['error'][:100]}...", "warning")
                
                sandbox_results.append(sandbox_result)
            
            # Save results
            results_summary = {
                "total": len(sandbox_results),
                "passed": len([r for r in sandbox_results if r.executed]),
                "failed": len([r for r in sandbox_results if not r.executed]),
                "results": [r.model_dump() for r in sandbox_results],
            }
            artifact_path = self.artifact_manager.save_sandbox_results(results_summary)
            
            # Update state
            state.sandbox_results = sandbox_results
            state.current_phase = MigrationPhase.SANDBOX_TESTING
            state.artifact_paths["sandbox_results"] = str(artifact_path)
            
            passed = len([r for r in sandbox_results if r.executed])
            failed = len([r for r in sandbox_results if not r.executed])
            self.log(f"Sandbox testing complete: {passed} passed, {failed} failed", 
                     "success" if failed == 0 else "warning")
            
        except Exception as e:
            self.log(f"Sandbox testing failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.SANDBOX_TESTING,
                "error_type": "sandbox_error",
                "error_message": str(e)
            })
        finally:
            self.executor.close()
        
        return state
    
    def _sort_by_dependency(self, tables: list, dependency_order: list) -> list:
        """Sort tables based on dependency graph order.
        
        Tables without foreign keys come first, then tables in dependency order.
        """
        if not dependency_order:
            # Fallback: Simple ordering - tables without FKs first
            return self._simple_dependency_sort(tables)
        
        # Build name to DDL mapping
        ddl_by_name = {ddl.object_name: ddl for ddl in tables}
        
        # Sort based on dependency order
        ordered = []
        for obj_id in dependency_order:
            # obj_id format is like "table:actor"
            if ":" in obj_id:
                obj_type, obj_name = obj_id.split(":", 1)
                if obj_type == "table" and obj_name in ddl_by_name:
                    ordered.append(ddl_by_name.pop(obj_name))
        
        # Add any remaining tables (not in dependency order)
        for ddl in ddl_by_name.values():
            ordered.append(ddl)
        
        return ordered
    
    def _simple_dependency_sort(self, tables: list) -> list:
        """Simple sort: tables without FK refs in DDL come first."""
        # Define known dependency order for Sakila
        # This is a fallback for when dependency graph isn't available
        priority_order = [
            # Level 0: No dependencies
            "actor", "category", "country", "language", "film_text",
            # Level 1: Depends on level 0
            "city",
            # Level 2: Depends on level 1
            "address", "film",
            # Level 3: Depends on level 2
            "customer", "staff", "store", "film_actor", "film_category", "inventory",
            # Level 4: Depends on level 3
            "rental",
            # Level 5: Depends on level 4
            "payment",
        ]
        
        def get_priority(ddl):
            try:
                return priority_order.index(ddl.object_name)
            except ValueError:
                return 999  # Unknown tables go last
        
        return sorted(tables, key=get_priority)
    
    def _fix_view_schema_references(self, ddl: str) -> str:
        """Remove sakila. schema prefix from view definitions."""
        # Replace sakila.table_name with just table_name
        import re
        fixed = re.sub(r'\bsakila\.(\w+)', r'\1', ddl)
        return fixed
    
    def _execute_and_record(self, ddl, state, sandbox_results: list) -> dict:
        """Execute DDL and record result."""
        self.log(f"Testing {ddl.object_type}: {ddl.object_name}")
        
        result = self.executor.execute_ddl(ddl.target_ddl)
        
        sandbox_result = SandboxResult(
            object_name=ddl.object_name,
            object_type=ddl.object_type,
            executed=result["success"],
            execution_time_ms=result["execution_time_ms"],
            errors=[result["error"]] if result["error"] else [],
            warnings=[],
            retry_count=0,
        )
        
        # If failed, try to fix and retry
        if not result["success"]:
            self.log(f"  ✗ Failed: {result['error'][:80]}...", "warning")
            fixed_ddl = self._attempt_fix(ddl.target_ddl, result["error"])
            
            if fixed_ddl != ddl.target_ddl:
                self.log("  Retrying with fixed DDL...")
                retry_result = self.executor.execute_ddl(fixed_ddl)
                
                if retry_result["success"]:
                    sandbox_result.executed = True
                    sandbox_result.errors = []
                    sandbox_result.retry_count = 1
                    ddl.target_ddl = fixed_ddl
                    ddl.status = MigrationStatus.SUCCESS
                    self.log("  ✓ Fixed and succeeded", "success")
                else:
                    sandbox_result.errors.append(retry_result["error"])
                    sandbox_result.retry_count = 1
                    ddl.status = MigrationStatus.FAILED
            else:
                ddl.status = MigrationStatus.FAILED
        else:
            ddl.status = MigrationStatus.SUCCESS
            self.log(f"  ✓ Success ({result['execution_time_ms']:.1f}ms)")
        
        sandbox_results.append(sandbox_result)
        return result
    
    def _attempt_fix(self, ddl: str, error: str) -> str:
        """Attempt to fix DDL based on error message."""
        import re
        fixes_applied = []
        fixed_ddl = ddl
        
        # Fix: Remove sakila. schema prefix
        if "sakila." in fixed_ddl:
            fixed_ddl = re.sub(r'\bsakila\.(\w+)', r'\1', fixed_ddl)
            fixes_applied.append("removed sakila. prefix")
        
        # Fix: Remove UNSIGNED if still present
        if "unsigned" in error.lower() or "UNSIGNED" in ddl:
            fixed_ddl = fixed_ddl.replace(" UNSIGNED", "").replace(" unsigned", "")
            fixes_applied.append("removed UNSIGNED")
        
        # Fix: Replace backticks with double quotes
        if "`" in fixed_ddl:
            fixed_ddl = fixed_ddl.replace("`", '"')
            fixes_applied.append("replaced backticks")
        
        # Fix: Remove ENGINE clause
        if "engine" in error.lower() or "ENGINE" in ddl:
            fixed_ddl = re.sub(r'\s*ENGINE\s*=\s*\w+', '', fixed_ddl, flags=re.IGNORECASE)
            fixes_applied.append("removed ENGINE")
        
        if fixes_applied:
            self.log(f"  Applied fixes: {', '.join(fixes_applied)}")
        
        return fixed_ddl


def sandbox_node(state: dict) -> dict:
    """LangGraph node function for sandbox testing."""
    agent = SandboxAgent()
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
