"""
Production Deploy Agent - Deploys validated schema and data from sandbox to production target.
Only runs after successful sandbox validation (Phase 2 of migration).
"""

import time
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase, MigrationStatus
from src.tools.artifact_manager import get_artifact_manager
from src.config import get_settings


class ProductionDeployAgent(BaseAgent):
    """
    Agent responsible for deploying validated migration to production target.
    
    Prerequisites:
    - Schema migration completed in sandbox
    - Data migration completed in sandbox  
    - All validations passed
    
    This agent:
    1. Applies DDL to production target
    2. Migrates data to production target
    3. Validates final deployment
    """
    
    def __init__(self):
        super().__init__(
            name="Production Deploy Agent",
            description="Deploys validated migration to production target database",
            use_complex_model=False,
            system_prompt="You are the Production Deploy Agent."
        )
        self.artifact_manager = get_artifact_manager()
        
        settings = get_settings()
        
        # Source: MySQL (for data)
        self._source_engine: Engine | None = None
        self.source_conn_str = settings.db.source_connection_string
        
        # Target: Production PostgreSQL
        self._target_engine: Engine | None = None
        self.target_conn_str = settings.db.target_connection_string
        
        self.batch_size = 1000
    
    @property
    def source_engine(self) -> Engine:
        if self._source_engine is None:
            self._source_engine = create_engine(self.source_conn_str)
        return self._source_engine
    
    @property
    def target_engine(self) -> Engine:
        if self._target_engine is None:
            self._target_engine = create_engine(self.target_conn_str)
        return self._target_engine
    
    def run(self, state: MigrationState) -> MigrationState:
        """Deploy schema and data to production target."""
        self.log("🚀 Starting production deployment...")
        
        # Check prerequisites
        if not state.validation_passed:
            self.log("❌ Cannot deploy: Sandbox validation not passed!", "error")
            state.errors.append({
                "phase": MigrationPhase.DATA_MIGRATION,
                "error_type": "prerequisite_error",
                "error_message": "Sandbox validation must pass before production deployment"
            })
            return state
        
        try:
            # Phase 1: Deploy schema to production
            self.log("📋 Phase 1: Deploying schema to production...")
            schema_result = self._deploy_schema(state)
            
            if not schema_result["success"]:
                raise Exception(f"Schema deployment failed: {schema_result['error']}")
            
            self.log(f"  ✓ Deployed {schema_result['objects_deployed']} objects")
            
            # Phase 2: Migrate data to production
            self.log("📦 Phase 2: Migrating data to production...")
            data_result = self._deploy_data(state)
            
            self.log(f"  ✓ Migrated {data_result['total_rows']:,} rows in {data_result['tables_migrated']} tables")
            
            # Phase 3: Final validation
            self.log("✅ Phase 3: Validating deployment...")
            validation_result = self._validate_deployment(state)
            
            passed = len([r for r in validation_result if r["match"]])
            total = len(validation_result)
            
            if passed == total:
                self.log(f"✅ Production deployment successful! {passed}/{total} tables validated", "success")
                state.production_deployed = True
            else:
                self.log(f"⚠️ Deployment completed with issues: {passed}/{total} tables match", "warning")
            
            # Save deployment results
            deployment_summary = {
                "status": "success" if passed == total else "partial",
                "schema_deployed": schema_result,
                "data_deployed": data_result,
                "validation": validation_result,
            }
            self.artifact_manager.save_json(deployment_summary, "production_deployment.json")
            
            state.artifact_paths["production_deployment"] = str(
                self.artifact_manager.artifacts_dir / "production_deployment.json"
            )
            
        except Exception as e:
            self.log(f"❌ Production deployment failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.DATA_MIGRATION,
                "error_type": "deployment_error",
                "error_message": str(e)
            })
        finally:
            self._close_connections()
        
        return state
    
    def _deploy_schema(self, state: MigrationState) -> dict:
        """Apply DDL to production target using state (like SandboxAgent)."""
        result = {
            "success": False,
            "objects_deployed": 0,
            "errors": [],
            "error": None
        }
        
        try:
            # Optional: Reset production (clean deploy)
            # Uncomment the lines below to reset production before deploy
            self.log("  Resetting production target...")
            self._reset_target()
            
            # Separate DDL by type (same approach as SandboxAgent)
            tables = [d for d in state.transformed_ddl if d.object_type == "table"]
            indexes = [d for d in state.transformed_ddl if d.object_type == "index"]
            views = [d for d in state.transformed_ddl if d.object_type == "view"]
            constraints = [d for d in state.transformed_ddl if d.object_type == "constraint"]
            triggers = [d for d in state.transformed_ddl if d.object_type == "trigger"]
            
            self.log(f"  Found: {len(tables)} tables, {len(indexes)} indexes, {len(views)} views, {len(constraints)} FKs, {len(triggers)} triggers")
            
            # 1. Deploy tables first
            self.log(f"  Deploying {len(tables)} tables...")
            for ddl in tables:
                try:
                    with self.target_engine.connect() as conn:
                        conn.execute(text(ddl.target_ddl))
                        conn.commit()
                        result["objects_deployed"] += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        result["errors"].append(f"table:{ddl.object_name}: {str(e)[:80]}")
            
            # 2. Deploy indexes after tables
            # Note: indexes may be stored as ONE object with concatenated DDL
            total_indexes = 0
            for ddl in indexes:
                # Split by semicolon to get individual statements
                for stmt in ddl.target_ddl.split(";"):
                    stmt = stmt.strip()
                    if stmt and not stmt.startswith("--"):
                        total_indexes += 1
                        try:
                            with self.target_engine.connect() as conn:
                                conn.execute(text(stmt))
                                conn.commit()
                                result["objects_deployed"] += 1
                        except Exception as e:
                            error_str = str(e).lower()
                            if "already exists" in error_str:
                                pass  # Skip - already exists
                            elif "no default operator class" in error_str or "point" in error_str:
                                # POINT type needs GIST index, not B-tree
                                # Retry with GIST access method
                                # Syntax: CREATE INDEX "name" ON "table" USING GIST ("column")
                                import re
                                try:
                                    # Insert USING GIST before the column list (before the opening parenthesis)
                                    gist_stmt = re.sub(r'\)\s*$', ')', re.sub(r'\("', 'USING GIST ("', stmt, count=1))
                                    with self.target_engine.connect() as gist_conn:
                                        gist_conn.execute(text(gist_stmt))
                                        gist_conn.commit()
                                        result["objects_deployed"] += 1
                                        self.log(f"    Fixed POINT index with GIST method")
                                except Exception as gist_e:
                                    result["errors"].append(f"index(gist): {str(gist_e)[:80]}")
                            else:
                                result["errors"].append(f"index: {str(e)[:80]}")
            self.log(f"  Deployed {total_indexes} index statements")
            
            # 3. Deploy deferred FK constraints
            # Note: FKs may also be stored as ONE object with concatenated DDL
            total_fks = 0
            for ddl in constraints:
                # Split by semicolon to get individual statements
                for stmt in ddl.target_ddl.split(";"):
                    stmt = stmt.strip()
                    if stmt and not stmt.startswith("--"):
                        total_fks += 1
                        try:
                            with self.target_engine.connect() as conn:
                                conn.execute(text(stmt))
                                conn.commit()
                                result["objects_deployed"] += 1
                        except Exception as e:
                            if "already exists" not in str(e).lower():
                                result["errors"].append(f"fk: {str(e)[:80]}")
            self.log(f"  Deployed {total_fks} FK constraint statements")
            
            # 4. Deploy views
            self.log(f"  Deploying {len(views)} views...")
            for ddl in views:
                try:
                    with self.target_engine.connect() as conn:
                        conn.execute(text(ddl.target_ddl))
                        conn.commit()
                        result["objects_deployed"] += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        result["errors"].append(f"view:{ddl.object_name}: {str(e)[:80]}")
            
            # 5. Deploy triggers (MISSING FROM OLD CODE!)
            self.log(f"  Deploying {len(triggers)} triggers...")
            for ddl in triggers:
                try:
                    with self.target_engine.connect() as conn:
                        conn.execute(text(ddl.target_ddl))
                        conn.commit()
                        result["objects_deployed"] += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        result["errors"].append(f"trigger:{ddl.object_name}: {str(e)[:80]}")
            
            # 6. Deploy procedures/functions
            self.log(f"  Deploying {len(state.converted_procedures)} procedures/functions...")
            for proc in state.converted_procedures:
                try:
                    with self.target_engine.connect() as conn:
                        conn.execute(text(proc.target_code))
                        conn.commit()
                        result["objects_deployed"] += 1
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        result["errors"].append(f"proc:{proc.name}: {str(e)[:80]}")
            
            # Determine success
            if result["objects_deployed"] == 0:
                result["error"] = "No DDL found in state.transformed_ddl"
            elif len(result["errors"]) > 0:
                result["error"] = f"{len(result['errors'])} errors during deployment: {result['errors'][:3]}"
            else:
                result["success"] = True
            
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    def _reset_target(self) -> dict:
        """Reset production target by dropping all objects (use with caution!)."""
        from src.tools.pg_executor import PostgreSQLExecutor
        executor = PostgreSQLExecutor(use_sandbox=False)  # Use target, not sandbox
        try:
            return executor.drop_all_objects()
        finally:
            executor.close()
    
    def _deploy_data(self, state: MigrationState) -> dict:
        """Migrate data from MySQL to production target."""
        from src.agents.data_migration_agent import DataMigrationAgent
        
        # Reuse data migration agent with target (not sandbox)
        agent = DataMigrationAgent(use_sandbox=False)
        updated_state = agent.run(state)
        
        return {
            "tables_migrated": len(updated_state.tables_migrated),
            "total_rows": sum(
                r.get("rows_migrated", 0) 
                for r in updated_state.artifact_paths.get("data_migration_results", {}).get("migration_results", [])
            ) if isinstance(updated_state.artifact_paths.get("data_migration_results"), dict) else 0
        }
    
    def _validate_deployment(self, state: MigrationState) -> list[dict]:
        """Validate data in production target matches source."""
        results = []
        
        table_order = state.tables_migrated or []
        
        for table_name in table_order:
            result = {
                "table": table_name,
                "source_count": 0,
                "target_count": 0,
                "match": False
            }
            
            try:
                with self.source_engine.connect() as conn:
                    src = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                    result["source_count"] = src.scalar() or 0
                
                with self.target_engine.connect() as conn:
                    tgt = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                    result["target_count"] = tgt.scalar() or 0
                
                result["match"] = result["source_count"] == result["target_count"]
                
            except Exception as e:
                result["error"] = str(e)
            
            results.append(result)
        
        return results
    
    def _close_connections(self):
        if self._source_engine:
            self._source_engine.dispose()
        if self._target_engine:
            self._target_engine.dispose()


def production_deploy_node(state: dict) -> dict:
    """LangGraph node function for production deployment."""
    agent = ProductionDeployAgent()
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
