"""
Blueprint Agent - Generates per-table blueprint JSON files containing complete context.
Each blueprint includes table schema, indexes, FKs, related views, triggers, and procedures.
"""

import json
from pathlib import Path

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase
from src.tools.artifact_manager import get_artifact_manager


class BlueprintAgent(BaseAgent):
    """
    Agent that generates per-table blueprint files.
    Each blueprint contains all information needed to generate complete SQL for a table.
    Produces: artifacts/blueprints/{table_name}.blueprint.json
    """
    
    def __init__(self):
        super().__init__(
            name="Blueprint Agent",
            description="Generates per-table blueprints with complete context",
            use_complex_model=True,  # Use oss-120b
            system_prompt="You are a database schema analyzer."
        )
        self.artifact_manager = get_artifact_manager()
        self.blueprints_dir = self.artifact_manager.artifacts_dir / "blueprints"
        self.blueprints_dir.mkdir(parents=True, exist_ok=True)
    
    def run(self, state: MigrationState) -> MigrationState:
        """Generate blueprint JSON files for each table."""
        self.log("Generating table blueprints...")
        
        if not state.schema_metadata:
            self.log("No schema metadata found!", "error")
            return state
        
        schema = state.schema_metadata
        blueprints = {}
        
        # Clear existing blueprints to avoid stale data
        self._clear_blueprints_dir()
        
        # Build lookup maps for cross-referencing
        views_by_table = self._map_views_to_tables(schema.views, schema.tables)
        triggers_by_table = self._map_triggers_to_tables(schema.triggers)
        procs_by_table = self._map_procedures_to_tables(schema.procedures, schema.tables)
        
        # Load dependency graph
        dep_graph = self._load_dependency_graph()
        
        # Detect circular dependencies from the graph
        circular_pairs = self._detect_circular_fks(schema.tables)
        if circular_pairs:
            self.log(f"⚠️ Detected circular FKs: {circular_pairs}", "warning")
        
        # Generate blueprint for each table
        for table in schema.tables:
            self.log(f"Creating blueprint: {table.name}")
            
            blueprint = self._create_table_blueprint(
                table=table,
                all_tables=schema.tables,
                views=views_by_table.get(table.name, []),
                triggers=triggers_by_table.get(table.name, []),
                procedures=procs_by_table.get(table.name, []),
                dep_graph=dep_graph,
                circular_pairs=circular_pairs  # Pass circular info
            )
            
            # Save blueprint to file
            blueprint_path = self.blueprints_dir / f"{table.name}.blueprint.json"
            with open(blueprint_path, 'w') as f:
                json.dump(blueprint, f, indent=2, default=str)
            
            blueprints[table.name] = str(blueprint_path)
            self.log(f"  ✓ Saved {blueprint_path.name}")
        
        # Save blueprint index with circular dependency info
        index = {
            "total_tables": len(blueprints),
            "blueprints": blueprints,
            "execution_order": dep_graph.get("migration_order", []),
            "circular_dependencies": list(circular_pairs)  # Store for sandbox
        }
        index_path = self.blueprints_dir / "_index.json"
        with open(index_path, 'w') as f:
            json.dump(index, f, indent=2)
        
        # Update state
        state.current_phase = MigrationPhase.DEPENDENCY_ANALYSIS
        state.artifact_paths["blueprints"] = str(self.blueprints_dir)
        
        self.log(f"Generated {len(blueprints)} blueprints", "success")
        return state
    
    def _detect_circular_fks(self, tables) -> set:
        """Detect circular foreign key relationships between tables."""
        # Build a map of table -> tables it references via FK
        fk_graph = {}
        for table in tables:
            fk_graph[table.name] = set()
            for fk in table.foreign_keys:
                referred = self._get_attr(fk, 'referred_table')
                if referred and referred != table.name:
                    fk_graph[table.name].add(referred)
        
        # Find circular pairs (A->B and B->A)
        circular_pairs = set()
        for table_a, refs in fk_graph.items():
            for table_b in refs:
                if table_b in fk_graph and table_a in fk_graph.get(table_b, set()):
                    # Found circular: A->B and B->A
                    pair = tuple(sorted([table_a, table_b]))
                    circular_pairs.add(pair)
        
        return circular_pairs
    
    def _get_attr(self, obj, key, default=None):
        """Helper to get attribute from object or dict."""
        if hasattr(obj, key):
            return getattr(obj, key)
        elif isinstance(obj, dict):
            return obj.get(key, default)
        return default
    
    def _create_table_blueprint(self, table, all_tables, views, triggers, procedures, dep_graph, circular_pairs=None) -> dict:
        """Create a comprehensive blueprint for a single table."""
        
        circular_pairs = circular_pairs or set()
        
        # Build table list for reference
        all_table_names = [t.name for t in all_tables]
        
        # Helper to get attribute from object or dict
        def get_attr(obj, key, default=None):
            if hasattr(obj, key):
                return getattr(obj, key)
            elif isinstance(obj, dict):
                return obj.get(key, default)
            return default
        
        # Check if this table is part of a circular dependency
        def is_circular_fk(table_name, ref_table):
            pair = tuple(sorted([table_name, ref_table]))
            return pair in circular_pairs
        
        # Find FK dependencies
        depends_on = []
        for fk in table.foreign_keys:
            referred = get_attr(fk, 'referred_table')
            if referred and referred != table.name:
                depends_on.append(referred)
        
        # Find tables that depend on this table
        depended_by = []
        for other_table in all_tables:
            if other_table.name == table.name:
                continue
            for fk in other_table.foreign_keys:
                if get_attr(fk, 'referred_table') == table.name:
                    depended_by.append(other_table.name)
        
        # Build column list
        columns = []
        for col in table.columns:
            columns.append({
                "name": get_attr(col, 'name'),
                "mysql_type": get_attr(col, 'type'),
                "nullable": get_attr(col, 'nullable', True),
                "default": get_attr(col, 'default'),
                "autoincrement": get_attr(col, 'autoincrement', False)
            })
        
        # Build indexes list
        indexes = []
        for idx in table.indexes:
            indexes.append({
                "name": get_attr(idx, 'name'),
                "columns": get_attr(idx, 'columns', []),
                "unique": get_attr(idx, 'unique', False)
            })
        
        # Build foreign keys list WITH circular flag
        outgoing_fks = []
        deferred_fks = []
        for fk in table.foreign_keys:
            ref_table = get_attr(fk, 'referred_table')
            fk_data = {
                "name": get_attr(fk, 'name'),
                "columns": get_attr(fk, 'columns', []),
                "references_table": ref_table,
                "references_columns": get_attr(fk, 'referred_columns', []),
                "is_deferred": is_circular_fk(table.name, ref_table)  # Mark circular!
            }
            outgoing_fks.append(fk_data)
            
            # Also collect deferred FKs separately for easy access
            if fk_data["is_deferred"]:
                deferred_fks.append(fk_data)
        
        blueprint = {
            "table_name": table.name,
            "all_tables_in_database": all_table_names,
            
            "schema": {
                "columns": columns,
                "primary_key": table.primary_key,
                "row_count": table.row_count
            },
            
            "indexes": indexes,
            
            "foreign_keys": {
                "outgoing": outgoing_fks,
                "incoming": [
                    {"from_table": t}
                    for t in depended_by
                ],
                "deferred": deferred_fks  # FKs that need ALTER TABLE (circular deps)
            },
            
            "constraints": [c.model_dump() if hasattr(c, 'model_dump') else c for c in table.constraints],
            
            "dependencies": {
                "depends_on": list(set(depends_on)),
                "depended_by": list(set(depended_by)),
                "has_circular_fk": len(deferred_fks) > 0  # Flag for easy checking
            },
            
            "related_views": [
                {
                    "name": v.get("name"),
                    "definition": v.get("definition", "")[:500]  # Truncate for token economy
                }
                for v in views
            ],
            
            "related_triggers": [
                {
                    "name": t.get("name"),
                    "timing": t.get("timing"),
                    "event": t.get("event"),
                    "source": t.get("source", "")[:300]
                }
                for t in triggers
            ],
            
            "related_procedures": [
                {
                    "name": p.get("name"),
                    "type": p.get("type"),
                    "source": p.get("source", "")[:300]
                }
                for p in procedures
            ],
            
            "conversion_hints": {
                "use_singular_table_names": True,
                "index_naming": f"idx_{table.name}_{{column_name}}",
                "fk_naming": f"fk_{table.name}_{{referenced_table}}"
            }
        }
        
        return blueprint
    
    def _map_views_to_tables(self, views, tables) -> dict:
        """Map views to tables they reference."""
        table_names = {t.name for t in tables}
        views_by_table = {name: [] for name in table_names}
        
        for view in views:
            definition = view.definition.lower() if view.definition else ""
            # Check which tables are referenced in the view
            for table_name in table_names:
                if table_name.lower() in definition or f'"{table_name}"' in definition.lower():
                    views_by_table[table_name].append({
                        "name": view.name,
                        "definition": view.definition
                    })
        
        return views_by_table
    
    def _map_triggers_to_tables(self, triggers) -> dict:
        """Map triggers to their tables."""
        triggers_by_table = {}
        
        for trigger in triggers:
            table_name = trigger.table_name
            if table_name not in triggers_by_table:
                triggers_by_table[table_name] = []
            triggers_by_table[table_name].append({
                "name": trigger.name,
                "timing": trigger.timing,
                "event": trigger.event,
                "source": trigger.source_code
            })
        
        return triggers_by_table
    
    def _map_procedures_to_tables(self, procedures, tables) -> dict:
        """Map procedures to tables they likely use."""
        table_names = {t.name for t in tables}
        procs_by_table = {name: [] for name in table_names}
        
        for proc in procedures:
            source = proc.source_code.lower() if proc.source_code else ""
            for table_name in table_names:
                if table_name.lower() in source:
                    procs_by_table[table_name].append({
                        "name": proc.name,
                        "type": proc.type,
                        "source": proc.source_code
                    })
        
        return procs_by_table
    
    def _load_dependency_graph(self) -> dict:
        """Load dependency graph from artifacts."""
        try:
            dep_path = self.artifact_manager.artifacts_dir / "dependency_graph.json"
            if dep_path.exists():
                with open(dep_path) as f:
                    return json.load(f)
        except Exception as e:
            self.log(f"Could not load dependency graph: {e}", "warning")
        return {"nodes": [], "edges": [], "migration_order": []}
    
    def _clear_blueprints_dir(self):
        """Clear the blueprints directory."""
        try:
            for item in self.blueprints_dir.iterdir():
                if item.is_file():
                    item.unlink()
            self.log("Cleared old blueprints directory")
        except Exception as e:
            self.log(f"Could not clear blueprints directory: {e}", "warning")


def blueprint_node(state: dict) -> dict:
    """LangGraph node function for blueprint generation."""
    agent = BlueprintAgent()
    
    if isinstance(state, dict):
        from src.state import MigrationState
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
