"""
Dependency Agent - Analyzes object dependencies and determines migration order.
"""

from src.agents.base_agent import BaseAgent
from src.state import (
    MigrationState, 
    MigrationPhase, 
    DependencyGraph,
    DependencyNode,
    DependencyEdge,
)
from src.tools.artifact_manager import get_artifact_manager


class DependencyAgent(BaseAgent):
    """
    Agent responsible for analyzing dependencies between database objects.
    Produces: dependency_graph.json artifact
    """
    
    def __init__(self):
        super().__init__(
            name="Dependency Agent",
            description="Analyzes dependencies between database objects and determines migration order",
            use_complex_model=True,  # Use oss-120b
            system_prompt="""You are the Dependency Analysis Agent. Your job is to:

1. Analyze foreign key relationships between tables
2. Identify view dependencies on tables
3. Identify procedure/function dependencies
4. Determine the correct migration order (topological sort)
5. Classify complexity of each object for migration

Output a dependency graph that ensures objects are migrated in correct order."""
        )
        self.artifact_manager = get_artifact_manager()
    
    def run(self, state: MigrationState) -> MigrationState:
        """Build dependency graph from schema metadata."""
        self.log("Analyzing dependencies...")
        
        if not state.schema_metadata:
            self.log("No schema metadata found!", "error")
            return state
        
        try:
            schema = state.schema_metadata
            nodes: list[DependencyNode] = []
            edges: list[DependencyEdge] = []
            
            # Create nodes for all objects
            for table in schema.tables:
                complexity = self._classify_complexity(table)
                nodes.append(DependencyNode(
                    id=f"table:{table.name}",
                    name=table.name,
                    type="table",
                    complexity=complexity,
                ))
            
            for view in schema.views:
                nodes.append(DependencyNode(
                    id=f"view:{view.name}",
                    name=view.name,
                    type="view",
                    complexity="medium",
                ))
            
            for proc in schema.procedures:
                nodes.append(DependencyNode(
                    id=f"procedure:{proc.name}",
                    name=proc.name,
                    type=proc.type,
                    complexity="high" if len(proc.source_code) > 500 else "medium",
                ))
            
            for trigger in schema.triggers:
                nodes.append(DependencyNode(
                    id=f"trigger:{trigger.name}",
                    name=trigger.name,
                    type="trigger",
                    complexity="medium",
                ))
            
            # Create edges for foreign keys
            for table in schema.tables:
                for fk in table.foreign_keys:
                    edges.append(DependencyEdge(
                        from_id=f"table:{table.name}",
                        to_id=f"table:{fk['referred_table']}",
                        edge_type="foreign_key",
                    ))
            
            # Create edges for view dependencies (simple parsing)
            for view in schema.views:
                for table in schema.tables:
                    if table.name.lower() in view.definition.lower():
                        edges.append(DependencyEdge(
                            from_id=f"view:{view.name}",
                            to_id=f"table:{table.name}",
                            edge_type="reference",
                        ))
            
            # Create edges for trigger dependencies
            for trigger in schema.triggers:
                edges.append(DependencyEdge(
                    from_id=f"trigger:{trigger.name}",
                    to_id=f"table:{trigger.table_name}",
                    edge_type="reference",
                ))
            
            # Determine migration order using topological sort
            migration_order = self._topological_sort(nodes, edges)
            
            # Build dependency graph
            dep_graph = DependencyGraph(
                nodes=nodes,
                edges=edges,
                migration_order=migration_order,
            )
            
            self.log(f"Built graph with {len(nodes)} nodes, {len(edges)} edges")
            self.log(f"Migration order: {migration_order[:5]}... ({len(migration_order)} total)")
            
            # Save artifact
            artifact_path = self.artifact_manager.save_dependency_graph(dep_graph)
            self.log(f"Saved dependency graph to {artifact_path}", "success")
            
            # Update state
            state.dependency_graph = dep_graph
            state.current_phase = MigrationPhase.DEPENDENCY_ANALYSIS
            state.artifact_paths["dependency_graph"] = str(artifact_path)
            
        except Exception as e:
            self.log(f"Dependency analysis failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.DEPENDENCY_ANALYSIS,
                "error_type": "dependency_error",
                "error_message": str(e)
            })
        
        return state
    
    def _classify_complexity(self, table) -> str:
        """Classify migration complexity of a table."""
        score = 0
        
        # More columns = more complex
        if len(table.columns) > 20:
            score += 2
        elif len(table.columns) > 10:
            score += 1
        
        # More foreign keys = more complex
        if len(table.foreign_keys) > 3:
            score += 2
        elif len(table.foreign_keys) > 0:
            score += 1
        
        # Large tables = more complex
        if table.row_count and table.row_count > 100000:
            score += 2
        elif table.row_count and table.row_count > 10000:
            score += 1
        
        if score >= 4:
            return "high"
        elif score >= 2:
            return "medium"
        return "low"
    
    def _topological_sort(
        self, 
        nodes: list[DependencyNode], 
        edges: list[DependencyEdge]
    ) -> list[str]:
        """Perform topological sort to determine migration order."""
        # Build adjacency list
        # This will create an empty list and a dictionary with all the nodes and their in-degrees
        # This is used to determine the migration order
        graph: dict[str, list[str]] = {node.id: [] for node in nodes}
        in_degree: dict[str, int] = {node.id: 0 for node in nodes}
        
        # This will check whether the node is present in the graph and if it is, it will add the edge to the graph
        # and increment the in-degree of the from_id
        for edge in edges:
            if edge.from_id in graph and edge.to_id in graph:
                graph[edge.to_id].append(edge.from_id)
                in_degree[edge.from_id] += 1
        
        # Find nodes with no dependencies
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            node_id = queue.pop(0)
            result.append(node_id)
            
            for dependent in graph.get(node_id, []):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # If not all nodes are processed, there's a cycle - add remaining
        remaining = [n for n in in_degree if n not in result]
        result.extend(remaining)
        
        return result


def dependency_node(state: dict) -> dict:
    """LangGraph node function for dependency analysis."""
    agent = DependencyAgent()
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
