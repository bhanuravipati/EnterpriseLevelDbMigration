"""
LangGraph Workflow - Defines the migration workflow graph.
Includes Error Fixer agent for intelligent sandbox error resolution.
Two-Phase approach: Phase 1 (Sandbox) + Phase 2 (Production Deploy - separate).
"""

from typing import Literal

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from src.state import MigrationState, MigrationPhase
from src.agents.introspection_agent import introspection_node
from src.agents.dependency_agent import dependency_node
from src.agents.blueprint_agent import blueprint_node
from src.agents.schema_agent import schema_node
from src.agents.logic_agent import logic_node
from src.agents.sandbox_agent import sandbox_node
from src.agents.error_fixer_agent import error_fixer_node
from src.agents.data_migration_agent import data_migration_node
from src.agents.validation_agent import validation_node
from src.agents.reporting_agent import reporting_node
from src.agents.data_migration_agent import data_migration_node



def should_continue_after_sandbox(state: dict) -> Literal["validation", "error_fixer"]:
    """
    Determine next step after sandbox testing.
    Routes to error_fixer if there are failures, otherwise to validation.
    """
    sandbox_results = state.get("sandbox_results", [])
    retry_count = state.get("sandbox_retry_count", 0)
    max_retries = 5
    
    # Count failures
    failures = [r for r in sandbox_results if not r.get("executed", False)]
    total = len(sandbox_results)
    
    print(f"📊 Sandbox Results: {total - len(failures)}/{total} passed, retry count: {retry_count}/{max_retries}")
    
    # If failures and retries remaining, go to error fixer
    if failures and retry_count < max_retries:
        print(f"🔧 Routing to Error Fixer... (attempt {retry_count + 1}/{max_retries})")
        return "error_fixer"
    
    # Max retries reached or no failures - continue to validation
    if failures:
        print(f"⚠️ Proceeding with {len(failures)} failures (max {max_retries} retries reached)")
    else:
        print(f"✅ All sandbox tests passed!")
    
    return "validation"


def should_continue_after_error_fixer(state: dict) -> Literal["sandbox"]:
    """
    After error fixer, always go back to sandbox to test fixes.
    NOTE: Counter is incremented in error_fixer_agent.run(), not here.
    LangGraph only persists state changes from nodes, not routing functions.
    """
    retry_count = state.get("sandbox_retry_count", 0)
    print(f"🔄 Re-running sandbox with fixes... (retry {retry_count}/3)")
    return "sandbox"


def should_continue_after_validation(state: dict) -> Literal["data_migration", "reporting"]:
    """
<<<<<<< HEAD
    After validation, route to data_migration if validation passed,
    otherwise skip to reporting.
=======
    After validation, continue to data migration if schema validation passed.
    Otherwise skip to reporting.
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    """
    validation_passed = state.get("validation_passed", False)
    validation_results = state.get("validation_results", [])
    
    passed = len([r for r in validation_results if r.get("status") == "pass"])
    failed = len([r for r in validation_results if r.get("status") == "fail"])
    
    print(f"📊 Validation Results: {passed} passed, {failed} failed")
    
<<<<<<< HEAD
    if not validation_passed:
        print("⚠️ Validation had issues - skipping data migration, going to report")
        return "reporting"
    
    print("✅ Validation passed! Proceeding to data migration...")
    return "data_migration"
=======
    if validation_passed:
        print("✅ Schema validation passed! Proceeding to data migration...")
        return "data_migration"
    else:
        print("⚠️ Schema validation failed - skipping data migration, going to report")
        return "reporting"
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b


def create_migration_workflow(checkpointer=None):
    """
    Create the LangGraph workflow for database migration.
    
    The workflow follows this sequence:
    1. Introspection → Extract schema from MySQL
    2. Dependency → Analyze dependencies
    3. Schema → Transform DDL to PostgreSQL
    4. Logic → Convert stored procedures
    5. Sandbox → Test in isolated environment
    6. Error Fixer → Fix any errors using LLM (if needed)
    7. Validation → Verify schema integrity
    8. Data Migration → Transfer data from MySQL to PostgreSQL
    9. Reporting → Generate final report
    
    Feedback loops:
    - Sandbox failures → Error Fixer → Sandbox (max 3 retries)
    
    Flow Diagram:
    
    Introspection → Dependency → Schema → Logic → Sandbox
                                                    ↓
                                               [failures?]
                                              ↙        ↘
                                    Error Fixer        Validation
                                         ↓                 ↓
                                      Sandbox          Reporting
                                    (retry up to 3x)       ↓
                                                         END
    """
    
    # Create the graph with state schema
    workflow = StateGraph(dict)
    
    # Add nodes for each agent
    workflow.add_node("introspection", introspection_node)
    workflow.add_node("dependency", dependency_node)
    workflow.add_node("blueprint", blueprint_node)
    workflow.add_node("schema", schema_node)
    workflow.add_node("logic", logic_node)
    workflow.add_node("sandbox", sandbox_node)
    workflow.add_node("error_fixer", error_fixer_node)
    workflow.add_node("validation", validation_node)
<<<<<<< HEAD
    workflow.add_node("data_migration", data_migration_node)  # NEW: Data migration
=======
    workflow.add_node("data_migration", data_migration_node)  # Phase 1: Data to sandbox
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    workflow.add_node("reporting", reporting_node)
    
    # Set entry point
    workflow.set_entry_point("introspection")
    
    # Normal flow edges: Introspection → Dependency → Blueprint → Schema → Logic → Sandbox
    workflow.add_edge("introspection", "dependency")
    workflow.add_edge("dependency", "blueprint")  # NEW: Blueprint after dependency
    workflow.add_edge("blueprint", "schema")       # Schema uses blueprints
    workflow.add_edge("schema", "logic")
    workflow.add_edge("logic", "sandbox")
    
    # After sandbox: either go to error_fixer or validation
    workflow.add_conditional_edges(
        "sandbox",
        should_continue_after_sandbox,
        {
            "validation": "validation",
            "error_fixer": "error_fixer",  # Route to fixer on failures
        }
    )
    
    # After error_fixer: always go back to sandbox
    workflow.add_edge("error_fixer", "sandbox")
    
<<<<<<< HEAD
    # After validation: go to data_migration if passed, else to reporting
=======
    # After validation: go to data_migration if passed, else reporting
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    workflow.add_conditional_edges(
        "validation",
        should_continue_after_validation,
        {
            "data_migration": "data_migration",
            "reporting": "reporting",
        }
    )
    
    # After data migration: go to reporting
    workflow.add_edge("data_migration", "reporting")
    
    # End after reporting
    workflow.add_edge("reporting", END)
    
    # Compile with optional checkpointer
    if checkpointer:
        return workflow.compile(checkpointer=checkpointer)
    
    return workflow.compile()


def create_workflow_with_memory():
    """Create workflow with in-memory checkpointing for state persistence."""
    checkpointer = MemorySaver()
    return create_migration_workflow(checkpointer=checkpointer)


# Convenience function to run migration
def run_migration(initial_state: dict | None = None, thread_id: str = "migration-1"):
    """
    Run the complete migration workflow.
    
    Args:
        initial_state: Optional initial state dict
        thread_id: Thread ID for checkpointing
        
    Returns:
        Final state after migration
    """
    workflow = create_workflow_with_memory()
    
    # Initialize state if not provided
    if initial_state is None:
        initial_state = MigrationState().model_dump()
    
    # Run the workflow
    config = {"configurable": {"thread_id": thread_id}}
    
    final_state = None
    for state in workflow.stream(initial_state, config=config):
        # Get the latest state
        for node_name, node_state in state.items():
            final_state = node_state
            print(f"✓ Completed: {node_name}")
    
    return final_state
