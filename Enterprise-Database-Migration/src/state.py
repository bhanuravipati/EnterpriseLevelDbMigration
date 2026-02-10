"""
LangGraph State definitions for the migration workflow.
Defines the shared state that flows through all agents.
"""

from datetime import datetime
from enum import Enum
from typing import Annotated, Any

from pydantic import BaseModel, Field
from langgraph.graph.message import add_messages


class MigrationPhase(str, Enum):
    """Phases of the migration workflow."""
    
    INITIALIZED = "initialized"
    INTROSPECTION = "introspection"
    DEPENDENCY_ANALYSIS = "dependency_analysis"
    PLANNING = "planning"
    SCHEMA_TRANSFORMATION = "schema_transformation"
    LOGIC_CONVERSION = "logic_conversion"
    SANDBOX_TESTING = "sandbox_testing"
    DATA_MIGRATION = "data_migration"
    VALIDATION = "validation"
    BENCHMARKING = "benchmarking"
    REPORTING = "reporting"
    COMPLETED = "completed"
    FAILED = "failed"


class MigrationStatus(str, Enum):
    """Status of the migration."""
    
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    NEEDS_RETRY = "needs_retry"


class ErrorInfo(BaseModel):
    """Information about an error that occurred."""
    
    phase: MigrationPhase
    object_name: str | None = None
    error_type: str
    error_message: str
    retry_count: int = 0
    timestamp: datetime = Field(default_factory=datetime.now)
    context: dict[str, Any] = Field(default_factory=dict)


class TableMetadata(BaseModel):
    """Metadata for a database table."""
    
    name: str
    schema_name: str = "public"
    columns: list[dict[str, Any]] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    indexes: list[dict[str, Any]] = Field(default_factory=list)
    foreign_keys: list[dict[str, Any]] = Field(default_factory=list)
    constraints: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int | None = None


class ViewMetadata(BaseModel):
    """Metadata for a database view."""
    
    name: str
    schema_name: str = "public"
    definition: str
    columns: list[dict[str, Any]] = Field(default_factory=list)


class ProcedureMetadata(BaseModel):
    """Metadata for a stored procedure or function."""
    
    name: str
    type: str  # "procedure" or "function"
    schema_name: str = "public"
    parameters: list[dict[str, Any]] = Field(default_factory=list)
    return_type: str | None = None
    source_code: str = ""


class TriggerMetadata(BaseModel):
    """Metadata for a database trigger."""
    
    name: str
    table_name: str
    timing: str  # BEFORE, AFTER
    event: str  # INSERT, UPDATE, DELETE
    source_code: str = ""


class SchemaMetadata(BaseModel):
    """Complete schema metadata from source database."""
    
    database_name: str
    database_type: str
    extracted_at: datetime = Field(default_factory=datetime.now)
    tables: list[TableMetadata] = Field(default_factory=list)
    views: list[ViewMetadata] = Field(default_factory=list)
    procedures: list[ProcedureMetadata] = Field(default_factory=list)
    triggers: list[TriggerMetadata] = Field(default_factory=list)


class DependencyNode(BaseModel):
    """A node in the dependency graph."""
    
    id: str
    name: str
    type: str  # table, view, procedure, trigger
    complexity: str = "low"  # low, medium, high


class DependencyEdge(BaseModel):
    """An edge in the dependency graph."""
    
    from_id: str
    to_id: str
    edge_type: str  # foreign_key, reference, calls


class DependencyGraph(BaseModel):
    """Dependency graph for migration ordering."""
    
    nodes: list[DependencyNode] = Field(default_factory=list)
    edges: list[DependencyEdge] = Field(default_factory=list)
    migration_order: list[str] = Field(default_factory=list)


class MigrationPlanPhase(BaseModel):
    """A phase in the migration plan."""
    
    phase_number: int
    name: str
    objects: list[str] = Field(default_factory=list)
    dependencies: list[int] = Field(default_factory=list)
    estimated_duration: str = ""
    rollback_checkpoint: bool = True


class MigrationPlan(BaseModel):
    """The complete migration plan."""
    
    strategy: str = "sequential"
    phases: list[MigrationPlanPhase] = Field(default_factory=list)
    high_risk_objects: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)


class TransformedDDL(BaseModel):
    """A transformed DDL statement."""
    
    object_name: str
    object_type: str
    source_ddl: str
    target_ddl: str
    type_mappings: list[dict[str, str]] = Field(default_factory=list)
    file_path: str = ""
    status: MigrationStatus = MigrationStatus.PENDING


class ConvertedProcedure(BaseModel):
    """A converted stored procedure."""
    
    name: str
    procedure_type: str
    source_code: str
    target_code: str
    conversion_notes: str = ""
    file_path: str = ""
    status: MigrationStatus = MigrationStatus.PENDING


class SandboxResult(BaseModel):
    """Result of executing DDL in sandbox."""
    
    object_name: str
    object_type: str
    executed: bool
    execution_time_ms: float = 0
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    retry_count: int = 0


class ValidationResult(BaseModel):
    """Result of a validation check."""
    
    validation_type: str
    object_name: str
    source_value: Any = None
    target_value: Any = None
    status: str  # pass, fail
    details: str = ""


class BenchmarkResult(BaseModel):
    """Result of a benchmark query."""
    
    query_id: str
    description: str
    source_time_ms: float
    target_time_ms: float
    status: str  # improved, stable, degraded


class DataMigrationTableResult(BaseModel):
    """Result of migrating a single table's data."""
    
    table_name: str
    rows_migrated: int
    duration_ms: float
    success: bool
    errors: list[str] = Field(default_factory=list)


# Define state reducers for list fields
def merge_errors(existing: list[ErrorInfo], new: list[ErrorInfo]) -> list[ErrorInfo]:
    """Merge error lists, keeping all errors."""
    return existing + new


def update_ddl_list(existing: list[TransformedDDL], new: list[TransformedDDL]) -> list[TransformedDDL]:
    """Update DDL list, replacing existing entries by object name."""
    result = {item.object_name: item for item in existing}
    for item in new:
        result[item.object_name] = item
    return list(result.values())


class MigrationState(BaseModel):
    """
    The complete state for the migration workflow.
    This state flows through all agents in the LangGraph.
    """
    
    # Workflow tracking
    current_phase: MigrationPhase = MigrationPhase.INITIALIZED
    overall_status: MigrationStatus = MigrationStatus.PENDING
    started_at: datetime = Field(default_factory=datetime.now)
    completed_at: datetime | None = None
    
    # Messages for agent communication
    messages: Annotated[list, add_messages] = Field(default_factory=list)
    
    # Phase 1: Introspection
    schema_metadata: SchemaMetadata | None = None
    
    # Phase 2: Dependency Analysis
    dependency_graph: DependencyGraph | None = None
    
    # Phase 3: Planning
    migration_plan: MigrationPlan | None = None
    
    # Phase 4: Schema Transformation
    transformed_ddl: list[TransformedDDL] = Field(default_factory=list)
    
    # Phase 5: Logic Conversion
    converted_procedures: list[ConvertedProcedure] = Field(default_factory=list)
    
    # Phase 6: Sandbox Testing
    sandbox_results: list[SandboxResult] = Field(default_factory=list)
    
    # Phase 7: Data Migration
    data_migration_complete: bool = False
    tables_migrated: list[str] = Field(default_factory=list)
    data_migration_results: list[DataMigrationTableResult] = Field(default_factory=list)
    data_migration_summary: dict[str, Any] = Field(default_factory=dict)
    
    # Phase 8: Validation
    validation_results: list[ValidationResult] = Field(default_factory=list)
    validation_passed: bool = False
    
    # Phase 9: Benchmarking
    benchmark_results: list[BenchmarkResult] = Field(default_factory=list)
    benchmark_passed: bool = False
    
    # Phase 10: Production Deployment
    production_deployed: bool = False
    data_migration_results: list[dict] = Field(default_factory=list)
    
    # Error tracking
    errors: list[ErrorInfo] = Field(default_factory=list)
    current_retry_count: int = 0
    sandbox_retry_count: int = 0  # Tracks retries for sandbox→schema loop (max 3)
    
    # Artifact paths
    artifact_paths: dict[str, str] = Field(default_factory=dict)
    
    class Config:
        arbitrary_types_allowed = True

