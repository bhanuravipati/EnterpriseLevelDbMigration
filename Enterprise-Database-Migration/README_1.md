# AI-Assisted Enterprise Database Migration POC
*MySQL to PostgreSQL Migration System using Multi-Agent Architecture*

## 🌟 Project Overview
This project is a Proof-of-Concept (POC) for an **AI-driven database migration engine**. It automates the complex process of migrating an enterprise-grade MySQL database (like `sakila` or `world`) to PostgreSQL. 

Unlike simple schema dump tools, this system uses a **multi-agent LLM architecture** (LangGraph) to understand, plan, transform, test, and deploy the migration, handling complex logic like stored procedures, triggers, and dependency resolution.

---

## 🏗️ Architecture & Core Components

The system is built on **Python**, **Streamlit**, and **Docker**, orchestrating a fleet of specialized AI agents.

### 🧠 The Agents (`src/agents/`)
Each agent has a specific responsibility in the migration pipeline:

1.  **`IntrospectionAgent`**: Connects to the Source MySQL DB, extracts schema metadata (tables, columns, constraints, routines), and builds a semantic understanding of the data.
2.  **`DependencyAgent`**: Analyzes Foreign Keys (FKs) to construct a directed acyclic graph (DAG) of table dependencies. This dictates the **safe deployment order**.
3.  **`BlueprintAgent`**: Generates a high-level "Implementation Plan" for each table/object, mapping MySQL types/logic to best-practice PostgreSQL equivalents.
4.  **`SchemaAgent`**: Uses LLM (Llama/GPT) to write the actual PostgreSQL DDL for tables, views, and indexes. It uses a "Two-Pass" approach for indexes to avoid circular dependency issues.
5.  **`LogicAgent`**: The "Senior Engineer" agent that transpiles complex PL/SQL (Stored Procedures, Functions, Triggers) into PL/pgSQL.
6.  **`SandboxAgent`**: **Critical verification layer.** Deploys the transformed DDL to a temporary **Sandbox PostgreSQL** instance. It catches syntax errors, dependency violations, and logic bugs *before* touching production.
7.  **`ProductionDeployAgent`**: The final executor. Takes the *validated* artifacts from the Sandbox phase and safely deploys them to the **Target Production PostgreSQL** database.
8.  **`DataMigrationAgent`**: Handles the bulk transfer of data (ETL), respecting the dependency order to prevent FK violations.
9.  **`ValidationAgent`**: Performs row counts and data integrity checks to ensure 100% fidelity.

### 🛠️ Key Utilities (`src/tools/`)
*   **`MySQLIntrospector` / `PostgreSQLExecutor`**: Database abstraction layers.
*   **`ArtifactManager`**: Manages the filesystem state (saving `.sql` files, `.json` metadata) in `artifacts/`.
*   **`SchemaValidator`**: utility for comparing data types.

### 🖥️ Application Layer
*   **`src/app.py`**: A rich **Streamlit Dashboard** acts as the control center. It visualizes the dependency graph, streams real-time agent logs, allows side-by-side "Query Lab" testing, and shows validation reports.
*   **`src/main.py`**: The LangGraph state machine definition, wiring the agents into a cohesive workflow.
*   **`src/state.py`**: Defines the shared memory (`MigrationState`) that flows between agents.

---

## 🔄 The Migration Workflow

The migration executes in **Two Major Phases**, visible in the UI:

### Phase 1: Planning & Sandbox Validation
1.  **Introspection**: Extract metadata from MySQL.
2.  **Dependency Analysis**: Build the table execution order.
3.  **Transformation**: LLM converts DDL and Logic (Procedures/Triggers).
4.  **Sandbox Testing**:
    *   **Reset**: Wipe the sandbox verify clean slate.
    *   **Deploy**: Execute DDL in dependency order.
    *   **Verify**: Ensure SQL syntax is valid for Postgres.

### Phase 2: Production Deployment
*Triggered manually by verification of Phase 1*
1.  **Artifact Loading**: Load the *validated* DDL and logic from Phase 1.
2.  **Schema Deploy**: Create tables, indexes, views, triggers in Production.
3.  **Data Load**: Bulk copy data from MySQL -> Postgres.
4.  **Verification**: Compare row counts and schema checksums.

---

## 🕵️‍♂️ Current Status & Recent Fixes (Debugging Log)

During the development of this POC, we encountered and resolved several critical challenges which define the current robust state of the codebase:

### 1. Production Deployment Parity (CRITICAL FIX)
**Issue**: The `ProductionDeployAgent` was originally reading from static `.sql` files split by type, whereas the `SandboxAgent` worked off in-memory `state`. This caused:
*   **Missing Triggers**: Triggers weren't being deployed to production.
*   **Stale DDL**: Production missed updates made during sandbox auto-fixing.
*   **Ordering Issues**: Files were read alphabetically, not by dependency.

**Fix**: Completely refactored `ProductionDeployAgent._deploy_schema` to mirror `SandboxAgent`. It now loads the full `transformed_ddl.json` state, ensuring exact parity (Tables -> Indexes -> Attributes -> Views -> Triggers -> Procedures).

### 2. Transaction Isolation & Cascade Failures
**Issue**: Originally, DDL deployment ran in a single transaction block. If *one* non-critical index failed (e.g., a spatial index issue), the *entire* transaction rolled back, leaving the database empty.
**Fix**: Implemented **Isolated Transactions**. Each DDL statement (Table create, Index add, FK constraint) now runs in its own transaction block. This ensures that valid objects persist even if individual "noisy" objects fail.

### 3. Spatial Data Support (GIST Indexes)
**Issue**: The `sakila` dataset uses `POINT` data types (for addresses). PostgreSQL B-Tree indexes (default) *cannot* index `POINT` columns. This caused deployment errors: `data type point has no default operator class`.
**Fix**: Added intelligent Auto-Fix logic in `ProductionDeployAgent`. When it detects a "no default operator class" error for a geometry type, it automatically rewrites the query to use **GiST** (`USING GIST ("location")`) and retries.

### 4. Index Concatenation Bug
**Issue**: The LLM often generated all indexes for a table as a *single* concatenated string. The execution engine treated this as one statement, executing only the first index and ignoring the rest.
**Fix**: Added explicit splitting logic. The deploy agents now split DDL payloads by `;` to ensure every single index and constraint is executed individually.

### 5. Duplicate Index "Warning"
**Status**: You currently see 44 indexes in Sandbox/Prod vs 42 in Source.
**Reason**: PostgreSQL automatically creates an internal index for `UNIQUE` constraints (named `*_key`). Our migration *also* explicitly creates the original named index from MySQL. This results in two indexes covering the same unique column(s).
**Verdict**: This is **safe and functional**, though redundant. It ensures we preserve the explicit naming conventions from the source database.

## 🚀 How to Run

1.  **Start Services**:
    ```bash
    docker-compose up -d
    ```
2.  **Launch Dashboard**:
    ```bash
    streamlit run src/app.py
    ```
3.  **Execute Phase 1**: Click "Start Migration" in the sidebar. Wait for Sandbox Validation.
4.  **Execute Phase 2**: Click "Deploy to Prod" button that appears after validation.
5.  **Verify**: Use the "Query Lab" tab to run SQL against both MySQL and Postgres simultaneously.

---
*Generated by Antigravity Agent - 2026-01-27*
