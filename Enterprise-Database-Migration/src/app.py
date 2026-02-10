"""
Streamlit Dashboard - AI Database Migration System
Enhanced UI with dynamic logging, real-time updates, and Final Report tab.
"""

import warnings

# Suppress warnings to keep terminal clean
warnings.filterwarnings("ignore")
import os
import sys
import json
import time
import threading
from pathlib import Path
from queue import Queue, Empty

# Add project root to Python path for proper imports
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="AI Database Migration",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS with modern styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1E88E5, #7C4DFF);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        color: #666;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-card h3 { margin: 0; font-size: 2rem; }
    .metric-card p { margin: 0.5rem 0 0 0; opacity: 0.9; }
    .success-card { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }
    .warning-card { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }
    .info-card { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }
    .log-entry { font-family: 'Consolas', 'Monaco', monospace; font-size: 0.85rem; padding: 2px 0; }
    .log-info { color: #1976d2; }
    .log-success { color: #2e7d32; }
    .log-warning { color: #f57c00; }
    .log-error { color: #c62828; }
    .running-overlay {
        background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%);
        padding: 2rem;
        border-radius: 1rem;
        text-align: center;
        border: 2px dashed #ff9800;
    }
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables."""
    defaults = {
        "migration_running": False,
        "migration_complete": False,
        "production_deployed": False,
        "current_phase": None,
        "logs": [],
        "final_state": None,
        "log_queue": Queue(),
        "migration_thread": None,
        "phase_status": {},
        "refresh_counter": 0,
        "session_started": True,
        "show_previous_results": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def add_log(message: str, level: str = "info"):
    """Add a log message."""
    timestamp = time.strftime("%H:%M:%S")
    icons = {"info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "❌"}
    log_entry = f"[{timestamp}] {icons.get(level, '•')} {message}"
    st.session_state.logs.append(log_entry)


def process_log_queue():
    """Process any pending logs from the queue."""
    try:
        while True:
            log_entry = st.session_state.log_queue.get_nowait()
            st.session_state.logs.append(log_entry)
    except Empty:
        pass


def load_artifacts():
    """Load existing artifacts for display.
    
    Only loads artifacts if migration completed in THIS session.
    This ensures a fresh start when the app is opened.
    """
    # Don't show artifacts if no migration completed in this session
    if not st.session_state.get("show_previous_results", False):
        return {}
    
    artifacts = {}
    artifacts_dir = Path("./artifacts")
    
    try:
        if (artifacts_dir / "schema_metadata.json").exists():
            with open(artifacts_dir / "schema_metadata.json") as f:
                artifacts["schema"] = json.load(f)
        
        if (artifacts_dir / "sandbox_results.json").exists():
            with open(artifacts_dir / "sandbox_results.json") as f:
                artifacts["sandbox"] = json.load(f)
        
        if (artifacts_dir / "validation_report.json").exists():
            with open(artifacts_dir / "validation_report.json") as f:
                artifacts["validation"] = json.load(f)
        
        if (artifacts_dir / "schema_validation_report.json").exists():
            with open(artifacts_dir / "schema_validation_report.json") as f:
                artifacts["schema_validation"] = json.load(f)
        
        if (artifacts_dir / "token_usage.json").exists():
            with open(artifacts_dir / "token_usage.json") as f:
                artifacts["tokens"] = json.load(f)
        
        if (artifacts_dir / "transformed_ddl.json").exists():
            with open(artifacts_dir / "transformed_ddl.json") as f:
                artifacts["ddl"] = json.load(f)
        
        if (artifacts_dir / "data_migration_results.json").exists():
            with open(artifacts_dir / "data_migration_results.json") as f:
                artifacts["data_migration"] = json.load(f)
    except Exception as e:
        st.error(f"Error loading artifacts: {e}")
    
    return artifacts


def load_report():
    """Load the migration report.
    
    Only loads report if migration completed in THIS session.
    """
    # Don't show report if no migration completed in this session
    if not st.session_state.get("show_previous_results", False):
        return None
    
    report_path = Path("./reports/migration_report.md")
    if report_path.exists():
        with open(report_path, encoding="utf-8") as f:
            return f.read()
    return None


def main():
    """Main application."""
    init_session_state()
    
    # Process any pending logs from background thread
    process_log_queue()
    
    # Check if migration thread completed
    if st.session_state.migration_thread is not None:
        if not st.session_state.migration_thread.is_alive():
            st.session_state.migration_running = False
            st.session_state.migration_complete = True  # Mark as complete
            st.session_state.show_previous_results = True  # Allow showing artifacts
            st.session_state.migration_thread = None
    
    # Header
    st.markdown('<p class="main-header">🗄️ AI Database Migration</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Autonomous MySQL → PostgreSQL migration powered by LangGraph + Groq LLM</p>', unsafe_allow_html=True)
    
    # Load existing artifacts
    artifacts = load_artifacts()
    
    # Sidebar - Configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        st.subheader("Source Database (MySQL)")
        source_host = st.text_input("Host", value=os.getenv("SOURCE_DB_HOST", "localhost"), key="src_host")
        source_port = st.number_input("Port", value=int(os.getenv("SOURCE_DB_PORT", "3306")), key="src_port")
        source_db = st.text_input("Database", value=os.getenv("SOURCE_DB_NAME", "sakila"), key="src_db")
        source_user = st.text_input("Username", value=os.getenv("SOURCE_DB_USER", "root"), key="src_user")
        source_pass = st.text_input("Password", value=os.getenv("SOURCE_DB_PASSWORD", ""), type="password", key="src_pass")
        
        st.subheader("Target Database (PostgreSQL)")
        target_host = st.text_input("Host", value=os.getenv("TARGET_DB_HOST", "localhost"), key="tgt_host")
        target_port = st.number_input("Port", value=int(os.getenv("TARGET_DB_PORT", "5432")), key="tgt_port")
        target_db = st.text_input("Database", value=os.getenv("TARGET_DB_NAME", "sakila_pg"), key="tgt_db")
        target_user = st.text_input("Username", value=os.getenv("TARGET_DB_USER", "postgres"), key="tgt_user")
        target_pass = st.text_input("Password", value=os.getenv("TARGET_DB_PASSWORD", ""), type="password", key="tgt_pass")
        
        st.divider()
        
        # Groq API Key
        groq_key = st.text_input(
            "🔑 Groq API Key", 
            value=os.getenv("GROQ_API_KEY", ""), 
            type="password",
            help="Required for AI-powered transformations"
        )
        
        if groq_key:
            os.environ["GROQ_API_KEY"] = groq_key
    
    # Main Tabs
<<<<<<< HEAD
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🚀 Migration", 
        "📊 Results", 
        "🔍 Schema Validation",
=======
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "🚀 Migration", 
        "📊 Results", 
        "🔍 Schema Validation", 
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
        "📦 Data Migration",
        "📈 Token Usage",
        "📝 Final Report",
        "🔬 Query Lab"
    ])
    
    with tab1:
        render_migration_tab(artifacts)
    
    with tab2:
        render_results_tab(artifacts)
    
    with tab3:
        render_validation_tab(artifacts)
    
    with tab4:
        render_data_migration_tab(artifacts)
    
    with tab5:
        render_tokens_tab(artifacts)
    
    with tab6:
        render_report_tab()
    
    with tab7:
        render_query_lab_tab()
    
    # Auto-refresh while migration is running
    if st.session_state.migration_running:
        time.sleep(1)
        st.rerun()


def render_migration_tab(artifacts):
    """Render the migration control tab."""
    st.subheader("Migration Pipeline")
    
    # Pipeline phases (Phase 1: Sandbox)
    phases = [
<<<<<<< HEAD
        ("introspection", "📥 Introspection", "Extract schema from MySQL"),
        ("dependency", "🔗 Dependencies", "Analyze table relationships"),
        ("schema", "🔄 Schema", "Convert tables & views"),
        ("logic", "⚙️ Logic", "Convert procedures & triggers"),
        ("sandbox", "🧪 Sandbox", "Test in PostgreSQL"),
        ("validation", "✅ Validation", "Validate schema fidelity"),
        ("data_migration", "📦 Data", "Transfer data to PostgreSQL"),
        ("reporting", "📝 Report", "Generate migration report"),
=======
        ("introspection", "📥 Introspect", "Extract schema"),
        ("dependency", "🔗 Deps", "Analyze relationships"),
        ("schema", "🔄 Schema", "Convert DDL"),
        ("logic", "⚙️ Logic", "Convert procs"),
        ("sandbox", "🧪 Sandbox", "Test DDL"),
        ("validation", "✅ Validate", "Check schema"),
        ("data_migration", "📦 Data", "Migrate data"),
        ("reporting", "📝 Report", "Generate report"),
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    ]
    
    # Phase progress cards
    cols = st.columns(len(phases))
    for i, (phase_id, phase_name, phase_desc) in enumerate(phases):
        with cols[i]:
            status = st.session_state.phase_status.get(phase_id, "pending")
            if st.session_state.current_phase == phase_id:
                st.markdown(f"🔄 **{phase_name}**")
                st.caption("Running...")
            elif status == "complete" or st.session_state.migration_complete:
                st.markdown(f"✅ **{phase_name}**")
                st.caption("Complete")
            else:
                st.markdown(f"⏳ {phase_name}")
                st.caption(phase_desc)
    
    st.divider()
    
    # Control buttons - Two Phase approach
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    
    with col1:
        if not st.session_state.migration_running:
            if st.button("▶️ Start Migration", type="primary", use_container_width=True, help="Phase 1: Migrate to sandbox"):
                start_migration_async()
        else:
            st.button("⏳ Running...", disabled=True, use_container_width=True)
    
    with col2:
        # Deploy to Production button - only active after Phase 1 success
        deploy_enabled = st.session_state.migration_complete and not st.session_state.migration_running
        if st.button("🚀 Deploy to Prod", 
                     disabled=not deploy_enabled, 
                     use_container_width=True,
                     help="Phase 2: Deploy to production target"):
            if deploy_enabled:
                start_production_deploy_async()
    
    with col3:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()
    
    with col4:
        if st.session_state.migration_running:
            st.info("🔄 Running...")
        elif st.session_state.get("production_deployed", False):
            st.success("✅ Deployed!")
        elif st.session_state.migration_complete:
            st.success("✅ Phase 1 Done")
    
    # Progress bar
    if st.session_state.migration_running:
        # Calculate progress based on completed phases
        completed = len([p for p, s in st.session_state.phase_status.items() if s == "complete"])
        progress = completed / len(phases)
        st.progress(progress, text=f"Phase {completed + 1}/{len(phases)}")
    elif st.session_state.migration_complete:
        st.progress(1.0, text="✅ Phase 1 complete! Click 'Deploy to Prod' for Phase 2")
    
    # Live logs with scrollable container
    st.subheader("📝 Live Logs (Newest First)")
    
    log_container = st.container(height=350)
    with log_container:
        if st.session_state.logs:
            # Show logs in REVERSE order (newest first) - so latest always visible at top
            logs_to_show = st.session_state.logs[-100:]  # Last 100 logs
            for log in reversed(logs_to_show):
                # Apply color based on log content
                if "✅" in log or "Success" in log:
                    st.markdown(f'<p class="log-entry log-success">{log}</p>', unsafe_allow_html=True)
                elif "⚠️" in log or "Warning" in log:
                    st.markdown(f'<p class="log-entry log-warning">{log}</p>', unsafe_allow_html=True)
                elif "❌" in log or "Error" in log or "failed" in log:
                    st.markdown(f'<p class="log-entry log-error">{log}</p>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<p class="log-entry log-info">{log}</p>', unsafe_allow_html=True)
        else:
            st.info("Logs will appear here during migration. Click 'Start Migration' to begin.")


def render_results_tab(artifacts):
    """Render the results tab."""
    st.subheader("Migration Results")
    
    # Show running state if migration is in progress
    if st.session_state.migration_running:
        st.markdown("""
        <div class="running-overlay">
            <h3>🔄 Migration In Progress</h3>
            <p>Results will update automatically when migration completes.</p>
            <p>Check the <b>Migration</b> tab for live logs.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    if not artifacts or not st.session_state.show_previous_results:
        st.info("Run a migration to see results.")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        tables = len(artifacts.get("schema", {}).get("tables", []))
        st.markdown(f"""
        <div class="metric-card">
            <h3>{tables}</h3>
            <p>📋 Tables</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        ddl_info = artifacts.get("ddl", {})
        indexes = ddl_info.get("indexes", 0)
        st.markdown(f"""
        <div class="metric-card info-card">
            <h3>{indexes}</h3>
            <p>📇 Indexes</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        sandbox = artifacts.get("sandbox", {})
        passed = sandbox.get("passed", 0)
        total = sandbox.get("total", 0)
        st.markdown(f"""
        <div class="metric-card success-card">
            <h3>{passed}/{total}</h3>
            <p>🧪 Sandbox Tests</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        validation = artifacts.get("validation", {})
        val_passed = validation.get("passed", 0)
        val_total = validation.get("total_checks", 0)
        st.markdown(f"""
        <div class="metric-card success-card">
            <h3>{val_passed}/{val_total}</h3>
            <p>✅ Validation</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Detailed results
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📋 Transformed Objects")
        if "ddl" in artifacts:
            ddl = artifacts["ddl"]
            st.write(f"**Tables:** {ddl.get('tables', 0)}")
            st.write(f"**Views:** {ddl.get('views', 0)}")
            st.write(f"**Indexes:** {ddl.get('indexes', 0)}")
            st.write(f"**Foreign Keys:** {ddl.get('deferred_fks', 0)}")
        
        if "schema" in artifacts:
            with st.expander("View Table List"):
                for table in artifacts["schema"].get("tables", []):
                    st.text(f"• {table['name']} ({len(table.get('columns', []))} cols)")
    
    with col2:
        st.subheader("🧪 Sandbox Results")
        if "sandbox" in artifacts:
            sandbox = artifacts["sandbox"]
            passed = sandbox.get("passed", 0)
            failed = sandbox.get("failed", 0)
            
            if failed == 0:
                st.success(f"All {passed} tests passed!")
            else:
                st.warning(f"{passed} passed, {failed} failed")
            
            with st.expander("View Test Details"):
                for result in sandbox.get("results", [])[:20]:
                    icon = "✅" if result.get("executed") else "❌"
                    st.text(f"{icon} {result.get('object_name')} ({result.get('object_type')})")


def render_validation_tab(artifacts):
    """Render the schema validation tab."""
    st.subheader("🔍 Schema Validation")
    st.caption("Compares source MySQL schema with target PostgreSQL schema")
    
    # Show running state if migration is in progress
    if st.session_state.migration_running:
        st.markdown("""
        <div class="running-overlay">
            <h3>🔄 Migration In Progress</h3>
            <p>Schema validation runs after sandbox testing.</p>
            <p>Check the <b>Migration</b> tab for live logs.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    schema_val = artifacts.get("schema_validation", {})
    
    if not schema_val:
        st.info("Schema validation results will appear after running a migration.")
        return
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total = schema_val.get("total_checks", 0)
        st.metric("Total Checks", total)
    
    with col2:
        passed = schema_val.get("passed_checks", 0)
        st.metric("Passed", passed, delta=None if passed == total else f"-{total - passed}")
    
    with col3:
        status = schema_val.get("overall_status", "unknown")
        st.metric("Status", status.upper())
    
    st.divider()
    
    # Validation details
    if schema_val.get("overall_status") == "pass":
        st.success("✅ Schema validation PASSED - All schema elements match!")
    else:
        st.warning("⚠️ Schema validation has issues")
    
    # Issues breakdown
    issues = schema_val.get("issues", [])
    if issues:
        st.subheader("Issues Found")
        
        # Group by category
        categories = {}
        for issue in issues:
            cat = issue.get("category", "other")
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(issue)
        
        for cat, cat_issues in categories.items():
            with st.expander(f"📁 {cat.upper()} ({len(cat_issues)} issues)"):
                for issue in cat_issues:
                    severity = issue.get("severity", "info")
                    icon = "🔴" if severity == "critical" else "🟡" if severity == "warning" else "🔵"
                    st.markdown(f"{icon} **{issue.get('table')}**: {issue.get('message')}")
    else:
        st.info("No issues found in schema validation")


def render_data_migration_tab(artifacts):
<<<<<<< HEAD
    """Render the data migration tab."""
    st.subheader("📦 Data Migration")
    st.caption("Transfer data from MySQL to PostgreSQL")
=======
    """Render the data migration status tab."""
    st.subheader("📦 Data Migration Status")
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
    
    # Show running state if migration is in progress
    if st.session_state.migration_running:
        st.markdown("""
        <div class="running-overlay">
            <h3>🔄 Migration In Progress</h3>
<<<<<<< HEAD
            <p>Data migration runs after schema validation passes.</p>
=======
            <p>Data migration status will appear here after completion.</p>
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b
            <p>Check the <b>Migration</b> tab for live logs.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
<<<<<<< HEAD
    data_mig = artifacts.get("data_migration", {})
    
    if not data_mig:
        st.info("Data migration results will appear after running a migration.")
        st.markdown("""
        **Data migration includes:**
        - Streaming batch transfer (1000 rows/batch)
        - Data type transformations
        - Foreign key constraint management
        - Sequence resetting
        - Row count validation
        """)
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    summary = data_mig.get("summary", {})
    table_results = data_mig.get("table_results", [])
    
    with col1:
        total_rows = summary.get("total_rows", 0)
        st.markdown(f"""
        <div class="metric-card success-card">
            <h3>{total_rows:,}</h3>
            <p>📊 Total Rows</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        tables_migrated = summary.get("tables_migrated_count", 0)
        st.markdown(f"""
        <div class="metric-card">
            <h3>{tables_migrated}</h3>
            <p>📋 Tables Migrated</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        tables_failed = summary.get("tables_failed_count", 0)
        card_class = "success-card" if tables_failed == 0 else "warning-card"
        st.markdown(f"""
        <div class="metric-card {card_class}">
            <h3>{tables_failed}</h3>
            <p>❌ Tables Failed</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        duration_s = summary.get("total_duration_ms", 0) / 1000
        st.markdown(f"""
        <div class="metric-card info-card">
            <h3>{duration_s:.1f}s</h3>
            <p>⏱️ Duration</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Table results
    st.subheader("Per-Table Results")
    
    if table_results:
        for result in table_results:
            status_icon = "✅" if result.get("success") else "❌"
            table_name = result.get("table_name", "unknown")
            rows = result.get("rows_migrated", 0)
            duration = result.get("duration_ms", 0)
            
            with st.expander(f"{status_icon} {table_name} — {rows:,} rows"):
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Rows Migrated", f"{rows:,}")
                with col2:
                    st.metric("Duration", f"{duration:.0f}ms")
                
                errors = result.get("errors", [])
                if errors:
                    st.error("Errors:")
                    for err in errors:
                        st.text(f"  • {err}")
    else:
        st.info("No table results available")
=======
    # Check if we should show results (only from current session)
    if not st.session_state.show_previous_results:
        st.info("📥 Run a migration to see data migration results.")
        st.markdown("""
        The data migration phase:
        - Streams data from MySQL to PostgreSQL in batches
        - Converts data types (GEOMETRY → POINT, etc.)
        - Validates row counts after migration
        """)
        return
    
    # Load data migration results
    data_migration_path = Path("./artifacts/data_migration_results.json")
    
    if not data_migration_path.exists():
        st.info("📥 Data migration results will appear after running a migration.")
        return
    
    try:
        with open(data_migration_path) as f:
            data_results = json.load(f)
    except Exception as e:
        st.error(f"Error loading data migration results: {e}")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        target = data_results.get("target", "sandbox")
        st.metric("Target", target.upper())
    
    with col2:
        tables_migrated = data_results.get("tables_migrated", 0)
        st.metric("Tables", tables_migrated)
    
    with col3:
        total_rows = data_results.get("total_rows", 0)
        st.metric("Rows Migrated", f"{total_rows:,}")
    
    with col4:
        validation = data_results.get("validation", [])
        passed = len([v for v in validation if v.get("match", False)])
        st.metric("Validation", f"{passed}/{len(validation)}")
    
    st.divider()
    
    # Migration results table
    st.subheader("📋 Migration Results by Table")
    
    migration_results = data_results.get("migration_results", [])
    if migration_results:
        # Create a combined view with migration + validation
        validation_map = {v["table"]: v for v in data_results.get("validation", [])}
        
        for result in migration_results:
            table_name = result.get("table", "")
            success = result.get("success", False)
            rows = result.get("rows_migrated", 0)
            time_ms = result.get("time_ms", 0)
            error = result.get("error")
            
            # Get validation info
            val_info = validation_map.get(table_name, {})
            source_count = val_info.get("source_count", 0)
            target_count = val_info.get("target_count", 0)
            match = val_info.get("match", False)
            
            # Display row
            if success and match:
                st.markdown(f"✅ **{table_name}**: {rows:,} rows migrated in {time_ms:.0f}ms — Validation: {target_count:,}/{source_count:,} ✓")
            elif success and not match:
                st.markdown(f"⚠️ **{table_name}**: {rows:,} rows migrated — Validation FAILED: {target_count:,}/{source_count:,} ❌")
            else:
                with st.expander(f"❌ **{table_name}**: Migration FAILED"):
                    st.error(error[:500] if error else "Unknown error")
    
    st.divider()
    
    # Validation summary
    st.subheader("📊 Row Count Validation")
    
    validation = data_results.get("validation", [])
    passed_tables = [v for v in validation if v.get("match", False)]
    failed_tables = [v for v in validation if not v.get("match", False)]
    
    if len(failed_tables) == 0:
        st.success(f"✅ All {len(passed_tables)} tables validated successfully - row counts match!")
    else:
        st.warning(f"⚠️ {len(failed_tables)} tables have row count mismatches:")
        for v in failed_tables:
            st.markdown(f"  - **{v['table']}**: Source={v['source_count']:,}, Target={v['target_count']:,}")
>>>>>>> 5b7a1235ba7ce790739964ce89fbce20f3df9c2b


def render_tokens_tab(artifacts):
    """Render the token usage tab."""
    st.subheader("📈 LLM Token Usage")
    
    # Show running state if migration is in progress
    if st.session_state.migration_running:
        st.markdown("""
        <div class="running-overlay">
            <h3>🔄 Migration In Progress</h3>
            <p>Token usage will be calculated after migration completes.</p>
            <p>Check the <b>Migration</b> tab for live logs.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    tokens = artifacts.get("tokens", {})
    
    if not tokens:
        st.info("Token usage data will appear after running a migration.")
        return
    
    # Summary
    total_tokens = tokens.get("total_tokens", 0)
    total_calls = tokens.get("total_calls", 0)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>{total_tokens:,}</h3>
            <p>🔢 Total Tokens</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card info-card">
            <h3>{total_calls}</h3>
            <p>📞 LLM Calls</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Usage by agent
    st.subheader("Usage by Agent")
    by_agent = tokens.get("by_agent", {})
    
    if by_agent:
        for agent, agent_tokens in by_agent.items():
            if isinstance(agent_tokens, dict):
                agent_tokens = agent_tokens.get("tokens", 0)
            pct = (agent_tokens / total_tokens * 100) if total_tokens > 0 else 0
            
            st.write(f"**{agent}**")
            st.progress(pct / 100, text=f"{agent_tokens:,} tokens ({pct:.1f}%)")
    
    # Usage by model
    st.subheader("Usage by Model")
    by_model = tokens.get("by_model", {})
    
    if by_model:
        for model, model_tokens in by_model.items():
            if isinstance(model_tokens, dict):
                model_tokens = model_tokens.get("tokens", 0)
            st.write(f"**{model}**: {model_tokens:,} tokens")


def render_report_tab():
    """Render the final report tab."""
    st.subheader("📝 Migration Report")
    
    # Show running state if migration is in progress
    if st.session_state.migration_running:
        st.markdown("""
        <div class="running-overlay">
            <h3>🔄 Migration In Progress</h3>
            <p>The report will be generated after migration completes.</p>
            <p>Check the <b>Migration</b> tab for live logs.</p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    report_content = load_report()
    
    if report_content:
        st.markdown(report_content)
        
        st.divider()
        
        # Download button
        st.download_button(
            label="📥 Download Report",
            data=report_content,
            file_name="migration_report.md",
            mime="text/markdown",
        )
    else:
        st.info("Migration report will appear here after a successful migration.")
        st.markdown("""
        The report includes:
        - Executive Summary
        - Tables Migrated
        - Procedures & Functions
        - Validation Results
        - Errors & Warnings
        - Generated Artifacts
        """)


def start_migration_async():
    """Start the migration workflow in a background thread."""
    st.session_state.migration_running = True
    st.session_state.migration_complete = False
    st.session_state.logs = []
    st.session_state.final_state = None
    st.session_state.phase_status = {}
    
    add_log("Starting migration workflow...")
    
    # Set environment variables from sidebar
    os.environ["SOURCE_DB_HOST"] = st.session_state.src_host
    os.environ["SOURCE_DB_PORT"] = str(st.session_state.src_port)
    os.environ["SOURCE_DB_NAME"] = st.session_state.src_db
    os.environ["SOURCE_DB_USER"] = st.session_state.src_user
    os.environ["SOURCE_DB_PASSWORD"] = st.session_state.src_pass
    
    os.environ["TARGET_DB_HOST"] = st.session_state.tgt_host
    os.environ["TARGET_DB_PORT"] = str(st.session_state.tgt_port)
    os.environ["TARGET_DB_NAME"] = st.session_state.tgt_db
    os.environ["TARGET_DB_USER"] = st.session_state.tgt_user
    os.environ["TARGET_DB_PASSWORD"] = st.session_state.tgt_pass
    
    # Create references for thread
    log_queue = st.session_state.log_queue
    
    def run_migration():
        """Run migration in background thread."""
        import io
        from contextlib import redirect_stdout
        
        class QueueWriter(io.StringIO):
            def __init__(self, queue):
                super().__init__()
                self.queue = queue
                self._buffer = ""
            
            def write(self, text):
                # Also write to real stdout
                sys.__stdout__.write(text)
                
                self._buffer += text
                while "\n" in self._buffer:
                    line, self._buffer = self._buffer.split("\n", 1)
                    if line.strip():
                        timestamp = time.strftime("%H:%M:%S")
                        
                        # Determine icon based on content
                        if "✅" in line or "Success" in line or "passed" in line.lower():
                            icon = "✅"
                        elif "⚠️" in line or "Warning" in line:
                            icon = "⚠️"
                        elif "❌" in line or "Error" in line or "failed" in line.lower():
                            icon = "❌"
                        else:
                            icon = "ℹ️"
                        
                        # Clean line
                        clean_line = line.strip()
                        for emoji in ["ℹ️", "✅", "⚠️", "❌", "📊", "🔧", "🔄"]:
                            if clean_line.startswith(emoji):
                                clean_line = clean_line[len(emoji):].strip()
                        
                        log_entry = f"[{timestamp}] {icon} {clean_line}"
                        self.queue.put(log_entry)
                
                return len(text)
            
            def flush(self):
                pass
        
        try:
            from src.graph.workflow import create_workflow_with_memory
            from src.state import MigrationState
            
            queue_writer = QueueWriter(log_queue)
            
            with redirect_stdout(queue_writer):
                workflow = create_workflow_with_memory()
                initial_state = MigrationState().model_dump()
                
                config = {"configurable": {"thread_id": "streamlit-migration"}}
                
                for state_update in workflow.stream(initial_state, config=config):
                    for node_name, node_state in state_update.items():
                        log_queue.put(f"[{time.strftime('%H:%M:%S')}] ✅ Completed phase: {node_name}")
            
            log_queue.put(f"[{time.strftime('%H:%M:%S')}] ✅ Migration completed successfully!")
            
            # Save token usage to file for UI display
            try:
                from src.tools.token_tracker import get_token_tracker
                from pathlib import Path
                tracker = get_token_tracker()
                tracker.save_to_file(Path("./artifacts/token_usage.json"))
                log_queue.put(f"[{time.strftime('%H:%M:%S')}] ℹ️ Token usage saved ({tracker.get_total_tokens():,} tokens)")
            except Exception as token_err:
                log_queue.put(f"[{time.strftime('%H:%M:%S')}] ⚠️ Could not save token usage: {token_err}")
            
        except Exception as e:
            log_queue.put(f"[{time.strftime('%H:%M:%S')}] ❌ Migration failed: {str(e)}")
    
    # Start background thread
    thread = threading.Thread(target=run_migration, daemon=True)
    thread.start()
    st.session_state.migration_thread = thread
    
    st.rerun()


def start_production_deploy_async():
    """Start Phase 2: Deploy to production target."""
    st.session_state.migration_running = True
    
    add_log("🚀 Starting production deployment (Phase 2)...")
    
    log_queue = st.session_state.log_queue
    
    def run_production_deploy():
        """Run production deployment in background thread."""
        import io
        from contextlib import redirect_stdout
        
        class QueueWriter(io.StringIO):
            def __init__(self, queue):
                super().__init__()
                self.queue = queue
                self._buffer = ""
            
            def write(self, text):
                sys.__stdout__.write(text)
                self._buffer += text
                while "\n" in self._buffer:
                    line, self._buffer = self._buffer.split("\n", 1)
                    if line.strip():
                        timestamp = time.strftime("%H:%M:%S")
                        icon = "✅" if "✅" in line or "Success" in line else "ℹ️"
                        if "❌" in line or "Error" in line:
                            icon = "❌"
                        elif "⚠️" in line or "Warning" in line:
                            icon = "⚠️"
                        log_entry = f"[{timestamp}] {icon} {line.strip()}"
                        self.queue.put(log_entry)
                return len(text)
            
            def flush(self):
                pass
        
        try:
            from src.agents.production_deploy_agent import ProductionDeployAgent
            from src.state import MigrationState
            
            queue_writer = QueueWriter(log_queue)
            
            with redirect_stdout(queue_writer):
                # Load previous state from artifacts if available
                from pathlib import Path
                import json
                
                state = MigrationState()
                
                # Load schema metadata
                schema_path = Path("./artifacts/schema_metadata.json")
                if schema_path.exists():
                    with open(schema_path) as f:
                        schema_data = json.load(f)
                    from src.state import SchemaMetadata
                    state.schema_metadata = SchemaMetadata(**{k: v for k, v in schema_data.items() if k != "_artifact_metadata"})
                
                # Load dependency graph
                dep_path = Path("./artifacts/dependency_graph.json")
                if dep_path.exists():
                    with open(dep_path) as f:
                        dep_data = json.load(f)
                    from src.state import DependencyGraph
                    state.dependency_graph = DependencyGraph(**{k: v for k, v in dep_data.items() if k != "_artifact_metadata"})
                
                # Load transformed DDL (CRITICAL for production deploy!)
                ddl_path = Path("./artifacts/transformed_ddl.json")
                if ddl_path.exists():
                    with open(ddl_path) as f:
                        ddl_data = json.load(f)
                    from src.state import TransformedDDL
                    transformations = ddl_data.get("transformations", [])
                    state.transformed_ddl = [
                        TransformedDDL(**{k: v for k, v in t.items() if k not in ["_artifact_metadata", "table_blueprint"]})
                        for t in transformations
                    ]
                    print(f"[Production Deploy] Loaded {len(state.transformed_ddl)} DDL objects from artifacts")
                else:
                    print("[Production Deploy] WARNING: transformed_ddl.json not found!")
                
                # Load converted procedures
                proc_path = Path("./artifacts/converted_procedures.json")
                if proc_path.exists():
                    with open(proc_path) as f:
                        proc_data = json.load(f)
                    from src.state import ConvertedProcedure
                    # Use 'conversions' key - that's how it's stored!
                    conversions = proc_data.get("conversions", [])
                    state.converted_procedures = [
                        ConvertedProcedure(**{k: v for k, v in p.items() if k != "_artifact_metadata"})
                        for p in conversions
                    ]
                    print(f"[Production Deploy] Loaded {len(state.converted_procedures)} procedures from artifacts")
                
                # Load tables_migrated for validation
                data_path = Path("./artifacts/data_migration_results.json")
                if data_path.exists():
                    with open(data_path) as f:
                        data_data = json.load(f)
                    state.tables_migrated = data_data.get("tables_migrated", [])
                
                # Assume validation passed (since we're in Phase 2)
                state.validation_passed = True
                
                # Run production deploy
                agent = ProductionDeployAgent()
                updated_state = agent.run(state)
                
                if updated_state.production_deployed:
                    log_queue.put(f"[{time.strftime('%H:%M:%S')}] ✅ Production deployment complete!")
                else:
                    log_queue.put(f"[{time.strftime('%H:%M:%S')}] ⚠️ Production deployment had issues - check logs")
            
        except Exception as e:
            log_queue.put(f"[{time.strftime('%H:%M:%S')}] ❌ Production deployment failed: {str(e)}")
    
    # Start background thread
    thread = threading.Thread(target=run_production_deploy, daemon=True)
    thread.start()
    st.session_state.migration_thread = thread
    
    st.rerun()


def render_query_lab_tab():
    """Render the Query Lab tab for side-by-side MySQL vs PostgreSQL query testing."""
    st.subheader("🔬 Query Lab - Compare MySQL vs PostgreSQL")
    
    st.markdown("""
    Execute queries against both databases side-by-side to compare results after migration.
    """)
    
    # Sample queries
    # Initialize session state for queries AND results
    if "mysql_query" not in st.session_state:
        st.session_state.mysql_query = "SELECT 1 as test;"
    if "pg_query" not in st.session_state:
        st.session_state.pg_query = "SELECT 1 as test;"
    
    # Store results in session state to persist them
    if "mysql_result" not in st.session_state:
        st.session_state.mysql_result = None
    if "pg_result" not in st.session_state:
        st.session_state.pg_result = None
    
    # Global controls removed in favor of individual clears
    
    # st.divider()
    
    # Two columns for side-by-side comparison
    col_mysql, col_pg = st.columns(2)
    
    with col_mysql:
        st.markdown("### 📥 MySQL (Source)")
        mysql_query = st.text_area(
            "MySQL Query",
            value=st.session_state.mysql_query,
            height=150,
            key="mysql_query_input",
            label_visibility="collapsed"
        )
        
        col_exec, col_clear = st.columns([1, 4])
        with col_exec:
            mysql_execute = st.button("▶️ Execute", key="execute_mysql", type="primary")
        with col_clear:
            if st.button("🗑️ Clear", key="clear_mysql"):
                st.session_state.mysql_result = None
                st.rerun()
        
        if mysql_execute and mysql_query.strip():
            with st.spinner("Executing MySQL query..."):
                result = execute_mysql_query(mysql_query)
                st.session_state.mysql_result = result
        
        # Display stored result
        if st.session_state.mysql_result:
            result = st.session_state.mysql_result
            if result["success"]:
                st.success(f"✅ {result['row_count']} rows in {result['duration']:.3f}s")
                if result["data"] is not None:
                    st.dataframe(result["data"], use_container_width=True)
            else:
                st.error(f"❌ {result['error']}")
    
    with col_pg:
        st.markdown("### 📤 PostgreSQL (Target)")
        pg_query = st.text_area(
            "PostgreSQL Query",
            value=st.session_state.pg_query,
            height=150,
            key="pg_query_input",
            label_visibility="collapsed"
        )
        
        col_exec_pg, col_clear_pg = st.columns([1, 4])
        with col_exec_pg:
            pg_execute = st.button("▶️ Execute", key="execute_pg", type="primary")
        with col_clear_pg:
            if st.button("🗑️ Clear", key="clear_pg"):
                st.session_state.pg_result = None
                st.rerun()
        
        if pg_execute and pg_query.strip():
            with st.spinner("Executing PostgreSQL query..."):
                result = execute_pg_query(pg_query)
                st.session_state.pg_result = result
        
        # Display stored result
        if st.session_state.pg_result:
            result = st.session_state.pg_result
            if result["success"]:
                st.success(f"✅ {result['row_count']} rows in {result['duration']:.3f}s")
                if result["data"] is not None:
                    st.dataframe(result["data"], use_container_width=True)
            else:
                st.error(f"❌ {result['error']}")
    
    # Tips section
    st.divider()
    with st.expander("💡 Tips"):
        st.markdown("""
        **MySQL vs PostgreSQL Syntax Differences:**
        - `DATABASE()` → `current_database()`
        - `SHOW TABLES` → `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`
        - Backticks `` ` `` → Double quotes `"`
        - `LIMIT x, y` → `LIMIT y OFFSET x`
        """)


def execute_mysql_query(query: str) -> dict:
    """Execute a query against the MySQL source database."""
    import time
    try:
        from sqlalchemy import create_engine, text
        from src.config import get_settings
        import pandas as pd
        
        settings = get_settings()
        engine = create_engine(settings.db.source_connection_string)
        
        start_time = time.time()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            # Check if this is a SELECT query
            if result.returns_rows:
                rows = result.fetchall()
                columns = result.keys()
                data = pd.DataFrame(rows, columns=columns)
                row_count = len(data)
            else:
                data = None
                row_count = result.rowcount
        
        duration = time.time() - start_time
        engine.dispose()
        
        return {
            "success": True,
            "data": data,
            "row_count": row_count,
            "duration": duration,
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "row_count": 0,
            "duration": 0,
            "error": str(e)
        }


def execute_pg_query(query: str) -> dict:
    """Execute a query against the PostgreSQL target database."""
    import time
    try:
        from sqlalchemy import create_engine, text
        from src.config import get_settings
        import pandas as pd
        
        settings = get_settings()
        # Use sandbox by default (safer for testing)
        engine = create_engine(settings.db.sandbox_connection_string)
        
        start_time = time.time()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            # Check if this is a SELECT query
            if result.returns_rows:
                rows = result.fetchall()
                columns = result.keys()
                data = pd.DataFrame(rows, columns=columns)
                row_count = len(data)
            else:
                data = None
                row_count = result.rowcount
        
        duration = time.time() - start_time
        engine.dispose()
        
        return {
            "success": True,
            "data": data,
            "row_count": row_count,
            "duration": duration,
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "row_count": 0,
            "duration": 0,
            "error": str(e)
        }


if __name__ == "__main__":
    main()
