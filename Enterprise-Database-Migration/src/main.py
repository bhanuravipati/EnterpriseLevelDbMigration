"""
Main CLI Entry Point - Interactive and command-line interface for migration.
"""

import os
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from dotenv import load_dotenv
from src.graph.workflow import run_migration
from src.state import MigrationState
        
from src.tools.token_tracker import get_token_tracker, print_model_reference, reset_token_tracker

# Load environment variables
load_dotenv()

app = typer.Typer(
    name="db-migrate",
    help="AI-Assisted Database Migration System",
    add_completion=False,
)
console = Console()


def print_banner():
    """Print application banner."""
    banner = """
╔═══════════════════════════════════════════════════════════╗
║       🗄️  AI Database Migration Assistant v1.0            ║
║       MySQL → PostgreSQL with Multi-Agent AI              ║
╚═══════════════════════════════════════════════════════════╝
    """
    console.print(banner, style="bold cyan")


@app.command()
def migrate(
    interactive: bool = typer.Option(
        False, "--interactive", "-i", help="Run in interactive mode with prompts"
    ),
    source_host: str = typer.Option(
        None, "--source-host", help="Source MySQL host"
    ),
    source_port: int = typer.Option(
        3306, "--source-port", help="Source MySQL port"
    ),
    source_db: str = typer.Option(
        None, "--source-db", help="Source database name"
    ),
    source_user: str = typer.Option(
        None, "--source-user", help="Source database user"
    ),
    source_pass: str = typer.Option(
        None, "--source-pass", help="Source database password"
    ),
    target_host: str = typer.Option(
        None, "--target-host", help="Target PostgreSQL host"
    ),
    target_port: int = typer.Option(
        5432, "--target-port", help="Target PostgreSQL port"
    ),
    target_db: str = typer.Option(
        None, "--target-db", help="Target database name"
    ),
    target_user: str = typer.Option(
        None, "--target-user", help="Target database user"
    ),
    target_pass: str = typer.Option(
        None, "--target-pass", help="Target database password"
    ),
):
    """
    Run database migration from MySQL to PostgreSQL.
    
    Use --interactive for guided setup or provide connection parameters directly.
    """
    print_banner()
    
    if interactive:
        run_interactive()
    else:
        # Use provided arguments or fall back to environment
        config = gather_config_from_args(
            source_host, source_port, source_db, source_user, source_pass,
            target_host, target_port, target_db, target_user, target_pass
        )
        run_migration_workflow(config)


def run_interactive():
    """Run interactive mode with prompts."""
    try:
        import questionary
    except ImportError:
        console.print("[red]questionary not installed. Run: pip install questionary[/red]")
        raise typer.Exit(1)
    
    console.print("\n[bold]Let's configure your migration:[/bold]\n")
    
    # Source database configuration
    console.print("[cyan]Source Database (MySQL)[/cyan]")
    source_host = questionary.text("Host:", default="localhost").ask()
    source_port = questionary.text("Port:", default="3306").ask()
    source_db = questionary.text("Database name:").ask()
    source_user = questionary.text("Username:", default="root").ask()
    source_pass = questionary.password("Password:").ask()
    
    console.print("\n[cyan]Target Database (PostgreSQL)[/cyan]")
    target_host = questionary.text("Host:", default="localhost").ask()
    target_port = questionary.text("Port:", default="5432").ask()
    target_db = questionary.text("Database name:").ask()
    target_user = questionary.text("Username:", default="postgres").ask()
    target_pass = questionary.password("Password:").ask()
    
    # Confirm
    console.print("\n[bold]Configuration Summary:[/bold]")
    table = Table(show_header=True)
    table.add_column("Setting", style="cyan")
    table.add_column("Source (MySQL)", style="green")
    table.add_column("Target (PostgreSQL)", style="blue")
    
    table.add_row("Host", source_host, target_host)
    table.add_row("Port", source_port, target_port)
    table.add_row("Database", source_db, target_db)
    table.add_row("User", source_user, target_user)
    
    console.print(table)
    
    if not questionary.confirm("\nProceed with migration?").ask():
        console.print("[yellow]Migration cancelled.[/yellow]")
        raise typer.Exit(0)
    
    # Build config and run
    config = {
        "source": {
            "host": source_host,
            "port": int(source_port),
            "database": source_db,
            "user": source_user,
            "password": source_pass,
        },
        "target": {
            "host": target_host,
            "port": int(target_port),
            "database": target_db,
            "user": target_user,
            "password": target_pass,
        }
    }
    
    run_migration_workflow(config)


def gather_config_from_args(
    source_host, source_port, source_db, source_user, source_pass,
    target_host, target_port, target_db, target_user, target_pass
) -> dict:
    """Gather configuration from args and environment."""
    return {
        "source": {
            "host": source_host or os.getenv("SOURCE_DB_HOST", "localhost"),
            "port": source_port or int(os.getenv("SOURCE_DB_PORT", "3306")),
            "database": source_db or os.getenv("SOURCE_DB_NAME", "sakila"),
            "user": source_user or os.getenv("SOURCE_DB_USER", "root"),
            "password": source_pass or os.getenv("SOURCE_DB_PASSWORD", "rootpass"),
        },
        "target": {
            "host": target_host or os.getenv("TARGET_DB_HOST", "localhost"),
            "port": target_port or int(os.getenv("TARGET_DB_PORT", "5432")),
            "database": target_db or os.getenv("TARGET_DB_NAME", "sakila_pg"),
            "user": target_user or os.getenv("TARGET_DB_USER", "postgres"),
            "password": target_pass or os.getenv("TARGET_DB_PASSWORD", "postgrespass"),
        }
    }


def run_migration_workflow(config: dict):
    """Run the migration workflow with configuration."""
    console.print("\n[bold green]🚀 Starting Migration...[/bold green]\n")
    
    # Reset token tracker for new migration
    reset_token_tracker()
    
    # Print model reference at the start
    print_model_reference()
    
    # Set environment variables for the config
    os.environ["SOURCE_DB_HOST"] = config["source"]["host"]
    os.environ["SOURCE_DB_PORT"] = str(config["source"]["port"])
    os.environ["SOURCE_DB_NAME"] = config["source"]["database"]
    os.environ["SOURCE_DB_USER"] = config["source"]["user"]
    os.environ["SOURCE_DB_PASSWORD"] = config["source"]["password"]
    
    os.environ["TARGET_DB_HOST"] = config["target"]["host"]
    os.environ["TARGET_DB_PORT"] = str(config["target"]["port"])
    os.environ["TARGET_DB_NAME"] = config["target"]["database"]
    os.environ["TARGET_DB_USER"] = config["target"]["user"]
    os.environ["TARGET_DB_PASSWORD"] = config["target"]["password"]
    
    try:
       
        # Run the workflow
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Running migration workflow...", total=None)
            
            final_state = run_migration()
            
            progress.update(task, completed=True)
        
        # Show results
        if final_state:
            show_results(final_state)
        
    except Exception as e:
        console.print(f"\n[red]Migration failed: {str(e)}[/red]")
        raise typer.Exit(1)


def show_results(state: dict):
    """Display migration results."""
    console.print("\n" + "=" * 60)
    
    status = state.get("overall_status", "unknown")
    if status == "success":
        console.print(Panel("[bold green]✅ Migration Completed Successfully![/bold green]"))
    else:
        console.print(Panel("[bold red]❌ Migration Completed with Issues[/bold red]"))
    
    # Show summary table
    table = Table(title="Migration Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")
    
    table.add_row("Status", status.upper())
    table.add_row("Tables", str(len(state.get("transformed_ddl", []))))
    table.add_row("Procedures", str(len(state.get("converted_procedures", []))))
    table.add_row("Validation", "✅ Passed" if state.get("validation_passed") else "❌ Failed")
    table.add_row("Errors", str(len(state.get("errors", []))))
    
    console.print(table)
    
    # Show artifact paths
    artifact_paths = state.get("artifact_paths", {})
    if artifact_paths:
        console.print("\n[bold]Generated Artifacts:[/bold]")
        for name, path in artifact_paths.items():
            console.print(f"  • {name}: [dim]{path}[/dim]")
    
    # Show token usage summary
    tracker = get_token_tracker()
    tracker.print_summary()
    
    # Save token usage to file
    from pathlib import Path
    tracker.save_to_file(Path("./artifacts/token_usage.json"))


@app.command()
def check():
    """Check database connections and dependencies."""
    print_banner()
    console.print("\n[bold]Checking dependencies...[/bold]\n")
    
    checks = [
        ("LangGraph", "langgraph"),
        ("LangChain", "langchain"),
        ("LangChain Groq", "langchain_groq"),
        ("SQLAlchemy", "sqlalchemy"),
        ("SQLGlot", "sqlglot"),
        ("PyMySQL", "pymysql"),
        ("Psycopg2", "psycopg2"),
        ("Streamlit", "streamlit"),
    ]
    
    for name, module in checks:
        try:
            __import__(module)
            console.print(f"  [green]✓[/green] {name}")
        except ImportError:
            console.print(f"  [red]✗[/red] {name} - not installed")
    
    console.print("\n[bold]Checking environment...[/bold]\n")
    
    groq_key = os.getenv("GROQ_API_KEY", "")
    if groq_key:
        console.print(f"  [green]✓[/green] GROQ_API_KEY is set")
    else:
        console.print(f"  [yellow]![/yellow] GROQ_API_KEY not set")


@app.command()
def version():
    """Show version information."""
    console.print("AI Database Migration System v1.0.0")


if __name__ == "__main__":
    app()
