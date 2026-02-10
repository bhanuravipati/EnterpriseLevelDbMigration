# AI-Assisted Database Migration System

An autonomous multi-agent system for migrating MySQL databases to PostgreSQL using LangGraph and AI.

## Features

- 🤖 **Multi-Agent Architecture**: 7 specialized AI agents for different migration tasks
- 🔄 **Feedback Loops**: Automatic retry and self-correction on failures
- 🧪 **Sandbox Testing**: Test DDL in isolated environment before production
- 📊 **Rich UI**: Streamlit dashboard for monitoring and control
- 📝 **Comprehensive Reports**: Detailed migration reports in Markdown

## Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Linux/Mac)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` to `.env` and set your credentials:

```bash
cp .env.example .env
```

Edit `.env`:
- Set `GROQ_API_KEY` with your Groq API key
- Configure database connections

### 3. Start Databases (Docker)

```bash
docker-compose up -d
```

Wait for databases to be healthy:
```bash
docker-compose ps
```

### 4. Run Migration

**Interactive CLI:**
```bash
python -m src.main migrate --interactive
```

**With arguments:**
```bash
python -m src.main migrate \
  --source-host localhost \
  --source-port 3306 \
  --source-db sakila \
  --source-user root \
  --source-pass rootpass \
  --target-host localhost \
  --target-port 5432 \
  --target-db sakila_pg \
  --target-user postgres \
  --target-pass postgrespass
```

**Streamlit Dashboard:**
```bash
streamlit run src/app.py
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    LangGraph Workflow                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Introspection → Dependency → Schema → Logic → Sandbox     │
│                                   ↑              ↓          │
│                                   └──── Retry ←──┘          │
│                                              ↓              │
│                              Validation → Reporting         │
│                                   ↑              ↓          │
│                                   └──── Retry ───┘          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Generated Artifacts

| Artifact | Format | Description |
|----------|--------|-------------|
| `schema_metadata.json` | JSON | Source database schema |
| `dependency_graph.json` | JSON | Object dependencies |
| `migration_plan.yaml` | YAML | Phased migration plan |
| `ddl/*.sql` | SQL | PostgreSQL DDL statements |
| `procedures/*.sql` | SQL | Converted PL/pgSQL code |
| `sandbox_results.json` | JSON | Test execution results |
| `validation_report.json` | JSON | Data integrity checks |
| `migration_report.md` | Markdown | Final report |

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Project Structure

```
├── src/
│   ├── agents/          # Migration agents
│   ├── tools/           # Database and utility tools
│   ├── graph/           # LangGraph workflow
│   ├── main.py          # CLI entry point
│   └── app.py           # Streamlit dashboard
├── tests/               # Unit and integration tests
├── artifacts/           # Generated artifacts
├── reports/             # Migration reports
├── docker-compose.yml   # Database containers
└── requirements.txt     # Python dependencies
```

## License

MIT
