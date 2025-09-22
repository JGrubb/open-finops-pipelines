# Open FinOps Pipelines

A lightweight, vendor-agnostic tool for ingesting cloud billing data into analytical databases. Built with simplicity, testability, and minimal dependencies in mind.

## Vision

Transform cloud billing data from multiple vendors (AWS, Azure) into queryable datasets for FinOps analysis, using local databases (DuckDB) or cloud warehouses (BigQuery) as analytical backends.

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Vendors   │    │  Pipeline    │    │  Backends   │
│             │────│              │────│             │
│ • AWS CUR   │    │ • Discovery  │    │ • DuckDB    │
│ • Azure     │    │ • State Mgmt │    │ • BigQuery  │
└─────────────┘    │ • Processing │    └─────────────┘
                   └──────────────┘
```

**Core Principles:**
- **Vendor-agnostic**: Common interface for AWS, Azure, and future cloud providers
- **Backend-flexible**: Support local (DuckDB) and cloud (BigQuery) analytical databases
- **State-aware**: Track processing to avoid reprocessing and enable resume
- **Test-driven**: Comprehensive test coverage with TDD development approach
- **Minimal dependencies**: Use Python standard library when possible

## Quick Start

```bash
# Install and configure
uv sync
cp config.example.toml config.toml  # Edit with your settings

# Discover available billing data
finops aws list-manifests --bucket my-cur-bucket --export-name my-export

# Check pipeline state
finops aws show-state

# Import billing data (coming in Phase 2B)
finops aws import-billing --start-date 2024-01 --end-date 2024-03
```

## Project Structure

```
src/finops/
├── vendors/           # Cloud provider integrations
│   └── aws/          # AWS CUR discovery and parsing
├── backends/         # Database integrations (future)
├── state/            # Pipeline state management
├── config/           # Configuration handling
└── cli/              # Command-line interface
```

## Development Philosophy

### Test-Driven Development
Every feature follows the TDD cycle:
1. Write failing tests first
2. Implement minimal code to pass
3. Refactor while keeping tests green

### Small, Focused Changes
- Each commit addresses a single concern
- Features developed incrementally
- Clear separation between vendors and backends

### Idiomatic Python
- Follow Python conventions and best practices
- Use type hints and Pydantic for validation
- Leverage standard library before adding dependencies

### Minimal Dependencies
Current production dependencies:
- `boto3` - AWS SDK (only dependency)
- `pydantic` - Configuration validation
- Standard library for everything else (SQLite, JSON, etc.)

## Configuration

Flexible configuration with clear precedence:

```toml
# config.toml
[aws]
bucket = "my-billing-bucket"
export_name = "my-cur-export"
region = "us-east-1"

[state]
database_path = "./data/pipeline_state.db"
```

**Precedence**: CLI args > Environment variables > config.toml > defaults

## Current Status

### ✅ Phase 1: Foundation (Complete)
- Modular CLI with AWS subcommands
- Configuration system with precedence handling
- Comprehensive test suite (44 tests passing)

### ✅ Phase 2A: AWS Integration & State Management (Complete)
- AWS CUR manifest discovery (v1 and v2 support)
- SQLite-based state tracking
- Smart duplicate detection and skipping

### 🔄 Phase 2B: Data Pipeline (Next)
- DuckDB integration for local analytics
- Actual data import from CUR files
- State transitions and error handling

### 🔮 Future Phases
- Azure billing integration
- BigQuery backend support
- Advanced features (incremental updates, monitoring)

## Contributing

This project follows strict TDD practices. Before implementing any feature:

1. Read the planning documents in `planning/`
2. Write comprehensive tests first
3. Implement the minimal code to pass tests
4. Ensure all existing tests continue to pass

Run tests with `uv run test` before submitting changes.