# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a CLI tool for AWS and Azure (next phase) billing data pipelines that ingests Cost and Usage Reports (CUR) into analytical data bases. The project uses a hierarchical CLI structure with argparse and follows a vendor-agnostic, state-aware pipeline architecture.

## Development Commands

```bash
# Install in development mode
uv pip install -e .

# Run the CLI
finops --help
finops config
finops aws discover-manifests

# Check installed CLI
which finops
```

## Architecture

### CLI Command Pattern
The codebase uses `set_defaults(func=function)` pattern for command dispatch instead of large elif chains:

```python
# In commands/aws.py
def setup_aws_parser(subparsers):
    cmd_parser = aws_subparsers.add_parser("command-name", help="...")
    cmd_parser.set_defaults(func=command_function)

def command_function(config_path, args):
    # All commands receive config_path and args
```

### Command Structure
```
finops config              # Display configuration
finops aws discover-manifests     # Scan S3 for CUR manifests
finops aws extract-manifests      # Download manifest files
finops aws show-state             # Show pipeline state
finops aws extract-billing        # Download billing CSVs
finops aws load-billing-local     # Load to DuckDB
finops aws export-parquet         # Export to Parquet
finops aws load-billing-remote    # Load to BigQuery
```

### Key Design Principles
- **Vendor-agnostic**: Support AWS, Azure via common interface
- **Backend-flexible**: Local DuckDB or remote BigQuery
- **State-aware**: Track processing state in SQLite
- **Schema evolution**: ALTER TABLE for new columns, normalize names
- **Column handling**: Force resourcetags_* to VARCHAR, sort manifests DESC by billing_month

## Pipeline State Flow
```
discovered → downloading → staged → loading → loaded
     |            |           |         |
   failed      failed      failed    failed
```

## Implementation Status
- ✅ CLI framework complete
- ✅ All command stubs defined
- ❌ Business logic (all functions return "[STUB] Implementation pending")
- ❌ Testing framework
- ❌ Configuration processing
- ❌ Dependencies (will need boto3, duckdb, etc.)

## Configuration
- Expected format: `config.toml` (TOML)
- Default path: `./config.toml`
- Configurable via `--config/-c` flag
- Passed as first argument to all command functions

## Project Structure
```
finops/
├── cli.py              # Main entry point with argparse setup
├── commands/
│   ├── aws.py         # AWS subcommands (setup_aws_parser + functions)
│   └── config.py      # Legacy - not used, config is in cli.py
└── __init__.py        # Version definition
```

## Development Notes
- Python 3.12+ required
- Uses UV package manager
- Zero external dependencies currently
- Console script: `finops = "finops.cli:main"`
- No testing or linting configured yet