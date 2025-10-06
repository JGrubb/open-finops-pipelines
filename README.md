# Open FinOps Pipelines

A lightweight, vendor-agnostic CLI tool for ingesting cloud billing data into analytical databases. Extract AWS Cost and Usage Reports (CUR) from S3, load into DuckDB for local analysis, and optionally sync to BigQuery for cloud-based analytics.

## Features

- **Multi-stage pipeline**: S3 → Staging → DuckDB → Parquet → BigQuery
- **Idempotent operations**: Skip already-processed data at every stage
- **Schema evolution**: Automatically detect and add new columns as AWS introduces them
- **State tracking**: Query destination databases to avoid reprocessing
- **Flexible backends**: Use DuckDB locally, BigQuery remotely, or both
- **CUR v1 & v2 support**: Handle both versions of AWS Cost and Usage Reports

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│     S3      │───▶│   Staging    │───▶│   DuckDB    │
│  CUR Data   │    │   CSV Files  │    │   (Local)   │
└─────────────┘    └──────────────┘    └──────┬──────┘
                                              │
                   ┌──────────────┐           │
                   │   Parquet    │◀──────────┘
                   │    Export    │
                   └──────┬───────┘
                          │
                   ┌──────▼───────┐
                   │  BigQuery    │
                   │   (Remote)   │
                   └──────────────┘
```

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/open_finops_pipelines.git
cd open_finops_pipelines

# Install with uv (recommended)
uv pip install -e .

# Or with pip
pip install -e .
```

## Quick Start

1. **Create configuration file:**

```bash
cp config.toml.example config.toml
# Edit config.toml with your AWS credentials and S3 bucket details
```

2. **Run the complete pipeline:**

```bash
finops aws run-pipeline
```

Or run individual steps:

```bash
# 1. Discover available manifests (diagnostic)
finops aws discover-manifests

# 2. Extract billing files from S3
finops aws extract-billing

# 3. Load to local DuckDB
finops aws load-billing-local

# 4. Export to Parquet
finops aws export-parquet

# 5. Load to BigQuery (optional)
finops aws load-billing-remote
```

## CLI Commands

### Global Commands

```bash
finops config                    # Display current configuration
finops --version                 # Show version
finops --help                    # Show help
```

### AWS Commands

```bash
# Diagnostic: Check what data is available vs loaded
finops aws discover-manifests [--bucket BUCKET] [--prefix PREFIX] [--export-name NAME]

# Extract billing files from S3 to staging directory
finops aws extract-billing [--start-date YYYY-MM] [--end-date YYYY-MM] [--staging-dir PATH]

# Load staged files into local DuckDB
finops aws load-billing-local [--start-date YYYY-MM] [--end-date YYYY-MM]

# Export DuckDB data to Parquet files
finops aws export-parquet [--output-dir PATH] [--start-date YYYY-MM] [--end-date YYYY-MM] [--overwrite]

# Load Parquet files to BigQuery
finops aws load-billing-remote [--start-date YYYY-MM] [--end-date YYYY-MM] [--overwrite]

# Run complete pipeline (all steps)
finops aws run-pipeline [--start-date YYYY-MM] [--end-date YYYY-MM] [--dry-run]
```

## Configuration

Configuration is stored in `config.toml`:

```toml
# Global settings
staging_dir = "./data/staging"
parquet_dir = "./data/exports"

[database.duckdb]
database_path = "./data/finops.duckdb"

[database.bigquery]
project_id = "your-project"
dataset_id = "your_dataset"
table_id = "aws_billing_data"
credentials_path = "/path/to/service-account.json"

[aws]
bucket = "your-cur-bucket"
prefix = "path/to/cur"
export_name = "your-export-name"
cur_version = "v2"  # or "v1"
aws_access_key_id = "YOUR_KEY"
aws_secret_access_key = "YOUR_SECRET"
region = "us-east-1"
```

## How It Works

### 1. Manifest Discovery

Scans S3 for CUR manifest files and determines what data is available:
- Lists manifest JSON files from S3
- Parses billing periods and execution IDs
- Queries destination databases to check what's already loaded
- Returns only unprocessed manifests

### 2. Billing Extraction

Downloads CSV files from S3 to local staging directory:
- Creates directory structure: `{staging_dir}/{billing_period}/{execution_id}/`
- Skips already-downloaded files (idempotent)
- Auto-cleans old execution IDs for same billing period

### 3. DuckDB Loading

Loads CSV data into local DuckDB with schema evolution:
- Automatically creates table on first run
- Detects new columns from manifests and adds them via ALTER TABLE
- Normalizes column names (`identity/LineItemId` → `identity_line_item_id`)
- Forces `resourcetags_*` columns to VARCHAR
- Deletes existing billing period data before loading new execution
- Tracks execution_id for deduplication

### 4. Parquet Export

Exports DuckDB data to monthly Parquet files:
- One file per billing period: `{billing_period}_aws_billing.parquet`
- Snappy compression by default
- Skips existing files unless `--overwrite` specified

### 5. BigQuery Loading

Loads Parquet files to BigQuery:
- Auto-creates table with monthly partitioning on `bill_billing_period_start_date`
- Clusters on `line_item_usage_start_date`
- DELETE + APPEND strategy per billing period
- Schema evolution via `ALLOW_FIELD_ADDITION`

## State Management

The pipeline uses a stateless approach, checking actual data sources instead of maintaining a separate state database:

- **Extraction state**: Filesystem (checks staging directory)
- **Loading state**: DuckDB (queries for execution_ids)
- **Remote state**: BigQuery (queries for billing periods)

This ensures state is always accurate and eliminates sync issues.

## Schema Evolution

As AWS adds new columns to CUR data:

1. Manifest contains column definitions
2. DuckDB loader detects new columns
3. Executes `ALTER TABLE ADD COLUMN` for each new field
4. Loads data with expanded schema
5. BigQuery inherits schema from Parquet (auto-detects new fields)

Special handling:
- `resourcetags_*` columns forced to VARCHAR (prevents type conflicts)
- Column names normalized (replace `/` and special chars with `_`)
- Manifests sorted DESC by billing_month to establish schema early

## Deployment

### Docker

Build and run the pipeline in a container:

```bash
# Build the image
docker build -t finops-pipeline .

# Run with mounted config and data volumes
docker run --rm \
  -v $(pwd)/config.toml:/app/config.toml:ro \
  -v $(pwd)/data:/app/data \
  finops-pipeline finops aws run-pipeline

# Run a specific command
docker run --rm \
  -v $(pwd)/config.toml:/app/config.toml:ro \
  finops-pipeline finops aws discover-manifests
```

### Google Cloud Run + Cloud Scheduler

Deploy as a scheduled job on GCP:

```bash
# 1. Set project and region
export PROJECT_ID=your-project-id
export REGION=us-central1
export SERVICE_NAME=finops-pipeline

# 2. Enable required APIs
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com

# 3. Create Artifact Registry repository (one-time)
gcloud artifacts repositories create finops \
  --repository-format=docker \
  --location=$REGION

# 4. Build and push image
gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/finops/$SERVICE_NAME

# 5. Deploy to Cloud Run (no public access, manual execution)
gcloud run deploy $SERVICE_NAME \
  --image $REGION-docker.pkg.dev/$PROJECT_ID/finops/$SERVICE_NAME \
  --region $REGION \
  --no-allow-unauthenticated \
  --memory 2Gi \
  --timeout 3600 \
  --set-env-vars "CONFIG_PATH=/app/config.toml"

# 6. Create Cloud Scheduler job (daily at 2 AM UTC)
gcloud scheduler jobs create http finops-daily \
  --location $REGION \
  --schedule "0 2 * * *" \
  --uri "https://$(gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)')" \
  --http-method POST \
  --oidc-service-account-email YOUR_SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com
```

Configuration options:
- Mount config.toml as a Secret Manager secret
- Use environment variables for sensitive credentials
- Adjust memory/timeout based on data volume
- Override CMD to run specific commands

### Environment Variables

Instead of config.toml, you can use environment variables:

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_DEFAULT_REGION=us-east-1
export FINOPS_S3_BUCKET=your-bucket
export FINOPS_S3_PREFIX=path/to/cur
# ... etc
```

## Development

```bash
# Install development dependencies
uv pip install -e ".[dev]"

# Run tests
pytest

# Check types
mypy finops/

# Format code
black finops/
```

## Project Structure

```
finops/
├── cli.py                      # Main CLI entry point
├── config.py                   # Configuration management
├── commands/
│   └── aws.py                  # AWS command implementations
└── services/
    ├── manifest_discovery.py   # S3 manifest scanning
    ├── billing_extractor.py    # S3 → staging download
    ├── duckdb_loader.py        # CSV → DuckDB loading
    ├── parquet_exporter.py     # DuckDB → Parquet export
    ├── bigquery_loader.py      # Parquet → BigQuery loading
    ├── state_checker.py        # Query destination databases
    └── schema_manager.py       # Schema evolution logic
```

## Roadmap

- [ ] Azure billing support
- [ ] GCP billing support
- [ ] ClickHouse backend support
- [ ] Web UI for pipeline monitoring
- [ ] Cost anomaly detection
- [ ] Budget alerting

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.
