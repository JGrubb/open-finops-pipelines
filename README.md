# Open Finops Pipelines

This is an open source project to implement data pipelines that will ingest billing data from cloud vendors - AWS and Azure for starters - and load it into arbitrary analytical databases for analysis - DuckDB for local analysis and BigQuery as a cloud warehouse (for starters).

We'll be proceeding with development in very small steps, and following a test driven development pattern: 
- Write tests for desired functionality
- Ensure that tests fail
- Implement the missing functionality
- Ensure the tests pass

This will be a python project.  We'll use uv as the package manager and for handling virtualenvs.

## CLI API Design

The API of the CLI interface for the project is as follows:

### Core Commands

```bash
# Primary import command
./finops aws import-billing [OPTIONS]

# Manifest exploration
./finops aws list-manifests [OPTIONS]

# State inspection
./finops aws show-state [OPTIONS] # shows previous executions of the pipeline and their state
```

### CLI Arguments

**Required (via config.toml, environment, or CLI):**
- `--bucket, -b`: S3 bucket containing CUR files
- `--export-name, -n`: Name of the CUR export
- AWS credentials (access key, secret key, region)

**Optional:**
- `--config, -c`: Path to config.toml file (default: ./config.toml)
- `--prefix, -p`: S3 prefix/path to CUR files (default: "")
- `--cur-version, -v`: CUR version v1|v2 (default: v1)
- `--export-format, -f`: File format csv|parquet (default: auto-detect)
- `--start-date, -s`: Start date YYYY-MM for import (default: all available)
- `--end-date, -e`: End date YYYY-MM for import (default: all available)
- `--reset, -r`: Drop existing tables before import

**Configuration Precedence:**
1. CLI arguments (highest priority)
2. Environment variables (OPEN_FINOPS_AWS_*)
3. config.toml file
4. Defaults (lowest priority)

## AWS CUR Business Logic

### Manifest Discovery

AWS CUR uses manifest files to track billing exports:

**CUR v1 Structure:**
```
s3://bucket/prefix/export-name/YYYYMMDD-YYYYMMDD/
├── export-name-Manifest.json          # Manifest file
├── export-name-00001.csv.gz           # Data files
├── export-name-00002.csv.gz
└── ...
```

**CUR v2 Structure:**
```
s3://bucket/prefix/export-name/year=YYYY/month=MM/
├── export-name-Manifest.json          # Manifest file
├── part-00000-*.parquet               # Data files
├── part-00001-*.parquet
└── ...
```

Azure manifest discovery is TBD, but plan on using a very similar algorithm to handle Azure billing data.

### Manifest Business Rules

1. **Discovery Pattern**: Find manifests by listing S3 objects with pattern `*-Manifest.json`
2. **Date Filtering**: Filter manifests by billing period (YYYY-MM format)
3. **Version Detection**: Detect CUR version from manifest structure and file types
4. **Assembly Tracking**: Each manifest has an assembly ID for versioning
5. **File Enumeration**: Manifest contains list of actual data files to process

### Data Processing Logic

1. **File Format Detection**: Auto-detect CSV vs Parquet from file extensions
2. **Progress Tracking**: Report progress per manifest and per file
3. **Error Handling**: Skip missing files, fail on permission errors
4. **Row Counting**: Track and report row counts for monitoring

## Configuration Schema

```toml
[aws]
bucket = "my-cur-bucket"
prefix = "cur-exports"
export_name = "my-cur-export"
cur_version = "v1"
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
region = "us-east-1"
start_date = "2024-01"
end_date = "2024-12"
dataset_name = "aws_billing"

[database]
backend = "duckdb"
[database.duckdb]
database_path = "./data/finops.duckdb"
```