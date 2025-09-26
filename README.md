# AWS Billing Pipeline Specification

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

## CLI interface

Each of these steps should be available via a CLI command.

```
Open source FinOps data pipelines for cloud billing analysis

positional arguments:
  {config,aws}          Available commands
    config              Display current configuration
    aws                 AWS billing data operations

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --config CONFIG, -c CONFIG
                        Path to config.toml file (default: ./config.toml)
```
```
± uv run finops aws --help
usage: finops aws [-h] {import-billing,list-manifests,show-state,extract-billing,load-billing} ...

positional arguments:
  {discover-manifests,extract-manifests,show-state,extract-billing,load-billing}
                        AWS commands
    discover-manifests      Import AWS CUR billing data
    extract-manifests      List available CUR manifest files
    show-state          Show previous pipeline executions and their state
    extract-billing     Extract billing files from S3 to staging directory
    load-billing-local        Load staged billing files into database
    export-parquet          Exports each month to Parquet format
    load-billing-remote

options:
  -h, --help            show this help message and exit
```


## Pipeline Steps

### 1. Manifest Discovery
```python
def discover_manifests():
    manifests = s3_client.list_manifests(bucket, prefix, export_name) # there are slight difference in pathing 
    # between v1 and v2, this is the biggert difference in the
    # algo between the two.
    manifests.sort(key=billing_month, reverse=True)  # newest first
    for manifest in manifests:
        extract_manifest(manifest.id, manifest.billing_month_start_date)
    return manifests
```

### 2. Manifest Extraction
```python
def extract_manifest(manifest):
    # manifest.id is assembly_id in v1, execution_id in v2
    state_db.update(manifest.id, "downloading manifest")
    local_path = staging_dir / manifest.id
    path_to_manifest = s3_client.download(manifest.files, local_path)
    decompress_files(local_path)
    state_db.update(manifest.id, path_to_manifest, "downloaded manifest")
    return local_path
```

### 4. Billing Extraction
```python
def extract_billing_files():
    # Query state DB for new manifests, group by month
    new_manifests = state_db.query("""
        SELECT * FROM manifests
        WHERE state = 'staged'
        ORDER BY billing_month DESC
    """)

    for manifest in new_manifests:
        staging_path = load_manifest(manifest)
        yield {
            'manifest_id': manifest.id,
            'billing_month': manifest.billing_month,
            'files': staging_path.glob('*.csv')
        }
```

### 5. Billing Load (DuckDB)
```python
def load_to_duckdb(billing_data):
    state_db.update(manifest.id, "loading")

    for file in billing_data.files:
        # Schema negotiation
        columns = detect_columns(file)
        clean_columns = normalize_column_names(columns) #convert all nonalphanumeric to _
        types = detect_types(file, force_resourcetags_varchar=True)

        # Table management
        if not table_exists(billing_data.billing_month):
            create_table(clean_columns, types)
        else:
            add_missing_columns(existing_table, clean_columns, types)

        # Data load
        delete_existing_month_data(billing_data.billing_month)
        bulk_insert(file, target_table)

    state_db.update(manifest.id, "loaded")
```

## Remote Pipeline Extension

### 6. Export to Parquet
```python
def export_to_parquet(month_range):
    for month in month_range:
        duckdb.execute("""
            COPY (SELECT * FROM billing_data
            WHERE billing_month = {month}
            ORDER BY billing_month DESC)
            TO 'export.parquet' (FORMAT PARQUET)
        """)
```

### 7. Load to BigQuery
```python
def load_to_bigquery():
    bq_client.load_table_from_file(
        'export.parquet',
        table_id='billing.aws_data',
        job_config=LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            write_disposition=WriteDisposition.WRITE_TRUNCATE
        )
    )
```

## State Transitions
```
discovered -> downloading -> staged -> loading -> loaded
     |            |           |         |
   failed      failed      failed    failed
```

## Key Implementation Notes

- **Sort manifests DESC by billing_month** to establish proper schema first
- **Force resourcetags_* columns to VARCHAR** to prevent type confusion
- **Clean column names**: `identity/LineItemId` -> `identity_lineitemid`
- **Additive schema evolution**: ALTER TABLE to add new columns
- **Batch operations**: CREATE TABLE AS SELECT, bulk inserts
- **State tracking**: SQLite DB with vendor, manifest_id, billing_month, state