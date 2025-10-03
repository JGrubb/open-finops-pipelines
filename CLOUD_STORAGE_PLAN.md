# Cloud Storage Implementation Plan

## Goal
Support both local filesystem and GCS storage as first-class options for the finops pipeline, with easy configuration switching between them.

## Architecture

### Storage Modes

**Local Mode** (current, default):
```
S3 CUR → ./data/staging/ → DuckDB (./data/finops.duckdb) → ./data/exports/ → BigQuery (optional)
```

**GCS Mode** (new):
```
S3 CUR → gs://bucket/staging/ → DuckDB (in-memory) → gs://bucket/exports/ → BigQuery
```

### Key Design Decisions

1. **Single `data_dir` config**: All pipeline data (staging, exports, duckdb) lives under one configurable directory
   - Local: `data_dir = "./data"` → staging at `./data/staging`, exports at `./data/exports`
   - GCS: `data_dir = "gs://bucket/finops"` → staging at `gs://bucket/finops/staging`, exports at `gs://bucket/finops/exports`

2. **In-memory DuckDB for GCS mode**: No persistent DuckDB file in cloud storage
   - DuckDB provides schema normalization and typing (crucial!)
   - Ephemeral database per run
   - BigQuery is source of truth for loaded state

3. **Storage abstraction layer**: Single interface for file operations
   - `StorageBackend` ABC with `LocalStorage` and `GCSStorage` implementations
   - Services use storage abstraction, not direct `Path()` operations

4. **Authentication**:
   - GCS: Service account JSON key file via `google-cloud-storage` library
   - AWS: Existing HMAC keys for S3 CUR access (unchanged)

## Configuration Structure

```toml
# config.toml
data_dir = "./data"  # or "gs://bucket/finops"

[aws]
bucket = "my-cur-bucket"
prefix = "cur-reports"
export_name = "my-cur-export"
aws_access_key_id = "..."
aws_secret_access_key = "..."
region = "us-east-1"
cur_version = "v2"

[database]
backend = "duckdb"  # or "bigquery"

[database.duckdb]
database_path = "./data/finops.duckdb"  # ignored if data_dir is gs://

[database.bigquery]
project_id = "my-project"
dataset_id = "finops"
table_id = "aws_billing"
credentials_path = "./gcs-service-account.json"

[gcs]
# Only needed when data_dir uses gs:// protocol
credentials_path = "./gcs-service-account.json"
project_id = "my-gcp-project"
```

## Implementation Tasks

### Phase 1: Configuration & Abstraction
1. ✅ Simplify config to use single `data_dir` with derived `staging_dir` and `parquet_dir` properties
2. Add GCS config section to config.py
3. Create `StorageBackend` ABC with operations:
   - `exists(path) -> bool`
   - `read_file(path) -> bytes`
   - `write_file(path, data)`
   - `list_dir(path) -> List[str]`
   - `delete(path)`
   - `glob(pattern) -> List[str]`
   - `mkdir(path)`
4. Implement `LocalStorage` backend (wraps `pathlib.Path`)
5. Implement `GCSStorage` backend (uses `google-cloud-storage`)
6. Create factory function: `get_storage_backend(config) -> StorageBackend`

### Phase 2: Service Updates
7. Update `DuckDBLoader`:
   - Support `database_path=None` for in-memory mode
   - Accept `StorageBackend` for reading CSV files from staging
8. Update `BillingExtractor`:
   - Accept `StorageBackend` for writing CSVs
   - Use storage backend for `get_staged_execution_ids()` and `clean_old_execution_ids()`
9. Update `ParquetExporter`:
   - Accept `StorageBackend` for writing Parquet files
   - Handle in-memory DuckDB (no file path needed)
10. Update `BigQueryLoader`:
    - Accept `StorageBackend` for reading Parquet files
11. Update `commands/aws.py`:
    - Initialize storage backend from config
    - Pass storage backend to all services
    - Handle DuckDB path (None for GCS mode)

### Phase 3: Dependencies & Testing
12. Add `google-cloud-storage` to pyproject.toml
13. Test local mode (existing behavior)
14. Test GCS mode end-to-end
15. Update README with GCS setup instructions

## File Operations Mapping

| Operation | Local | GCS |
|-----------|-------|-----|
| Write CSV | `Path.write_bytes()` | `blob.upload_from_string()` |
| Read CSV | `Path.read_bytes()` | `blob.download_as_bytes()` |
| List execution_ids | `Path.iterdir()` | `client.list_blobs(prefix=..., delimiter='/')` |
| Delete old execution | `shutil.rmtree()` | `bucket.delete_blobs(prefix=...)` |
| Write Parquet | `Path.write_bytes()` | `blob.upload_from_file()` |
| Read Parquet | `Path.read_bytes()` | `blob.download_as_bytes()` |
| Check exists | `Path.exists()` | `blob.exists()` |
| Find CSV files | `Path.glob("*.csv.gz")` | `list_blobs(prefix=..., match=...)` |

## Backwards Compatibility

- Existing local configs continue to work (default `data_dir = "./data"`)
- All existing CLI commands work unchanged
- No migration needed for local mode
- GCS mode is opt-in via config change

## Future Enhancements

- Add S3 storage backend (if needed for staging billing data)
- Support Azure Blob Storage
- Add caching layer for frequently accessed files
- Parallel uploads/downloads for large files
