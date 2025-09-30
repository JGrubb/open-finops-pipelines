# State Management Refactor Plan

## Current State

- SQLite DB (`finops_state.db`) tracks manifests, exports, and runs
- Used by: manifest discovery, billing extraction, DuckDB loading, parquet export, BigQuery loading
- **Problems**: Can get out of sync with actual data, separate file to manage

## Proposed Architecture

Store state where the data lives:
- **Local pipeline state** (discover-manifests, extract-billing, load-data-local) → DuckDB `finops_state` schema
- **Remote load state** (load-billing-remote) → BigQuery table in same dataset as billing data
- **Benefits**: State lives with data, harder to get out of sync, one less file to manage

## Implementation Plan

### Phase 1: Create DuckDB State Schema

1. Modify `StateDB` class (`finops/services/state_db.py`):
   - Change `__init__` to accept DuckDB connection/path instead of SQLite path
   - Create `finops_state` schema in DuckDB
   - Migrate tables: `manifests`, `exports`, `runs` → `finops_state.manifests`, etc.
   - Keep same table structures and methods (minimal API changes)

Tables in `finops_state` schema:
```sql
finops_state.manifests
finops_state.exports
finops_state.runs
```

### Phase 2: Create BigQuery State Tracking

2. Create `BigQueryStateDB` class (new file or extend `BigQueryLoader`):
   - Track loads in `{table_id}_loads` table in same dataset
   - Methods: `save_load()`, `update_load_state()`, `get_loaded_periods()`
   - Store: `billing_period`, `load_timestamp`, `row_count`, `state` (loading/loaded/failed)

Table structure:
```sql
{dataset_id}.{table_id}_loads
- billing_period (STRING)
- load_timestamp (TIMESTAMP)
- row_count (INTEGER)
- state (STRING) -- loading/loaded/failed
- error_message (STRING)
```

### Phase 3: Update Service Integrations

3. Update all services to use new state management:
   - `manifest_discovery.py` → DuckDB state
   - `billing_extractor.py` → DuckDB state
   - `duckdb_loader.py` → DuckDB state
   - `parquet_exporter.py` → DuckDB state
   - `bigquery_loader.py` → BigQuery state (separate from local)

### Phase 4: Update Config and Commands

4. Remove `state_db` field from:
   - `finops/config.py` (FinopsConfig class)
   - `config.toml.example`

5. Update command functions in `finops/commands/aws.py`:
   - Instantiate `StateDB` with DuckDB path from config
   - Pass DuckDB connection path instead of separate SQLite path

6. Optional: Add migration helper to move existing SQLite data to DuckDB

## Key Decisions

### DuckDB State Schema
- Schema: `finops_state`
- Tables: `finops_state.manifests`, `finops_state.exports`, `finops_state.runs`
- Location: Same DuckDB file as billing data (`finops.duckdb`)

### BigQuery State Storage
- Store load tracking in `{table_id}_loads` table within same dataset
- Keeps everything together, no separate dataset needed

### Backward Compatibility
- Option A: Breaking change, users must re-discover manifests
- Option B: Provide one-time migration script
- **Recommendation**: Breaking change is simpler, state rebuild is fast

## Risks & Mitigations

**Risk**: If DuckDB file is deleted, lose all state
- **Mitigation**: State is reconstructable by re-running discovery

**Risk**: Multiple processes accessing DuckDB state simultaneously
- **Mitigation**: DuckDB handles concurrent reads well, writes are typically sequential in pipeline

**Risk**: BigQuery state separate from local creates new sync issue
- **Mitigation**: Expected behavior for remote system, intentional separation

## Implementation Notes

- Maintain same `StateDB` public API to minimize changes to calling code
- DuckDB schemas support namespacing: `CREATE SCHEMA IF NOT EXISTS finops_state`
- Table references: `finops_state.manifests` vs `main.aws_billing_data`
- Consider adding state export/import for debugging

## Questions for Review

1. BigQuery state table naming: `{table_id}_loads` or separate `_state` table?
2. Migration tool needed or acceptable breaking change?
3. Should state tables have different retention policy?