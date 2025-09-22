# Phase 2A: Pipeline State Database Implementation Plan

**Status**: ✅ COMPLETED SUCCESSFULLY

## Overview
Create a SQLite-based state tracking system to avoid reprocessing billing data that has already been loaded, using a generic identifier that works across vendors (AWS, Azure).

## Architecture Analysis

### Naming Convention Decision
**Recommendation**: Use `billing_version_id` instead of `assembly_id`
- **Rationale**: More vendor-agnostic than AWS-specific `assembly_id`
- **AWS mapping**: `billing_version_id = manifest.assembly_id`
- **Azure mapping**: Will map to equivalent versioning identifier when implemented
- **Future-proof**: Works for any vendor's versioning scheme

### Database Structure

**Location**: Configurable via config.toml, default: `./data/pipeline_state.db` (SQLite)
- Matches existing `DuckDBConfig.database_path = "./data/finops.duckdb"` pattern
- Creates `data/` directory if it doesn't exist

**Schema**:
```sql
CREATE TABLE billing_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor TEXT NOT NULL,                    -- 'aws', 'azure', etc.
    billing_version_id TEXT NOT NULL,       -- Assembly ID (AWS) or equivalent
    billing_month TEXT NOT NULL,            -- 'YYYY-MM' format
    export_name TEXT NOT NULL,              -- CUR export name or equivalent
    state TEXT NOT NULL,                    -- 'discovered', 'loading', 'loaded', 'failed'
    is_current BOOLEAN NOT NULL DEFAULT 1,  -- Latest version for this month
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,                     -- For failed states
    UNIQUE(vendor, billing_version_id)
);
```

## Implementation Strategy

### 1. Create State Management Module
```
src/finops/state/
├── __init__.py
├── models.py      # SQLAlchemy models and enums
├── manager.py     # State management operations
└── database.py    # Database initialization and utilities
```

### 2. State Workflow
1. **Discovery**: Mark manifests as 'discovered'
2. **Loading**: Update to 'loading' when processing starts
3. **Completion**: Mark as 'loaded' or 'failed'
4. **Currency**: When new assembly_id found for same billing month, mark old as `is_current=false`

### 3. Integration Points
- **ManifestDiscovery**: Check state before returning manifests
- **CLI Commands**: Show state information, support state filtering
- **Future Import**: Update state during data loading

### 4. Dependencies
- Add `sqlalchemy>=2.0.0` for ORM

### 5. Error Handling
- Fail gracefully when state database is corrupted or missing
- Offer to recreate database (resets entire state and billing data tables)

### 6. Configuration
- Add state database path to config schema
- Default: `./data/pipeline_state.db`

## Benefits
1. **Avoid Reprocessing**: Skip already-loaded billing periods
2. **Resume Failed Jobs**: Retry only failed imports
3. **Audit Trail**: Track what's been processed and when
4. **Multi-Vendor Ready**: Generic design supports AWS + Azure
5. **Incremental Processing**: Process only new/updated manifests

## File Changes Required
- `pyproject.toml`: Add SQLAlchemy dependency
- `src/finops/state/`: New module (4 files)
- `src/finops/vendors/aws/manifest.py`: State integration
- `src/finops/cli/aws.py`: Enhanced commands with state
- `tests/test_state.py`: Comprehensive state management tests
- `data/`: New directory for databases

## Testing Strategy
- Unit tests for state operations (create, update, query)
- Integration tests with manifest discovery
- CLI tests for new state-aware commands
- Test state transitions and error handling

---

## ✅ IMPLEMENTATION COMPLETED

### What Was Delivered
- **Complete state management system** using SQLite (no additional dependencies)
- **Daily workflow integration** in `list-manifests` command
- **TDD approach** with comprehensive test coverage (44 tests passing)
- **State-aware CLI commands** with clear user feedback

### Key Changes Made
1. **Dependencies**: Used built-in `sqlite3` instead of SQLAlchemy (simpler, no dependencies)
2. **Implementation**: TDD approach - tests first, then implementation
3. **CLI Integration**: `list-manifests` now records/skips manifests based on state
4. **File Structure**:
   ```
   src/finops/state/
   ├── __init__.py
   ├── database.py      # SQLite initialization and connections
   ├── manager.py       # StateManager with CRUD operations
   └── models.py        # Pydantic models (StateRecord)

   tests/
   ├── test_state_manager.py      # Core TDD tests
   └── test_cli_integration.py    # CLI workflow tests
   ```

### Daily Workflow Now Works
1. **First run**: `finops aws list-manifests` discovers new manifests → records as "discovered"
2. **Subsequent runs**: Skips already seen manifests → only shows new ones
3. **State viewing**: `finops aws show-state` shows complete pipeline history
4. **Processing ready**: `StateManager.get_manifests_to_process()` ready for future pipeline

### Configuration Added
```toml
[state]
database_path = "./data/pipeline_state.db"  # Default location
```

### Test Coverage
- **5 core TDD tests** - daily workflow scenarios
- **2 CLI integration tests** - end-to-end workflow simulation
- **37 existing tests** - all still passing
- **Total: 44 tests passing** ✅

The system is now ready for Phase 2B (actual data pipeline implementation).