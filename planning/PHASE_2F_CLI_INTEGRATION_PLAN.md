# Phase 2F: Complete CLI Integration Plan

**Status**: 📋 PLANNED

## Problem Statement

The `import_billing_command()` function is currently a placeholder that only prints configuration values. It needs to orchestrate the complete pipeline workflow developed in phases 2B-E.

**Current Placeholder**:
```python
def import_billing_command(config, args):
    """Import AWS CUR billing data."""
    print(f"Importing billing data from bucket: {config.aws.bucket}")
    print(f"Export name: {config.aws.export_name}")
    print(f"Reset tables: {getattr(args, 'reset', False)}")
```

**Required Functionality**:
- Execute complete pipeline: discover → download → process → load
- Progress reporting and user feedback
- Error handling and recovery options
- Integration with state management
- Support for pipeline resume and reset operations

## Architecture Design

### Command Enhancement Strategy
Replace placeholder with full pipeline orchestration:

```python
def import_billing_command(config, args):
    """Execute complete AWS CUR data import pipeline."""
    # 1. Initialize pipeline components
    # 2. Get manifests ready for processing (Phase 2B)
    # 3. Download files from S3 (Phase 2D)
    # 4. Process and load data (Phase 2E)
    # 5. Update state management
    # 6. Report results and cleanup
```

### User Experience Design
- **Progress Indicators**: Real-time status of each pipeline stage
- **Error Recovery**: Clear error messages with suggested actions
- **Resume Capability**: Continue from failed/interrupted runs
- **Reset Options**: Clean slate processing with `--reset` flag

## Implementation Strategy

### 1. Pipeline Orchestration
```python
class ImportPipelineOrchestrator:
    def __init__(self, config, state_manager, backend)
    def execute_import(self, reset: bool = False) -> ImportResult
    def resume_failed_imports(self) -> ImportResult
    def get_pipeline_status(self) -> PipelineStatus
```

### 2. Progress Reporting
- Stage-based progress (Discovering → Downloading → Processing → Loading)
- File-level progress for downloads
- Row-level progress for data loading
- ETA calculations and throughput metrics

### 3. Command Line Options
```bash
# Basic import
finops aws import-billing

# Reset and reimport all data
finops aws import-billing --reset

# Resume failed imports
finops aws import-billing --resume

# Dry run (discover only)
finops aws import-billing --dry-run
```

### 4. Integration Points
- **Phase 2B**: Use `get_pipeline_ready_manifests()` for manifest selection
- **Phase 2C**: Initialize DuckDB backend connection
- **Phase 2D**: Execute file download pipeline
- **Phase 2E**: Execute data processing and loading

## Error Handling Strategy

### Pipeline Failures
- **Discovery Errors**: AWS credentials, bucket access
- **Download Errors**: Network failures, insufficient disk space
- **Processing Errors**: Corrupt data, schema conflicts
- **Loading Errors**: Database connection, constraint violations

### User Guidance
- Clear error messages with suggested fixes
- Recovery options (retry, reset, skip)
- Log file references for detailed debugging
- State inspection commands for troubleshooting

## CLI User Experience

### Success Output
```
✓ Discovered 3 manifests ready for processing
✓ Downloaded 15 files (2.3 GB) to staging area
✓ Processed 1,205,432 billing records
✓ Loaded data into DuckDB warehouse
✓ Cleaned up staging files

Pipeline completed successfully in 4m 32s
```

### Error Output
```
✗ Failed to download file: network timeout

  Retry with: finops aws import-billing --resume
  Reset all: finops aws import-billing --reset

  See logs: ~/.finops/logs/import_2024-01-15_14-30.log
```

## Testing Strategy
1. **Unit Tests**: Command argument parsing and validation
2. **Integration Tests**: Full pipeline execution with mocked components
3. **End-to-End Tests**: Complete workflow with real AWS data
4. **Error Simulation**: Test failure scenarios and recovery
5. **Performance Tests**: Large dataset import benchmarks

## Configuration Enhancements
```toml
[pipeline]
max_concurrent_operations = 4
progress_reporting = true
cleanup_on_success = true
log_level = "INFO"

[pipeline.retry]
max_attempts = 3
backoff_factor = 2.0
```

## Success Criteria
- ✅ Complete pipeline execution from single command
- ✅ Intuitive progress reporting and user feedback
- ✅ Robust error handling with recovery options
- ✅ Integration with all pipeline phases (2B-E)
- ✅ Performance suitable for production workloads
- ✅ Comprehensive logging and debugging support

## Future Enhancements
- Scheduled/cron-based imports
- Incremental import optimization
- Multi-vendor pipeline support (Azure)
- Web UI integration for pipeline monitoring