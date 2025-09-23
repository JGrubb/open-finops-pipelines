# Phase 2D: Manifest File Download Pipeline Plan

**Status**: 📋 PLANNED

## Problem Statement

Manifests contain references to billing data files in S3, but there's no mechanism to download these files for local processing and loading into the warehouse.

**Current Gap**:
- Manifest parsing extracts file lists (`manifest.files`, `manifest.report_keys`)
- No download orchestration or file management
- No staging area for downloaded files
- No validation of downloaded file integrity

**Required Capabilities**:
- Download billing files from S3 to local staging area
- Validate file integrity (checksums, sizes)
- Manage staging directory cleanup
- Handle partial downloads and resume capability
- Progress tracking for large file downloads

## Architecture Design

### Module Structure
```
src/finops/pipeline/
├── __init__.py
├── downloader.py    # S3 file download orchestration
├── staging.py       # Local file staging management
├── validator.py     # File integrity validation
└── progress.py      # Download progress tracking
```

### Download Workflow
1. **Pre-download**: Create staging directory, check available space
2. **Download**: Multi-threaded download of manifest files
3. **Validation**: Verify checksums and file sizes
4. **Staging**: Organize files for backend loading
5. **Cleanup**: Remove files after successful loading

## Implementation Strategy

### 1. Staging Directory Management
```python
# Default staging structure
./data/staging/{vendor}/{billing_month}/{assembly_id}/
├── manifest.json
├── billing_data_001.csv.gz
├── billing_data_002.csv.gz
└── .checksums
```

### 2. Download Orchestration
- Parallel downloads with configurable concurrency
- Resume capability for interrupted downloads
- Bandwidth throttling options
- Progress reporting to CLI

### 3. Integration Points
- **State Management**: Update to "loading" during downloads
- **AWS Client**: Reuse existing S3 client infrastructure
- **Configuration**: Download settings (staging path, concurrency)

### 4. Error Handling
- Network failures and retries
- Insufficient disk space
- Corrupted file recovery
- S3 permission errors

## Configuration
```toml
[pipeline.download]
staging_path = "./data/staging"
max_concurrent_downloads = 4
retry_attempts = 3
checksum_validation = true
cleanup_after_load = true
```

## TDD Test Strategy
1. **Unit Tests**: File download, validation, staging
2. **Integration Tests**: Full manifest download workflow
3. **Mock S3 Tests**: Simulate network failures and retries
4. **Performance Tests**: Large file download scenarios

## Dependencies
- Existing AWS S3 client (`src/finops/vendors/aws/client.py`)
- State management integration
- Configuration system extension

## Success Criteria
- ✅ Reliable download of all files referenced in manifests
- ✅ Integrity validation prevents corrupted data loading
- ✅ Resumable downloads handle network interruptions
- ✅ Progress tracking provides user feedback
- ✅ Automatic cleanup prevents disk space issues
- ✅ Configurable performance tuning

## Future Enhancements
- Incremental downloads (only changed files)
- Compression/decompression during download
- Cloud-to-cloud transfers (bypass local staging)
- Download caching for development workflows