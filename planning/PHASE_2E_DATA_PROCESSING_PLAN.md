# Phase 2E: Data Processing & Loading Pipeline Plan

**Status**: 📋 PLANNED

## Problem Statement

Downloaded billing files (CSV/Parquet) need to be parsed, processed, and loaded into the DuckDB warehouse with proper schema handling and data validation.

**Current Gap**:
- No CSV/Parquet parsing infrastructure
- No schema inference from CUR data
- No data type handling and validation
- No bulk loading optimization
- No data deduplication logic

**Required Capabilities**:
- Parse AWS CUR CSV and Parquet files
- Infer and validate schema from data
- Handle data type conversions and cleaning
- Efficient bulk loading into DuckDB
- Data deduplication and conflict resolution

## Architecture Design

### Module Structure
```
src/finops/pipeline/processing/
├── __init__.py
├── parsers.py       # CSV/Parquet parsing logic
├── schema.py        # Schema inference and validation
├── transformers.py  # Data cleaning and transformation
└── loaders.py       # Database loading operations
```

### Processing Workflow
1. **Detection**: Identify file format (CSV vs Parquet)
2. **Parsing**: Read files with appropriate parser
3. **Schema**: Infer schema and validate against expected format
4. **Transform**: Clean data, handle nulls, standardize types
5. **Load**: Bulk insert into DuckDB with conflict handling
6. **Validate**: Row counts, data integrity checks

## Implementation Strategy

### 1. File Format Support
- **CSV**: Compressed (.gz) and uncompressed
- **Parquet**: Native DuckDB Parquet integration
- **Auto-detection**: Based on file extensions and content

### 2. Schema Management
```python
# Dynamic schema inference from CUR data
class CURSchema:
    def infer_from_file(self, file_path: str) -> Dict[str, str]
    def validate_against_expected(self, schema: Dict) -> bool
    def generate_ddl(self, table_name: str) -> str
```

### 3. Data Transformations
- Date/timestamp parsing and standardization
- Numeric precision handling for costs
- String trimming and normalization
- Null value handling strategies

### 4. Loading Strategy
- **Batch Processing**: Process files in configurable chunks
- **Upsert Logic**: Handle overlapping billing periods
- **Transaction Management**: All-or-nothing loading
- **Progress Tracking**: Real-time loading status

## Integration Points

### State Management
- Update state to "loading" during processing
- Mark as "loaded" or "failed" based on outcome
- Track processing metrics (rows loaded, errors)

### DuckDB Backend
- Use Phase 2C backend for database operations
- Schema creation and table management
- Bulk loading optimizations

### Configuration
```toml
[pipeline.processing]
batch_size = 10000
parallel_processing = true
validate_checksums = true
handle_duplicates = "latest"  # latest, skip, error
```

## Error Handling Strategy

### Data Quality Issues
- Invalid date formats
- Missing required columns
- Data type mismatches
- Encoding problems

### Processing Failures
- Insufficient memory for large files
- Database connection failures
- Disk space constraints
- Schema evolution conflicts

## TDD Test Strategy
1. **Parser Tests**: Various file formats and edge cases
2. **Schema Tests**: Inference accuracy and validation
3. **Transform Tests**: Data cleaning and type conversion
4. **Loading Tests**: Bulk operations and conflict handling
5. **Integration Tests**: End-to-end processing workflows

## Success Criteria
- ✅ Accurate parsing of AWS CUR data files
- ✅ Robust schema inference and validation
- ✅ Efficient bulk loading with progress tracking
- ✅ Data quality validation and error reporting
- ✅ Configurable processing options
- ✅ Transaction safety and rollback capability

## Performance Considerations
- Memory-efficient streaming for large files
- Parallel processing where possible
- DuckDB-native optimizations (COPY commands)
- Column-oriented processing for Parquet files