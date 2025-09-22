# Open FinOps Pipelines - Roadmap

## Project Status

**Current State**: Early development with basic CLI foundation

**Completed**:
- ✅ Basic uv project structure with console script entry point
- ✅ Modular CLI architecture (`src/finops/cli/`)
- ✅ AWS subcommand structure (`finops aws --help`)
- ✅ Test framework setup with `uv run test` shortcut
- ✅ TDD workflow established

## Phase 1: CLI Foundation (In Progress)

### Next Steps
- [ ] Add AWS import-billing command structure
- [ ] Add AWS list-manifests command structure
- [ ] Add AWS show-state command structure
- [ ] Implement basic argument parsing for required options (bucket, export-name)
- [ ] Add configuration file support (config.toml)

## Phase 2: AWS CUR Discovery

### Manifest Discovery Logic
- [ ] S3 client setup with credential handling
- [ ] Manifest file discovery (`*-Manifest.json` pattern)
- [ ] CUR version detection (v1 vs v2)
- [ ] Date filtering for billing periods
- [ ] Assembly ID tracking

### Data Processing
- [ ] File format detection (CSV vs Parquet)
- [ ] Progress tracking and reporting
- [ ] Error handling for missing files/permissions

## Phase 3: Database Integration

### DuckDB Support
- [ ] Local DuckDB database creation
- [ ] Schema generation from CUR data
- [ ] Data ingestion pipeline
- [ ] Row counting and monitoring

### BigQuery Support (Future)
- [ ] BigQuery client setup
- [ ] Dataset and table management
- [ ] batch ingestion strategy

## Phase 4: Azure Support (Future)

### CLI Extension
- [ ] Add azure subcommand structure
- [ ] Azure-specific argument parsing
- [ ] Configuration schema updates

### Azure Billing Integration
- [ ] Azure billing API discovery
- [ ] Manifest equivalent logic
- [ ] Data processing pipeline

## Phase 5: Advanced Features (Future)

### Configuration Management
- [ ] Environment variable precedence
- [ ] Multiple cloud vendor configs
- [ ] Profile management

### Monitoring & Observability
- [ ] Execution state tracking
- [ ] Pipeline run history
- [ ] Error reporting and recovery

### Performance & Scale
- [ ] Parallel processing
- [ ] Incremental updates
- [ ] Large dataset optimization

---

## Development Principles

- **TDD**: Write tests first, implement to make them pass
- **Small Steps**: Each change should be minimal and testable
- **Modular**: Keep vendor-specific logic separate
- **Idiomatic**: Follow Python and uv best practices