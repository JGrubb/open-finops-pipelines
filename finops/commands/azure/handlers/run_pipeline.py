"""Azure complete pipeline orchestrator."""
import toml
from pathlib import Path


def run_pipeline(config_path: Path, args):
    """
    Run the complete Azure billing pipeline.

    Orchestrates the full pipeline flow:
    1. Discover manifests
    2. Extract billing data
    3. Load to local database
    4. Export to Parquet
    5. Load to remote warehouse

    Args:
        config_path: Path to configuration file
        args: Command line arguments
    """
    print("[STUB] Azure run-pipeline - Implementation pending")
    print(f"Config: {config_path}")
    print(f"Start Date: {args.start_date}")
    print(f"End Date: {args.end_date}")
    print(f"Dry Run: {args.dry_run}")

    # TODO: Implement pipeline orchestration
    # 1. Validate configuration
    # 2. Run discover_manifests
    # 3. Run extract_billing
    # 4. Run load_billing_local
    # 5. Run export_parquet
    # 6. Run load_billing_remote
    # 7. Report overall pipeline results
    #
    # If dry_run=True, show what would be processed without executing
