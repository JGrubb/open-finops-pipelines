"""Azure billing data extraction handler."""
import toml
from pathlib import Path


def extract_billing(config_path: Path, args):
    """
    Extract billing CSV files from Azure Blob Storage.

    Downloads billing CSV files from discovered manifests to a staging directory.
    Only processes manifests in 'discovered' or 'failed' state matching the
    specified date range.

    Args:
        config_path: Path to configuration file
        args: Command line arguments
    """
    print("[STUB] Azure extract-billing - Implementation pending")
    print(f"Config: {config_path}")
    print(f"Start Date: {args.start_date}")
    print(f"End Date: {args.end_date}")

    if config_path.exists():
        config = toml.load(config_path)
        staging_dir = args.staging_dir or config.get("data_dir", "./data")
        print(f"Staging Directory: {staging_dir}")

    # TODO: Implement billing extraction logic
    # 1. Load pipeline state database
    # 2. Query for manifests in date range with state = 'discovered' or 'failed'
    # 3. For each manifest:
    #    a. Update state to 'downloading'
    #    b. Download all CSV blobs listed in manifest
    #    c. Verify downloads (file size, checksums if available)
    #    d. Update state to 'staged' on success, 'failed' on error
    # 4. Report extraction results
