"""Azure billing data remote loading handler."""
import toml
from pathlib import Path


def load_billing_remote(config_path: Path, args):
    """
    Load Azure billing data to remote warehouse (BigQuery, etc.).

    Transfers data from local DuckDB or Parquet files to the configured
    remote data warehouse.

    Args:
        config_path: Path to configuration file
        args: Command line arguments
    """
    print("[STUB] Azure load-billing-remote - Implementation pending")
    print(f"Config: {config_path}")
    print(f"Start Date: {args.start_date}")
    print(f"End Date: {args.end_date}")
    print(f"Overwrite: {args.overwrite}")

    if config_path.exists():
        config = toml.load(config_path)
        azure_config = config.get("azure", {})
        dest_config = azure_config.get("destination", {})
        db_config = config.get("database", {})

        backend = dest_config.get("backend", db_config.get("destination", "bigquery"))
        dataset = dest_config.get("dataset")
        table = dest_config.get("table")

        print(f"Backend: {backend}")
        print(f"Dataset: {dataset}")
        print(f"Table: {table}")

    # TODO: Implement remote loading logic
    # 1. Load pipeline state database
    # 2. Query for manifests in date range with state = 'loaded'
    # 3. Connect to remote warehouse (BigQuery, etc.)
    # 4. Transfer data (via Parquet or direct load)
    # 5. Handle schema evolution in remote warehouse
    # 6. Update pipeline state
    # 7. Report loading results
