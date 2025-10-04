"""Azure billing data local loading handler."""
import toml
from pathlib import Path


def load_billing_local(config_path: Path, args):
    """
    Load staged Azure billing CSV files into local DuckDB database.

    Reads CSV files from staging directory and loads them into DuckDB,
    handling schema evolution and data type normalization.

    Args:
        config_path: Path to configuration file
        args: Command line arguments
    """
    print("[STUB] Azure load-billing-local - Implementation pending")
    print(f"Config: {config_path}")
    print(f"Start Date: {args.start_date}")
    print(f"End Date: {args.end_date}")

    if config_path.exists():
        config = toml.load(config_path)
        db_config = config.get("database", {})
        local_backend = db_config.get("local", "duckdb")
        print(f"Local Backend: {local_backend}")

    # TODO: Implement local loading logic
    # 1. Load pipeline state database
    # 2. Query for manifests in date range with state = 'staged'
    # 3. Initialize DuckDB connection
    # 4. For each manifest:
    #    a. Update state to 'loading'
    #    b. Read CSV schema and create/alter table as needed
    #    c. Load CSV data into DuckDB
    #    d. Handle schema evolution (new columns)
    #    e. Normalize column names and data types
    #    f. Update state to 'loaded' on success, 'failed' on error
    # 5. Report loading results
