"""Utility functions for AWS commands."""
import re
import sys
from pathlib import Path
from functools import wraps
from finops.config import FinopsConfig
from finops.services.manifest_discovery import ManifestDiscoveryService
from finops.services.state_checker import StateChecker


def load_and_validate_config(config_path, cli_args=None):
    """Load and validate configuration with optional CLI overrides."""
    if cli_args is None:
        cli_args = {}
    config = FinopsConfig.from_cli_args(config_path, cli_args)
    config.validate_aws()
    return config


def validate_date_range(start_date, end_date):
    """Validate date format (YYYY-MM). Returns True if valid, prints error and returns False if invalid."""
    if start_date and not re.match(r'^\d{4}-\d{2}$', start_date):
        print(f"Error: start-date must be in YYYY-MM format (e.g., 2024-01)")
        return False
    if end_date and not re.match(r'^\d{4}-\d{2}$', end_date):
        print(f"Error: end-date must be in YYYY-MM format (e.g., 2024-12)")
        return False
    return True


def discover_and_filter_manifests(config, state_checker, start_date=None, end_date=None):
    """Discover manifests from S3 and filter by date range."""
    discovery_service = ManifestDiscoveryService(config.aws, state_checker)
    manifests = discovery_service.discover_manifests()

    # Filter by date range if specified
    if start_date or end_date:
        filtered_manifests = []
        for manifest in manifests:
            billing_period = manifest.billing_period
            if start_date and billing_period < start_date:
                continue
            if end_date and billing_period > end_date:
                continue
            filtered_manifests.append(manifest)
        manifests = filtered_manifests

    return manifests


def ensure_duckdb_path(config):
    """Ensure DuckDB parent directory exists."""
    if not config.duckdb:
        raise ValueError("DuckDB configuration is missing")
    duckdb_path = Path(config.duckdb_path)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb_path


def handle_command_errors(func):
    """Decorator to handle command errors consistently."""
    @wraps(func)
    def wrapper(config_path, args):
        try:
            return func(config_path, args)
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
    return wrapper
