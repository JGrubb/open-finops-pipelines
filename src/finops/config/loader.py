import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from .schema import FinopsConfig


def load_config(
    config_path: Optional[str] = None,
    cli_args: Optional[Dict[str, Any]] = None
) -> FinopsConfig:
    """Load configuration with precedence: CLI args > env vars > config.toml > defaults.

    Args:
        config_path: Path to config.toml file (default: ./config.toml)
        cli_args: Dictionary of CLI arguments to override config

    Returns:
        FinopsConfig: Validated configuration object
    """
    # Start with empty config data
    config_data = {}

    # 1. Load from config.toml (lowest precedence)
    if config_path is None:
        config_path = "./config.toml"

    config_file = Path(config_path)
    if config_file.exists():
        with open(config_file, "rb") as f:
            config_data = tomllib.load(f)

    # 2. Override with environment variables
    env_data = _load_from_env()
    if env_data:
        config_data = _deep_merge(config_data, env_data)

    # 3. Override with CLI arguments (highest precedence)
    if cli_args:
        config_data = _merge_cli_args(config_data, cli_args)

    # Create and validate configuration
    return FinopsConfig(**config_data)


def _load_from_env() -> Dict[str, Any]:
    """Load configuration from environment variables."""
    config_data = {}
    aws_data = {}

    # Map environment variables to config structure
    env_mapping = {
        'OFS_AWS_BUCKET': ('bucket', aws_data),
        'OFS_AWS_EXPORT_NAME': ('export_name', aws_data),
        'OFS_AWS_PREFIX': ('prefix', aws_data),
        'OFS_AWS_CUR_VERSION': ('cur_version', aws_data),
        'OFS_AWS_ACCESS_KEY_ID': ('access_key_id', aws_data),
        'OFS_AWS_SECRET_ACCESS_KEY': ('secret_access_key', aws_data),
        'OFS_AWS_REGION': ('region', aws_data),
        'OFS_AWS_START_DATE': ('start_date', aws_data),
        'OFS_AWS_END_DATE': ('end_date', aws_data),
        'OFS_AWS_DATASET_NAME': ('dataset_name', aws_data),
    }

    for env_var, (config_key, target_dict) in env_mapping.items():
        value = os.environ.get(env_var)
        if value is not None:
            target_dict[config_key] = value

    if aws_data:
        config_data['aws'] = aws_data

    return config_data


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _merge_cli_args(config_data: Dict[str, Any], cli_args: Dict[str, Any]) -> Dict[str, Any]:
    """Merge CLI arguments into configuration data."""
    # Create a copy to avoid modifying the original
    merged = config_data.copy()

    # Handle AWS-specific arguments
    aws_args = {}
    for key, value in cli_args.items():
        if value is None:
            continue

        if key in ['bucket', 'export_name', 'prefix', 'cur_version',
                   'access_key_id', 'secret_access_key', 'region',
                   'start_date', 'end_date', 'dataset_name']:
            aws_args[key] = value

    if aws_args:
        if 'aws' not in merged:
            merged['aws'] = {}
        merged['aws'].update(aws_args)

    return merged