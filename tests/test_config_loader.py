import os
import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest

from finops.config.loader import load_config
from finops.config.schema import FinopsConfig


def test_load_config_from_dict():
    """Test loading config from dictionary (CLI args)."""
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export",
        "prefix": "test-prefix"
    }

    config = load_config(cli_args=cli_args)

    assert config.aws.bucket == "test-bucket"
    assert config.aws.export_name == "test-export"
    assert config.database.backend == "duckdb"


def test_load_config_from_toml_file():
    """Test loading config from TOML file."""
    toml_content = """
[aws]
bucket = "toml-bucket"
export_name = "toml-export"
prefix = "cur-data"
region = "us-west-2"

[database]
backend = "duckdb"
[database.duckdb]
database_path = "./custom.duckdb"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write(toml_content)
        toml_path = f.name

    try:
        config = load_config(config_path=toml_path)

        assert config.aws.bucket == "toml-bucket"
        assert config.aws.export_name == "toml-export"
        assert config.aws.prefix == "cur-data"
        assert config.aws.region == "us-west-2"
        assert config.database.backend == "duckdb"
        assert config.database.duckdb.database_path == "./custom.duckdb"
    finally:
        os.unlink(toml_path)


def test_load_config_precedence_cli_over_toml():
    """Test that CLI arguments override TOML file values."""
    toml_content = """
[aws]
bucket = "toml-bucket"
export_name = "toml-export"
region = "us-west-2"
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write(toml_content)
        toml_path = f.name

    try:
        cli_args = {
            "bucket": "cli-bucket",  # Should override TOML
            "prefix": "cli-prefix"   # Should add to config
        }

        config = load_config(config_path=toml_path, cli_args=cli_args)

        assert config.aws.bucket == "cli-bucket"  # CLI override
        assert config.aws.export_name == "toml-export"  # From TOML
        assert config.aws.prefix == "cli-prefix"  # From CLI
        assert config.aws.region == "us-west-2"  # From TOML
    finally:
        os.unlink(toml_path)


def test_load_config_env_variables():
    """Test loading config from environment variables."""
    env_vars = {
        "OFS_AWS_BUCKET": "env-bucket",
        "OFS_AWS_EXPORT_NAME": "env-export",
        "OFS_AWS_PREFIX": "env-prefix",
        "OFS_AWS_REGION": "eu-west-1"
    }

    with patch.dict(os.environ, env_vars):
        config = load_config()

        assert config.aws.bucket == "env-bucket"
        assert config.aws.export_name == "env-export"
        assert config.aws.region == "eu-west-1"


def test_load_config_precedence_cli_over_env():
    """Test that CLI arguments override environment variables."""
    env_vars = {
        "OFS_AWS_BUCKET": "env-bucket",
        "OFS_AWS_EXPORT_NAME": "env-export",
        "OFS_AWS_PREFIX": "env-prefix"
    }

    with patch.dict(os.environ, env_vars):
        cli_args = {
            "bucket": "cli-bucket"  # Should override env var
        }

        config = load_config(cli_args=cli_args)

        assert config.aws.bucket == "cli-bucket"  # CLI override
        assert config.aws.export_name == "env-export"  # From env


def test_load_config_nonexistent_file():
    """Test loading config when file doesn't exist but required fields provided via CLI."""
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export",
        "prefix": "test-prefix"
    }

    config = load_config(config_path="/nonexistent/config.toml", cli_args=cli_args)

    # Should use defaults for optional fields
    assert config.aws.bucket == "test-bucket"
    assert config.aws.export_name == "test-export"
    assert config.database.backend == "duckdb"
    assert config.database.duckdb.database_path == "./data/finops.duckdb"


def test_load_config_cli_args_none_values_ignored():
    """Test that None values in CLI args are ignored."""
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export",
        "prefix": "test-prefix",  # Required field
        "region": None   # Should be ignored
    }

    config = load_config(cli_args=cli_args)

    assert config.aws.bucket == "test-bucket"
    assert config.aws.export_name == "test-export"
    assert config.aws.prefix == "test-prefix"  # Required field
    assert config.aws.region == "us-east-1"  # Default value