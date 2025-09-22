"""Tests for config CLI command."""

import json
import tempfile
import os
from unittest.mock import patch
from io import StringIO
import pytest

from finops.cli.main import config_command
from finops.config import load_config


class Args:
    """Simple args class for testing."""
    def __init__(self, format="json"):
        self.format = format


def test_config_command_json_format():
    """Test config command with JSON output."""
    # Create a test config
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export"
    }
    config = load_config(cli_args=cli_args)

    # Capture output
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        args = Args(format="json")
        config_command(config, args)

    # Verify JSON output
    output = mock_stdout.getvalue()
    parsed_json = json.loads(output)

    assert parsed_json["aws"]["bucket"] == "test-bucket"
    assert parsed_json["aws"]["export_name"] == "test-export"
    assert parsed_json["database"]["backend"] == "duckdb"


def test_config_command_toml_format():
    """Test config command with TOML output."""
    # Create a test config
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export",
        "region": "us-west-2"
    }
    config = load_config(cli_args=cli_args)

    # Capture output
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        args = Args(format="toml")
        config_command(config, args)

    # Verify TOML output contains expected sections
    output = mock_stdout.getvalue()
    assert "[aws]" in output
    assert 'bucket = "test-bucket"' in output
    assert 'export_name = "test-export"' in output
    assert 'region = "us-west-2"' in output
    assert "[database]" in output
    assert 'backend = "duckdb"' in output
    assert "[database.duckdb]" in output


def test_config_command_from_toml_file():
    """Test config command when config is loaded from TOML file."""
    toml_content = """
[aws]
bucket = "toml-bucket"
export_name = "toml-export"
prefix = "cur-data"

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

        # Test JSON output
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            args = Args(format="json")
            config_command(config, args)

        output = mock_stdout.getvalue()
        parsed_json = json.loads(output)

        assert parsed_json["aws"]["bucket"] == "toml-bucket"
        assert parsed_json["aws"]["export_name"] == "toml-export"
        assert parsed_json["aws"]["prefix"] == "cur-data"
        assert parsed_json["database"]["duckdb"]["database_path"] == "./custom.duckdb"

    finally:
        os.unlink(toml_path)


def test_config_command_with_env_variables():
    """Test config command with environment variables."""
    env_vars = {
        "OFS_AWS_BUCKET": "env-bucket",
        "OFS_AWS_EXPORT_NAME": "env-export",
        "OFS_AWS_REGION": "eu-west-1"
    }

    with patch.dict(os.environ, env_vars):
        config = load_config()

        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            args = Args(format="json")
            config_command(config, args)

        output = mock_stdout.getvalue()
        parsed_json = json.loads(output)

        assert parsed_json["aws"]["bucket"] == "env-bucket"
        assert parsed_json["aws"]["export_name"] == "env-export"
        assert parsed_json["aws"]["region"] == "eu-west-1"


def test_config_command_yaml_format_missing_dependency():
    """Test config command with YAML format when PyYAML is not installed."""
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export"
    }
    config = load_config(cli_args=cli_args)

    # Mock yaml import to raise ImportError
    with patch('builtins.__import__', side_effect=lambda name, *args: exec('raise ImportError()') if name == 'yaml' else __import__(name, *args)):
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            with pytest.raises(SystemExit) as exc_info:
                args = Args(format="yaml")
                config_command(config, args)

            assert exc_info.value.code == 1
            error_output = mock_stderr.getvalue()
            assert "PyYAML not installed" in error_output