"""Tests for config CLI command output formatting."""

import json
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
    """Test config command with JSON output format."""
    # Create a test config
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export",
        "prefix": "test-prefix"
    }
    config = load_config(cli_args=cli_args)

    # Capture output
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        args = Args(format="json")
        config_command(config, args)

    # Verify JSON output is valid and contains expected structure
    output = mock_stdout.getvalue()
    parsed_json = json.loads(output)

    assert "aws" in parsed_json
    assert "database" in parsed_json
    assert parsed_json["aws"]["bucket"] == "test-bucket"
    assert parsed_json["aws"]["export_name"] == "test-export"


def test_config_command_toml_format():
    """Test config command with TOML output format."""
    # Create a test config
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export",
        "prefix": "test-prefix",
        "region": "us-west-2"
    }
    config = load_config(cli_args=cli_args)

    # Capture output
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        args = Args(format="toml")
        config_command(config, args)

    # Verify TOML output contains expected sections and format
    output = mock_stdout.getvalue()
    assert "[aws]" in output
    assert "[database]" in output
    assert "[database.duckdb]" in output
    assert 'bucket = "test-bucket"' in output
    assert 'export_name = "test-export"' in output


def test_config_command_yaml_format_missing_dependency():
    """Test config command with YAML format when PyYAML is not installed."""
    cli_args = {
        "bucket": "test-bucket",
        "export_name": "test-export",
        "prefix": "test-prefix"
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