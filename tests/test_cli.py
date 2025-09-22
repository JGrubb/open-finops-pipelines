"""Tests for CLI entry point."""

import subprocess
import sys
import tomllib
from pathlib import Path


def test_cli_help():
    """Test that CLI shows help output."""
    result = subprocess.run(
        ["finops", "--help"],
        capture_output=True,
        text=True
    )

    assert result.returncode == 0
    assert "finops" in result.stdout
    assert "Open source FinOps data pipelines" in result.stdout


def test_cli_version():
    """Test that CLI shows version."""
    # Read version from pyproject.toml
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)
    expected_version = pyproject["project"]["version"]

    result = subprocess.run(
        ["finops", "--version"],
        capture_output=True,
        text=True
    )

    assert result.returncode == 0
    assert f"finops {expected_version}" in result.stdout