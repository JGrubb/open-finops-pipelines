"""Tests for AWS CLI commands."""

import subprocess


def test_aws_subcommand_help():
    """Test that aws subcommand shows help output."""
    result = subprocess.run(
        ["finops", "aws", "--help"],
        capture_output=True,
        text=True
    )

    assert result.returncode == 0
    assert "aws" in result.stdout.lower()
    assert "usage:" in result.stdout.lower()