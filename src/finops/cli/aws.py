"""AWS CLI commands."""

import argparse


def setup_aws_parser(subparsers):
    """Set up AWS subcommand parser."""
    aws_parser = subparsers.add_parser(
        "aws",
        help="AWS billing data operations"
    )

    return aws_parser