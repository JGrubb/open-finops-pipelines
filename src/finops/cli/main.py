"""Main CLI entry point for finops."""

import argparse
from finops.cli.aws import setup_aws_parser


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="finops",
        description="Open source FinOps data pipelines for cloud billing analysis"
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )

    # Add subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # AWS subcommand
    setup_aws_parser(subparsers)

    args = parser.parse_args()


if __name__ == "__main__":
    main()