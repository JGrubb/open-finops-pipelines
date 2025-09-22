"""Main CLI entry point for finops."""

import argparse
import sys
from finops.cli.aws import setup_aws_parser
from finops.config import load_config


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

    # Global configuration arguments
    parser.add_argument(
        "--config", "-c",
        help="Path to config.toml file (default: ./config.toml)",
        default="./config.toml"
    )

    # Add subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands", required=True)

    # AWS subcommand
    setup_aws_parser(subparsers)

    args = parser.parse_args()

    # Handle the command
    if hasattr(args, 'func'):
        try:
            # Load configuration with CLI argument precedence
            cli_args = vars(args)
            config = load_config(config_path=args.config, cli_args=cli_args)
            args.func(config, args)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()