import argparse
import sys
from pathlib import Path

from finops import __version__
from finops.commands.aws import setup_aws_parser
from finops.commands.azure import setup_azure_parser


def config_command(config_path, args):
    """Display current configuration."""
    print(f"Configuration file: {config_path}")

    if config_path.exists():
        print(f"Status: Found")
        with open(config_path, 'r') as f:
            content = f.read().strip()
            if content:
                print("Contents:")
                print(content)
            else:
                print("Contents: (empty)")
    else:
        print("Status: Not found")
        print("Create a config.toml file with your settings.")


def main():
    parser = argparse.ArgumentParser(
        prog="finops",
        description="Open source FinOps data pipelines for cloud billing analysis"
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )

    parser.add_argument(
        "--config", "-c",
        metavar="CONFIG",
        default="./config.toml",
        help="Path to config.toml file (default: ./config.toml)"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        help="Available commands",
        required=True
    )

    # Config command
    config_parser = subparsers.add_parser(
        "config",
        help="Display current configuration"
    )
    config_parser.set_defaults(func=config_command)

    # AWS subcommand
    setup_aws_parser(subparsers)

    # Azure subcommand
    setup_azure_parser(subparsers)

    args = parser.parse_args()

    # Handle the command
    if hasattr(args, 'func'):
        config_path = Path(args.config)
        args.func(config_path, args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()