"""Main CLI entry point for finops."""

import argparse
import sys
import json
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

    # Config command
    config_parser = subparsers.add_parser(
        "config",
        help="Display current configuration"
    )
    config_parser.add_argument(
        "--format", "-f",
        choices=["json", "toml", "yaml"],
        default="json",
        help="Output format (default: json)"
    )
    config_parser.set_defaults(func=config_command)

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


def config_command(config, args):
    """Display current configuration."""
    output_format = args.format

    if output_format == "json":
        # Convert pydantic model to dict, then to pretty JSON
        config_dict = config.model_dump()
        print(json.dumps(config_dict, indent=2))
    elif output_format == "toml":
        # Convert to TOML format
        config_dict = config.model_dump()
        print(_dict_to_toml(config_dict))
    elif output_format == "yaml":
        try:
            import yaml
            config_dict = config.model_dump()
            print(yaml.dump(config_dict, default_flow_style=False))
        except ImportError:
            print("Error: PyYAML not installed. Use 'pip install pyyaml' to enable YAML output.", file=sys.stderr)
            sys.exit(1)


def _dict_to_toml(data, indent=0):
    """Convert dictionary to TOML format string."""
    lines = []
    tables = {}

    # Process simple values first
    for key, value in data.items():
        if isinstance(value, dict):
            tables[key] = value
        else:
            if isinstance(value, str):
                lines.append(f"{key} = \"{value}\"")
            else:
                lines.append(f"{key} = {json.dumps(value)}")

    # Process tables
    for table_name, table_data in tables.items():
        if lines:  # Add blank line before tables if there are simple values
            lines.append("")
        lines.append(f"[{table_name}]")

        # Handle nested tables
        simple_values = {}
        nested_tables = {}

        for key, value in table_data.items():
            if isinstance(value, dict):
                nested_tables[key] = value
            else:
                simple_values[key] = value

        # Add simple values for this table
        for key, value in simple_values.items():
            if isinstance(value, str):
                lines.append(f"{key} = \"{value}\"")
            else:
                lines.append(f"{key} = {json.dumps(value)}")

        # Add nested tables
        for nested_name, nested_data in nested_tables.items():
            lines.append(f"[{table_name}.{nested_name}]")
            for key, value in nested_data.items():
                if isinstance(value, str):
                    lines.append(f"{key} = \"{value}\"")
                else:
                    lines.append(f"{key} = {json.dumps(value)}")

    return "\n".join(lines)


if __name__ == "__main__":
    main()