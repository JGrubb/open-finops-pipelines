"""Main CLI entry point for finops."""

import argparse
import sys
import json
from pydantic import ValidationError
from finops.cli.aws import setup_aws_parser
from finops.config import load_config, FinopsConfig


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
        except ValidationError as e:
            print("Configuration Error: Missing required fields", file=sys.stderr)
            print("", file=sys.stderr)

            missing_fields = []
            for error in e.errors():
                if error['type'] == 'missing':
                    field_path = '.'.join(str(loc) for loc in error['loc'])
                    missing_fields.append(field_path)

            if missing_fields:
                print("Required fields:", file=sys.stderr)
                for field in missing_fields:
                    if field == 'aws':
                        # When the entire AWS section is missing, show all required AWS fields
                        _show_required_aws_fields()
                    elif field.startswith('aws.'):
                        field_name = field.replace('aws.', '')
                        _show_field_help('aws', field_name)
                    else:
                        print(f"  - {field}", file=sys.stderr)

                print("", file=sys.stderr)
                print("Examples:", file=sys.stderr)
                print("  finops aws import-billing --bucket my-bucket --export-name my-export", file=sys.stderr)
                print("  OFS_AWS_BUCKET=my-bucket OFS_AWS_EXPORT_NAME=my-export finops config", file=sys.stderr)
                print("  echo '[aws]\\nbucket = \"my-bucket\"\\nexport_name = \"my-export\"' > config.toml", file=sys.stderr)

            sys.exit(1)
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


def _show_required_aws_fields():
    """Show all required AWS fields by introspecting the schema."""
    from finops.config.schema import AWSConfig

    # Get required fields from the AWSConfig model
    required_fields = []
    for field_name, field_info in AWSConfig.model_fields.items():
        if field_info.is_required():
            required_fields.append(field_name)

    for field_name in required_fields:
        _show_field_help('aws', field_name)


def _show_field_help(section, field_name):
    """Show help for a specific field with CLI flag, env var, and config.toml options."""
    if section == 'aws':
        env_var = f"OFS_AWS_{field_name.upper()}"
        cli_flag = f"--{field_name.replace('_', '-')}"
        print(f"  - {section}.{field_name}: Provide via {cli_flag}, {env_var}, or config.toml", file=sys.stderr)
    else:
        print(f"  - {section}.{field_name}", file=sys.stderr)


if __name__ == "__main__":
    main()