"""AWS CLI commands."""

import argparse


def setup_aws_parser(subparsers):
    """Set up AWS subcommand parser."""
    aws_parser = subparsers.add_parser(
        "aws",
        help="AWS billing data operations"
    )

    # AWS subcommands
    aws_subparsers = aws_parser.add_subparsers(dest="aws_command", help="AWS commands", required=True)

    # import-billing command
    import_parser = aws_subparsers.add_parser(
        "import-billing",
        help="Import AWS CUR billing data"
    )
    _add_aws_common_args(import_parser)
    import_parser.add_argument(
        "--reset", "-r",
        action="store_true",
        help="Drop existing tables before import"
    )
    import_parser.set_defaults(func=import_billing_command)

    # list-manifests command
    list_parser = aws_subparsers.add_parser(
        "list-manifests",
        help="List available CUR manifest files"
    )
    _add_aws_common_args(list_parser)
    list_parser.set_defaults(func=list_manifests_command)

    # show-state command
    state_parser = aws_subparsers.add_parser(
        "show-state",
        help="Show previous pipeline executions and their state"
    )
    _add_aws_common_args(state_parser)
    state_parser.set_defaults(func=show_state_command)

    return aws_parser


def _add_aws_common_args(parser):
    """Add common AWS arguments to a parser."""
    # Required arguments (can also come from config/env)
    parser.add_argument(
        "--bucket", "-b",
        help="S3 bucket containing CUR files"
    )
    parser.add_argument(
        "--export-name", "-n",
        dest="export_name",
        help="Name of the CUR export"
    )

    # Optional arguments
    parser.add_argument(
        "--prefix", "-p",
        help="S3 prefix/path to CUR files (default: \"\")"
    )
    parser.add_argument(
        "--cur-version", "-v",
        dest="cur_version",
        choices=["v1", "v2"],
        help="CUR version v1|v2 (default: v1)"
    )
    parser.add_argument(
        "--export-format", "-f",
        dest="export_format",
        choices=["csv", "parquet"],
        help="File format csv|parquet (default: auto-detect)"
    )
    parser.add_argument(
        "--start-date", "-s",
        dest="start_date",
        help="Start date YYYY-MM for import (default: all available)"
    )
    parser.add_argument(
        "--end-date", "-e",
        dest="end_date",
        help="End date YYYY-MM for import (default: all available)"
    )


def import_billing_command(config, args):
    """Import AWS CUR billing data."""
    print(f"Importing billing data from bucket: {config.aws.bucket}")
    print(f"Export name: {config.aws.export_name}")
    print(f"Reset tables: {getattr(args, 'reset', False)}")
    # TODO: Implement actual import logic


def list_manifests_command(config, args):
    """List available CUR manifest files."""
    print(f"Listing manifests from bucket: {config.aws.bucket}")
    print(f"Export name: {config.aws.export_name}")
    # TODO: Implement actual manifest listing logic


def show_state_command(config, args):
    """Show previous pipeline executions and their state."""
    print(f"Showing state for bucket: {config.aws.bucket}")
    print(f"Export name: {config.aws.export_name}")
    # TODO: Implement actual state showing logic