"""AWS CLI commands."""

import argparse
from finops.vendors.aws.client import AWSClient
from finops.vendors.aws.manifest import ManifestDiscovery


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
    try:
        # Create AWS client
        aws_client = AWSClient(config.aws)

        # Test connection first
        print(f"Connecting to S3 bucket: {config.aws.bucket}")
        aws_client.test_connection()
        print("✓ Connection successful")

        # Discover manifests
        discovery = ManifestDiscovery(aws_client)
        print(f"Discovering manifests for export: {config.aws.export_name}")

        start_date = getattr(args, 'start_date', None) or config.aws.start_date
        end_date = getattr(args, 'end_date', None) or config.aws.end_date

        manifests = discovery.discover_manifests(start_date=start_date, end_date=end_date)

        if not manifests:
            print("No manifests found matching the criteria")
            return

        print(f"\nFound {len(manifests)} manifest(s):")
        print("-" * 80)

        for manifest in manifests:
            print(f"Billing Period: {manifest.billing_period}")
            print(f"Assembly ID: {manifest.assembly_id}")
            print(f"CUR Version: {manifest.cur_version.value}")
            print(f"Files: {len(manifest.files)}")
            if manifest.files:
                total_size = sum(f.size for f in manifest.files)
                print(f"Total Size: {total_size:,} bytes")
            print(f"Format: {manifest.format or 'Unknown'}")
            print("-" * 80)

    except Exception as e:
        print(f"Error: {e}")
        return 1


def show_state_command(config, args):
    """Show previous pipeline executions and their state."""
    print(f"Showing state for bucket: {config.aws.bucket}")
    print(f"Export name: {config.aws.export_name}")
    # TODO: Implement actual state showing logic