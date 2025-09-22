"""AWS CLI commands."""

import argparse
from finops.vendors.aws.client import AWSClient
from finops.vendors.aws.manifest import ManifestDiscovery
from finops.state.manager import StateManager


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
    list_parser.add_argument(
        "--include-processed",
        action="store_true",
        help="Include already processed manifests in the list"
    )
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
    """List available CUR manifest files and update state database."""
    try:
        # Create AWS client and state manager
        aws_client = AWSClient(config.aws)
        state_manager = StateManager(config.state.database_path)

        # Test connection first
        print(f"Connecting to S3 bucket: {config.aws.bucket}")
        aws_client.test_connection()
        print("✓ Connection successful")

        # Discover manifests from S3
        discovery = ManifestDiscovery(aws_client)
        print(f"Discovering manifests for export: {config.aws.export_name}")

        start_date = getattr(args, 'start_date', None) or config.aws.start_date
        end_date = getattr(args, 'end_date', None) or config.aws.end_date

        manifests = discovery.discover_manifests(start_date=start_date, end_date=end_date)

        if not manifests:
            print("No manifests found matching the criteria")
            return

        print(f"\nFound {len(manifests)} manifest(s) in S3:")

        # Daily workflow: Check state and record new manifests
        include_processed = getattr(args, 'include_processed', False)
        newly_discovered = []
        already_processed = []

        for manifest in manifests:
            # Check if we've seen this manifest before (in any state)
            if state_manager.is_already_seen("aws", manifest.assembly_id):
                already_processed.append(manifest)
                if not include_processed:
                    continue  # Skip already seen manifests unless --include-processed
            else:
                # Record new manifest as discovered
                state_manager.record_discovered(
                    vendor="aws",
                    billing_version_id=manifest.assembly_id,
                    billing_month=manifest.billing_year_month,
                    export_name=config.aws.export_name
                )
                newly_discovered.append(manifest)

        # Display results
        if include_processed:
            manifests_to_show = manifests
            print(f"Showing all {len(manifests_to_show)} manifest(s):")
        else:
            manifests_to_show = newly_discovered
            print(f"Showing {len(manifests_to_show)} new manifest(s) (use --include-processed to see all):")

        if newly_discovered:
            print(f"✓ Recorded {len(newly_discovered)} new manifest(s) as 'discovered' in state database")

        if already_processed and not include_processed:
            print(f"⏭ Skipped {len(already_processed)} already processed manifest(s)")

        if not manifests_to_show:
            if include_processed:
                print("No manifests found")
            else:
                print("No new manifests found (all already processed)")
            return

        print("\n" + "=" * 100)

        for manifest in manifests_to_show:
            print(f"Billing Period: {manifest.billing_period}")
            print(f"Assembly ID: {manifest.assembly_id}")
            print(f"CUR Version: {manifest.cur_version.value}")
            print(f"Files: {len(manifest.files)}")
            if manifest.files:
                total_size = sum(f.size for f in manifest.files)
                print(f"Total Size: {total_size:,} bytes")
            print(f"Format: {manifest.format or 'Unknown'}")

            # Show processing status
            if state_manager.is_already_processed("aws", manifest.assembly_id):
                print("Status: ✓ Already processed")
            else:
                print("Status: ○ Newly discovered (ready for processing)")

            print("-" * 80)

        # Summary
        print(f"\nSummary:")
        print(f"  New manifests discovered: {len(newly_discovered)}")
        print(f"  Already processed: {len(already_processed)}")
        print(f"  Total in S3: {len(manifests)}")

        if newly_discovered:
            print(f"\nNext steps:")
            print(f"  • Run 'finops aws show-state' to see all pipeline state")
            print(f"  • Future: implement pipeline to process 'discovered' manifests")

    except Exception as e:
        print(f"Error: {e}")
        return 1


def show_state_command(config, args):
    """Show previous pipeline executions and their state."""
    try:
        print(f"Showing state for bucket: {config.aws.bucket}")
        print(f"Export name: {config.aws.export_name}")

        # Create state manager
        state_manager = StateManager(config.state.database_path)

        # Get date filters
        start_date = getattr(args, 'start_date', None) or config.aws.start_date
        end_date = getattr(args, 'end_date', None) or config.aws.end_date

        # Get all processed manifests for AWS
        all_records = state_manager.get_manifests_to_process("aws")

        # For show-state, we actually want ALL records, not just discovered ones
        # Let's add a method to get all records
        from finops.state.database import get_connection

        with get_connection(config.state.database_path) as conn:
            cursor = conn.cursor()

            # Apply filters
            where_conditions = ["vendor = ?"]
            params = ["aws"]

            if start_date:
                where_conditions.append("billing_month >= ?")
                params.append(start_date)
            if end_date:
                where_conditions.append("billing_month <= ?")
                params.append(end_date)

            # Filter by export name
            where_conditions.append("export_name = ?")
            params.append(config.aws.export_name)

            where_clause = " AND ".join(where_conditions)

            cursor.execute(f"""
                SELECT * FROM billing_state
                WHERE {where_clause}
                ORDER BY billing_month DESC, created_at DESC
            """, params)

            rows = cursor.fetchall()

        if not rows:
            print("\nNo pipeline state records found.")
            return

        print(f"\nFound {len(rows)} state record(s):")
        print("=" * 100)

        # Group by billing month for better display
        by_month = {}
        for row in rows:
            month = row['billing_month']
            if month not in by_month:
                by_month[month] = []
            by_month[month].append(row)

        for month in sorted(by_month.keys(), reverse=True):
            records = by_month[month]
            print(f"\nBilling Month: {month}")
            print("-" * 50)

            for row in records:
                status_icon = {
                    "discovered": "○",
                    "loading": "⏳",
                    "loaded": "✓",
                    "failed": "✗"
                }.get(row['state'], "?")

                current_marker = " (CURRENT)" if row['is_current'] else ""
                print(f"  {status_icon} Assembly ID: {row['billing_version_id']}{current_marker}")
                print(f"    State: {row['state'].upper()}")
                print(f"    Created: {row['created_at']}")
                print(f"    Updated: {row['updated_at']}")
                print()

        print("=" * 100)

        # Summary statistics
        state_counts = {}
        for row in rows:
            state = row['state']
            state_counts[state] = state_counts.get(state, 0) + 1

        print(f"\nSummary:")
        for state, count in sorted(state_counts.items()):
            icon = {"discovered": "○", "loading": "⏳", "loaded": "✓", "failed": "✗"}.get(state, "?")
            print(f"  {icon} {state.title()}: {count}")

    except Exception as e:
        print(f"Error: {e}")
        return 1