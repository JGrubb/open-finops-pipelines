"""Extract billing command handler."""
import sys
from finops.commands.aws.utils import (
    handle_command_errors,
    load_and_validate_config,
    validate_date_range,
    discover_and_filter_manifests,
)
from finops.services.billing_extractor import BillingExtractorService
from finops.services.state_checker import StateChecker


@handle_command_errors
def extract_billing(config_path, args):
    """Extract billing files from S3."""
    print("Extracting billing files from S3...")

    # Load configuration
    config = load_and_validate_config(config_path)

    # Validate date arguments
    start_date = args.start_date
    end_date = args.end_date
    staging_dir = args.staging_dir or config.staging_dir

    if not validate_date_range(start_date, end_date):
        sys.exit(1)

    # Show what we're doing
    if start_date and end_date:
        print(f"Date range: {start_date} to {end_date}")
    elif start_date:
        print(f"From: {start_date} onwards")
    elif end_date:
        print(f"Up to: {end_date}")
    else:
        print("All manifests")

    print(f"Staging directory: {staging_dir}")
    print()

    # Discover and filter manifests
    print("Discovering manifests...")
    state_checker = StateChecker(config)
    manifests = discover_and_filter_manifests(config, state_checker, start_date, end_date)

    if not manifests:
        print("No manifests to extract")
        return

    print(f"Found {len(manifests)} manifest(s) to extract")
    print()

    # Extract files
    extractor = BillingExtractorService(config.aws)
    stats = extractor.extract_billing_files(manifests, staging_dir)

    # Display results
    print(f"\nExtraction complete:")
    print(f"  Manifests processed: {stats['manifests_processed']}")
    print(f"  Files downloaded: {stats['files_downloaded']}")
    if stats['errors'] > 0:
        print(f"  Errors: {stats['errors']}")

    if stats['manifests_processed'] > 0:
        print(f"\nNext step: Use 'finops aws load-billing-local' to load staged files into DuckDB")
