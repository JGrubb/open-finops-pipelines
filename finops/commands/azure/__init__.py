from finops.commands.azure.handlers import (
    discover_manifests,
    extract_billing,
    load_billing_local,
    export_parquet,
    load_billing_remote,
    run_pipeline,
)


def setup_azure_parser(subparsers):
    """Set up the Azure subcommand parser."""
    azure_parser = subparsers.add_parser(
        "azure",
        help="Azure billing data operations"
    )

    azure_subparsers = azure_parser.add_subparsers(
        dest="azure_command",
        help="Azure commands",
        required=True
    )

    # discover-manifests command
    discover_parser = azure_subparsers.add_parser(
        "discover-manifests",
        help="Discover Azure Cost Management export manifests"
    )
    discover_parser.add_argument(
        "--storage-account",
        help="Azure storage account name"
    )
    discover_parser.add_argument(
        "--container",
        help="Container name where billing exports are stored"
    )
    discover_parser.add_argument(
        "--export-name",
        help="Export name"
    )
    discover_parser.set_defaults(func=discover_manifests)

    # extract-billing command
    extract_billing_parser = azure_subparsers.add_parser(
        "extract-billing",
        help="Extract billing files from Azure Blob Storage to staging directory"
    )
    extract_billing_parser.add_argument(
        "--start-date",
        help="Start date for billing period (YYYY-MM format, e.g., 2024-01)"
    )
    extract_billing_parser.add_argument(
        "--end-date",
        help="End date for billing period (YYYY-MM format, e.g., 2024-12)"
    )
    extract_billing_parser.add_argument(
        "--staging-dir",
        help="Directory to download CSV files to (default: ./staging)"
    )
    extract_billing_parser.set_defaults(func=extract_billing)

    # load-billing-local command
    load_local_parser = azure_subparsers.add_parser(
        "load-billing-local",
        help="Load staged billing files into local database"
    )
    load_local_parser.add_argument(
        "--start-date",
        help="Start date for billing period (YYYY-MM format, e.g., 2024-01)"
    )
    load_local_parser.add_argument(
        "--end-date",
        help="End date for billing period (YYYY-MM format, e.g., 2024-12)"
    )
    load_local_parser.set_defaults(func=load_billing_local)

    # export-parquet command
    export_parser = azure_subparsers.add_parser(
        "export-parquet",
        help="Export each month to Parquet format"
    )
    export_parser.add_argument(
        "--output-dir",
        help="Output directory for Parquet files (overrides config)"
    )
    export_parser.add_argument(
        "--start-date",
        help="Start date for export range (YYYY-MM format)"
    )
    export_parser.add_argument(
        "--end-date",
        help="End date for export range (YYYY-MM format)"
    )
    export_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Parquet files"
    )
    export_parser.add_argument(
        "--compression",
        default="snappy",
        choices=["snappy", "gzip", "lz4", "zstd"],
        help="Parquet compression type (default: snappy)"
    )
    export_parser.set_defaults(func=export_parquet)

    # load-billing-remote command
    load_remote_parser = azure_subparsers.add_parser(
        "load-billing-remote",
        help="Load billing data to remote warehouse"
    )
    load_remote_parser.add_argument(
        "--start-date",
        help="Start date for loading (YYYY-MM format)",
        metavar="YYYY-MM"
    )
    load_remote_parser.add_argument(
        "--end-date",
        help="End date for loading (YYYY-MM format)",
        metavar="YYYY-MM"
    )
    load_remote_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing data in remote warehouse"
    )
    load_remote_parser.set_defaults(func=load_billing_remote)

    # run-pipeline command - meta-pipeline that runs the complete flow
    pipeline_parser = azure_subparsers.add_parser(
        "run-pipeline",
        help="Run the complete pipeline: discover → extract → load → export → load-remote"
    )
    pipeline_parser.add_argument(
        "--start-date",
        help="Start date for billing period (YYYY-MM format, e.g., 2024-01)"
    )
    pipeline_parser.add_argument(
        "--end-date",
        help="End date for billing period (YYYY-MM format, e.g., 2024-12)"
    )
    pipeline_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without executing"
    )
    pipeline_parser.set_defaults(func=run_pipeline)
