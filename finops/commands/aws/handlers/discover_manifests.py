"""Discover manifests command handler."""
from finops.commands.aws.utils import (
    handle_command_errors,
    load_and_validate_config,
)
from finops.services.manifest_discovery import ManifestDiscoveryService
from finops.services.state_checker import StateChecker


@handle_command_errors
def discover_manifests(config_path, args):
    """Diagnostic tool: Show what manifests are available vs already loaded."""
    print("Diagnostic: Checking manifest status...")

    # Build CLI args for config override
    cli_args = {k: v for k, v in {
        "bucket": args.bucket,
        "prefix": args.prefix,
        "export_name": args.export_name,
        "cur_version": args.cur_version,
        "region": args.region
    }.items() if v is not None}

    # Load and validate config
    config = load_and_validate_config(config_path, cli_args)

    print(f"S3 Location: s3://{config.aws.bucket}/{config.aws.prefix}")
    print(f"Export: {config.aws.export_name} (CUR {config.aws.cur_version})\n")

    # Check state and discover manifests
    state_checker = StateChecker(config)
    discovery_service = ManifestDiscoveryService(config.aws, state_checker)
    manifests = discovery_service.discover_manifests()

    # Show summary
    print(discovery_service.get_manifest_summary(manifests))

    if manifests:
        print(f"\nTo process these manifests: finops aws extract-billing")
