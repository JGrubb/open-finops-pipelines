import toml
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class AWSConfig:
    """AWS configuration settings."""
    bucket: str
    prefix: str
    export_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    cur_version: str = "v2"  # Default to v2
    region: str = "us-east-1"  # Default region


@dataclass
class FinopsConfig:
    """Main configuration for finops CLI."""
    aws: AWSConfig
    staging_dir: str = "./staging"
    state_db: str = "./finops_state.db"
    duckdb_path: str = "./data/finops.duckdb"
    parquet_dir: str = "./data/parquet"

    @classmethod
    def from_file(cls, config_path: Path) -> "FinopsConfig":
        """Load configuration from TOML file."""
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        config_data = toml.load(config_path)

        # Extract AWS config
        aws_data = config_data.get("aws", {})
        if not aws_data:
            raise ValueError("Missing [aws] section in configuration")

        required_aws_fields = ["bucket", "prefix", "export_name", "aws_access_key_id", "aws_secret_access_key"]
        for field in required_aws_fields:
            if field not in aws_data:
                raise ValueError(f"Missing required AWS configuration: {field}")

        aws_config = AWSConfig(
            bucket=aws_data["bucket"],
            prefix=aws_data["prefix"],
            export_name=aws_data["export_name"],
            aws_access_key_id=aws_data["aws_access_key_id"],
            aws_secret_access_key=aws_data["aws_secret_access_key"],
            cur_version=aws_data.get("cur_version", "v2"),
            region=aws_data.get("region", "us-east-1")
        )

        # Extract global config
        staging_dir = config_data.get("staging_dir", "./staging")
        state_db = config_data.get("state_db", "./finops_state.db")
        duckdb_path = config_data.get("duckdb_path", "./data/finops.duckdb")
        parquet_dir = config_data.get("parquet_dir", "./data/parquet")

        return cls(
            aws=aws_config,
            staging_dir=staging_dir,
            state_db=state_db,
            duckdb_path=duckdb_path,
            parquet_dir=parquet_dir
        )

    @classmethod
    def from_cli_args(cls, config_path: Path, cli_args: dict) -> "FinopsConfig":
        """Load configuration with CLI argument precedence."""
        # Start with file config
        try:
            config = cls.from_file(config_path)
        except (FileNotFoundError, ValueError):
            # If no config file, create minimal config from CLI args
            aws_config = AWSConfig(
                bucket=cli_args.get("bucket", ""),
                prefix=cli_args.get("prefix", ""),
                export_name=cli_args.get("export_name", ""),
                aws_access_key_id=cli_args.get("aws_access_key_id", ""),
                aws_secret_access_key=cli_args.get("aws_secret_access_key", ""),
                cur_version=cli_args.get("cur_version", "v2"),
                region=cli_args.get("region", "us-east-1")
            )
            config = cls(aws=aws_config)

        # Override with CLI arguments if provided
        if cli_args.get("bucket"):
            config.aws.bucket = cli_args["bucket"]
        if cli_args.get("prefix"):
            config.aws.prefix = cli_args["prefix"]
        if cli_args.get("export_name"):
            config.aws.export_name = cli_args["export_name"]
        if cli_args.get("cur_version"):
            config.aws.cur_version = cli_args["cur_version"]
        if cli_args.get("region"):
            config.aws.region = cli_args["region"]

        return config

    def validate(self):
        """Validate configuration and raise errors for missing required fields."""
        errors = []

        if not self.aws.bucket:
            errors.append("AWS bucket is required")
        if not self.aws.prefix:
            errors.append("AWS prefix is required")
        if not self.aws.export_name:
            errors.append("AWS export_name is required")
        if not self.aws.aws_access_key_id:
            errors.append("AWS aws_access_key_id is required")
        if not self.aws.aws_secret_access_key:
            errors.append("AWS aws_secret_access_key is required")
        if self.aws.cur_version not in ["v1", "v2"]:
            errors.append("AWS cur_version must be 'v1' or 'v2'")

        if errors:
            raise ValueError("Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors))