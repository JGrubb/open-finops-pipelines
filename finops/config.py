import toml
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class AWSConfig:
    """AWS configuration settings - combines source + destination for backward compatibility."""
    # Source settings
    bucket: str
    prefix: str
    export_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    cur_version: str = "v2"
    region: str = "us-east-1"

    # Destination settings
    destination_backend: str = "bigquery"
    destination_dataset: str = ""
    destination_table: str = ""


@dataclass
class AzureConfig:
    """Azure configuration settings - combines source + destination."""
    # Source settings
    storage_account: str
    container: str
    export_name: str
    tenant_id: str
    client_id: str
    client_secret: str

    # Destination settings
    destination_backend: str = "bigquery"
    destination_dataset: str = ""
    destination_table: str = ""


@dataclass
class BigQueryConfig:
    """BigQuery configuration settings."""
    project_id: str
    credentials_path: str
    dataset_id: str = ""  # Vendor-specific dataset
    table_id: str = ""    # Vendor-specific table


@dataclass
class DatabaseConfig:
    """Database backend configuration."""
    destination: str = "bigquery"  # Default destination backend
    local: str = "duckdb"  # Local operations backend
    duckdb: Optional['DuckDBConfig'] = None
    bigquery: Optional[BigQueryConfig] = None


@dataclass
class DuckDBConfig:
    """DuckDB configuration settings."""
    persistent: bool = False


@dataclass
class FinopsConfig:
    """Main configuration for finops CLI."""
    aws: Optional[AWSConfig] = None
    azure: Optional[AzureConfig] = None
    database: DatabaseConfig = None
    data_dir: str = "./data"

    @property
    def staging_dir(self) -> str:
        """Staging directory for CSV files."""
        return f"{self.data_dir}/staging"

    @property
    def parquet_dir(self) -> str:
        """Parquet export directory."""
        return f"{self.data_dir}/exports"

    @property
    def duckdb_path(self) -> str:
        """DuckDB database path - returns :memory: if not persistent."""
        if self.database.duckdb and self.database.duckdb.persistent:
            return f"{self.data_dir}/finops.duckdb"
        return ":memory:"

    @property
    def duckdb(self) -> Optional[DuckDBConfig]:
        """Convenience property to access DuckDB config."""
        return self.database.duckdb

    @property
    def bigquery(self) -> Optional[BigQueryConfig]:
        """Convenience property to access BigQuery config with AWS-specific dataset/table."""
        if not self.database.bigquery:
            return None

        # Return BigQuery config with vendor-specific dataset/table if AWS is configured
        if self.aws and self.aws.destination_backend == "bigquery":
            return BigQueryConfig(
                project_id=self.database.bigquery.project_id,
                credentials_path=self.database.bigquery.credentials_path,
                dataset_id=self.aws.destination_dataset,
                table_id=self.aws.destination_table
            )

        return self.database.bigquery

    def get_bigquery_config_for_vendor(self, vendor: str) -> Optional[BigQueryConfig]:
        """Get BigQuery config with vendor-specific dataset/table."""
        if not self.database.bigquery:
            return None

        if vendor == "aws" and self.aws:
            return BigQueryConfig(
                project_id=self.database.bigquery.project_id,
                credentials_path=self.database.bigquery.credentials_path,
                dataset_id=self.aws.destination_dataset,
                table_id=self.aws.destination_table
            )
        elif vendor == "azure" and self.azure:
            return BigQueryConfig(
                project_id=self.database.bigquery.project_id,
                credentials_path=self.database.bigquery.credentials_path,
                dataset_id=self.azure.destination_dataset,
                table_id=self.azure.destination_table
            )

        return self.database.bigquery

    @classmethod
    def from_file(cls, config_path: Path) -> "FinopsConfig":
        """Load configuration from TOML file with source/destination structure."""
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        config_data = toml.load(config_path)

        # Extract database config first (needed for inheritance)
        db_data = config_data.get("database", {})
        default_destination = db_data.get("destination", "bigquery")
        local_backend = db_data.get("local", "duckdb")

        duckdb_config = None
        bigquery_config = None

        # Load DuckDB config if present
        duckdb_data = db_data.get("duckdb", {})
        if duckdb_data or local_backend == "duckdb":
            duckdb_config = DuckDBConfig(
                persistent=duckdb_data.get("persistent", False)
            )

        # Load BigQuery config if present
        bq_data = db_data.get("bigquery", {})
        if bq_data:
            required_bq_fields = ["project_id", "credentials_path"]
            missing_fields = [f for f in required_bq_fields if f not in bq_data]

            if not missing_fields:
                bigquery_config = BigQueryConfig(
                    project_id=bq_data["project_id"],
                    credentials_path=bq_data["credentials_path"]
                )

        database_config = DatabaseConfig(
            destination=default_destination,
            local=local_backend,
            duckdb=duckdb_config,
            bigquery=bigquery_config
        )

        # Extract AWS config (source + destination)
        aws_config = None
        if "aws" in config_data:
            aws_source = config_data["aws"].get("source", {})
            aws_dest = config_data["aws"].get("destination", {})

            if aws_source:
                aws_config = AWSConfig(
                    # Source settings
                    bucket=aws_source.get("bucket", ""),
                    prefix=aws_source.get("prefix", ""),
                    export_name=aws_source.get("export_name", ""),
                    aws_access_key_id=aws_source.get("aws_access_key_id", ""),
                    aws_secret_access_key=aws_source.get("aws_secret_access_key", ""),
                    cur_version=aws_source.get("cur_version", "v2"),
                    region=aws_source.get("region", "us-east-1"),
                    # Destination settings (inherit from database.destination if not specified)
                    destination_backend=aws_dest.get("backend", default_destination),
                    destination_dataset=aws_dest.get("dataset", ""),
                    destination_table=aws_dest.get("table", "")
                )

        # Extract Azure config (source + destination)
        azure_config = None
        if "azure" in config_data:
            azure_source = config_data["azure"].get("source", {})
            azure_dest = config_data["azure"].get("destination", {})

            if azure_source:
                azure_config = AzureConfig(
                    # Source settings
                    storage_account=azure_source.get("storage_account", ""),
                    container=azure_source.get("container", ""),
                    export_name=azure_source.get("export_name", ""),
                    tenant_id=azure_source.get("tenant_id", ""),
                    client_id=azure_source.get("client_id", ""),
                    client_secret=azure_source.get("client_secret", ""),
                    # Destination settings (inherit from database.destination if not specified)
                    destination_backend=azure_dest.get("backend", default_destination),
                    destination_dataset=azure_dest.get("dataset", ""),
                    destination_table=azure_dest.get("table", "")
                )

        # Extract global config
        data_dir = config_data.get("data_dir", "./data")

        return cls(
            aws=aws_config,
            azure=azure_config,
            database=database_config,
            data_dir=data_dir
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
            # Default to DuckDB for CLI-only config
            database_config = DatabaseConfig(
                backend="duckdb",
                duckdb=DuckDBConfig()
            )
            config = cls(aws=aws_config, database=database_config)

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

    def validate_aws(self):
        """Validate AWS configuration and raise errors for missing required fields."""
        if not self.aws:
            raise ValueError("AWS configuration is not loaded")

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
            raise ValueError("AWS configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors))

    def validate_azure(self):
        """Validate Azure configuration and raise errors for missing required fields."""
        if not self.azure:
            raise ValueError("Azure configuration is not loaded")

        errors = []

        if not self.azure.storage_account:
            errors.append("Azure storage_account is required")
        if not self.azure.container:
            errors.append("Azure container is required")
        if not self.azure.export_name:
            errors.append("Azure export_name is required")
        if not self.azure.tenant_id:
            errors.append("Azure tenant_id is required")
        if not self.azure.client_id:
            errors.append("Azure client_id is required")
        if not self.azure.client_secret:
            errors.append("Azure client_secret is required")

        if errors:
            raise ValueError("Azure configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors))