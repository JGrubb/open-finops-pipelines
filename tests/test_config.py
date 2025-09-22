import pytest
from pydantic import ValidationError
from finops.config.schema import FinopsConfig, AWSConfig, DatabaseConfig


def test_aws_config_required_fields():
    """Test that AWS config requires bucket and export_name."""
    with pytest.raises(ValidationError) as exc_info:
        AWSConfig()

    errors = exc_info.value.errors()
    field_names = {error['loc'][0] for error in errors}
    assert 'bucket' in field_names
    assert 'export_name' in field_names


def test_aws_config_with_required_fields():
    """Test AWS config with minimum required fields."""
    config = AWSConfig(bucket="test-bucket", export_name="test-export")

    assert config.bucket == "test-bucket"
    assert config.export_name == "test-export"
    assert config.prefix == ""
    assert config.cur_version == "v1"
    assert config.region == "us-east-1"
    assert config.dataset_name == "aws_billing"


def test_aws_config_all_fields():
    """Test AWS config with all fields set."""
    config = AWSConfig(
        bucket="test-bucket",
        export_name="test-export",
        prefix="cur-exports",
        cur_version="v2",
        access_key_id="AKIATEST",
        secret_access_key="secret",
        region="us-west-2",
        start_date="2024-01",
        end_date="2024-12",
        dataset_name="custom_billing"
    )

    assert config.bucket == "test-bucket"
    assert config.export_name == "test-export"
    assert config.prefix == "cur-exports"
    assert config.cur_version == "v2"
    assert config.access_key_id == "AKIATEST"
    assert config.secret_access_key == "secret"
    assert config.region == "us-west-2"
    assert config.start_date == "2024-01"
    assert config.end_date == "2024-12"
    assert config.dataset_name == "custom_billing"


def test_database_config_defaults():
    """Test database config defaults."""
    config = DatabaseConfig()

    assert config.backend == "duckdb"
    assert config.duckdb.database_path == "./data/finops.duckdb"


def test_finops_config_minimal():
    """Test complete FinOps config with minimal AWS settings."""
    config = FinopsConfig(aws={"bucket": "test-bucket", "export_name": "test-export"})

    assert config.aws.bucket == "test-bucket"
    assert config.aws.export_name == "test-export"
    assert config.database.backend == "duckdb"
    assert config.database.duckdb.database_path == "./data/finops.duckdb"


def test_finops_config_validation_error():
    """Test that FinOps config validation fails without required AWS fields."""
    with pytest.raises(ValidationError):
        FinopsConfig(aws={})