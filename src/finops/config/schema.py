from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class AWSConfig(BaseModel):
    bucket: str = Field(..., description="S3 bucket containing CUR files")
    export_name: str = Field(..., description="Name of the CUR export")
    prefix: str = Field(..., description="S3 prefix/path to CUR files")
    cur_version: str = Field("v1", description="CUR version v1|v2")
    access_key_id: Optional[str] = Field(None, description="AWS access key ID")
    secret_access_key: Optional[str] = Field(None, description="AWS secret access key")
    region: str = Field("us-east-1", description="AWS region")
    start_date: Optional[str] = Field(None, description="Start date YYYY-MM for import")
    end_date: Optional[str] = Field(None, description="End date YYYY-MM for import")
    dataset_name: str = Field("aws_billing", description="Dataset name for analytics")


class DuckDBConfig(BaseModel):
    database_path: str = Field("./data/finops.duckdb", description="Path to DuckDB database file")


class StateConfig(BaseModel):
    database_path: str = Field("./data/pipeline_state.db", description="Path to pipeline state database file")


class DatabaseConfig(BaseModel):
    backend: str = Field("duckdb", description="Database backend type")
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)


class FinopsConfig(BaseModel):
    model_config = ConfigDict(
        env_prefix="OFS_",
        env_nested_delimiter="_"
    )

    aws: AWSConfig
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    state: StateConfig = Field(default_factory=StateConfig)