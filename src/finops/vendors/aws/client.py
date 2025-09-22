"""AWS client setup and credential handling."""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Optional

from finops.config.schema import AWSConfig


class AWSClient:
    """AWS S3 client with credential handling and error management."""

    def __init__(self, config: AWSConfig):
        """Initialize AWS client from configuration.

        Args:
            config: AWS configuration containing credentials and region
        """
        self.config = config
        self._s3_client = None

    @property
    def s3(self):
        """Get or create S3 client with lazy initialization."""
        if self._s3_client is None:
            self._s3_client = self._create_s3_client()
        return self._s3_client

    def _create_s3_client(self):
        """Create S3 client with credential handling."""
        try:
            # If credentials are provided in config, use them explicitly
            if self.config.access_key_id and self.config.secret_access_key:
                return boto3.client(
                    's3',
                    aws_access_key_id=self.config.access_key_id,
                    aws_secret_access_key=self.config.secret_access_key,
                    region_name=self.config.region
                )
            else:
                # Fall back to default credential chain (env vars, IAM roles, etc.)
                return boto3.client('s3', region_name=self.config.region)
        except NoCredentialsError as e:
            raise RuntimeError(
                "AWS credentials not found. Provide them via config file, "
                "environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY), "
                "or IAM roles."
            ) from e

    def test_connection(self) -> bool:
        """Test AWS connection and bucket access.

        Returns:
            True if connection and bucket access successful

        Raises:
            RuntimeError: If connection fails or bucket is inaccessible
        """
        try:
            # Test basic S3 access by listing objects in the bucket (limit 1)
            response = self.s3.list_objects_v2(
                Bucket=self.config.bucket,
                MaxKeys=1
            )
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                raise RuntimeError(f"S3 bucket '{self.config.bucket}' does not exist") from e
            elif error_code == 'AccessDenied':
                raise RuntimeError(f"Access denied to S3 bucket '{self.config.bucket}'") from e
            else:
                raise RuntimeError(f"S3 error: {error_code} - {e.response['Error']['Message']}") from e