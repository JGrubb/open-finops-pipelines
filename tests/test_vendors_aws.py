"""Tests for AWS vendor functionality."""

import json
import pytest
from unittest.mock import Mock, patch
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from finops.config.schema import AWSConfig
from finops.vendors.aws.client import AWSClient
from finops.vendors.aws.manifest import ManifestDiscovery
from finops.vendors.aws.types import Manifest, ManifestFile, CURVersion


class TestAWSClient:
    """Test AWS client functionality."""

    def test_aws_client_init(self):
        """Test AWS client initialization."""
        config = AWSConfig(
            bucket="test-bucket",
            export_name="test-export",
            prefix="test-prefix"
        )
        client = AWSClient(config)
        assert client.config == config
        assert client._s3_client is None

    @mock_aws
    def test_aws_client_with_explicit_credentials(self):
        """Test AWS client with explicit credentials."""
        config = AWSConfig(
            bucket="test-bucket",
            export_name="test-export",
            prefix="test-prefix",
            access_key_id="test-key",
            secret_access_key="test-secret",
            region="us-east-1"
        )
        client = AWSClient(config)
        s3_client = client.s3
        assert s3_client is not None
        assert client._s3_client is s3_client  # Test caching

    @mock_aws
    def test_aws_client_without_credentials(self):
        """Test AWS client without explicit credentials (uses default chain)."""
        config = AWSConfig(
            bucket="test-bucket",
            export_name="test-export",
            prefix="test-prefix",
            region="us-west-2"
        )
        client = AWSClient(config)
        s3_client = client.s3
        assert s3_client is not None

    def test_aws_client_no_credentials_error(self):
        """Test AWS client raises error when no credentials available."""
        config = AWSConfig(
            bucket="test-bucket",
            export_name="test-export",
            prefix="test-prefix"
        )

        with patch('boto3.client') as mock_boto:
            mock_boto.side_effect = NoCredentialsError()
            client = AWSClient(config)

            with pytest.raises(RuntimeError, match="AWS credentials not found"):
                _ = client.s3

    @mock_aws
    def test_test_connection_success(self):
        """Test successful connection test."""
        # Create mock S3 bucket
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')

        config = AWSConfig(
            bucket="test-bucket",
            export_name="test-export",
            prefix="test-prefix"
        )
        client = AWSClient(config)

        assert client.test_connection() is True

    @mock_aws
    def test_test_connection_no_such_bucket(self):
        """Test connection test with non-existent bucket."""
        config = AWSConfig(
            bucket="non-existent-bucket",
            export_name="test-export",
            prefix="test-prefix"
        )
        client = AWSClient(config)

        with pytest.raises(RuntimeError, match="does not exist"):
            client.test_connection()


class TestManifestTypes:
    """Test AWS manifest data types."""

    def test_manifest_file_creation(self):
        """Test ManifestFile creation."""
        file = ManifestFile(key="test-file.csv", size=1000, checksum="abc123")
        assert file.key == "test-file.csv"
        assert file.size == 1000
        assert file.checksum == "abc123"

    def test_manifest_creation(self):
        """Test Manifest creation."""
        files = [ManifestFile(key="file1.csv", size=1000)]
        manifest = Manifest(
            assembly_id="12345",
            billing_period="20240101-20240131",
            bucket="test-bucket",
            report_name="test-report",
            report_keys=["file1.csv"],
            files=files,
            cur_version=CURVersion.V1
        )
        assert manifest.assembly_id == "12345"
        assert manifest.cur_version == CURVersion.V1
        assert len(manifest.files) == 1

    def test_manifest_billing_year_month_v1(self):
        """Test billing year/month extraction for CUR v1."""
        manifest = Manifest(
            assembly_id="12345",
            billing_period="20240101-20240131",
            bucket="test-bucket",
            report_name="test-report",
            report_keys=[],
            files=[],
            cur_version=CURVersion.V1
        )
        assert manifest.billing_year_month == "2024-01"

    def test_manifest_billing_year_month_v2(self):
        """Test billing year/month extraction for CUR v2."""
        manifest = Manifest(
            assembly_id="12345",
            billing_period="2024-01",
            bucket="test-bucket",
            report_name="test-report",
            report_keys=[],
            files=[],
            cur_version=CURVersion.V2
        )
        assert manifest.billing_year_month == "2024-01"


class TestManifestDiscovery:
    """Test manifest discovery functionality."""

    @mock_aws
    def setup_method(self, method):
        """Set up test environment."""
        # Create mock S3 bucket and client
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.s3.create_bucket(Bucket='test-bucket')

        self.config = AWSConfig(
            bucket="test-bucket",
            export_name="test-export",
            prefix="test-prefix"
        )
        self.aws_client = AWSClient(self.config)
        self.discovery = ManifestDiscovery(self.aws_client)

    def _create_v1_manifest(self, billing_period="20240101-20240131"):
        """Create a CUR v1 manifest for testing."""
        manifest_content = {
            "assemblyId": "12345-v1",
            "billingPeriod": billing_period,
            "reportKeys": [
                "test-prefix/test-export/20240101-20240131/test-export-00001.csv.gz",
                "test-prefix/test-export/20240101-20240131/test-export-00002.csv.gz"
            ],
            "compression": "GZIP",
            "format": "textORcsv"
        }

        key = f"test-prefix/test-export/{billing_period}/test-export-Manifest.json"
        self.s3.put_object(
            Bucket='test-bucket',
            Key=key,
            Body=json.dumps(manifest_content)
        )
        return key, manifest_content

    def _create_v2_manifest(self, billing_period="2024-01"):
        """Create a CUR v2 manifest for testing."""
        manifest_content = {
            "assemblyId": "67890-v2",
            "billingPeriod": billing_period,
            "reportKeys": [
                {
                    "key": f"test-prefix/test-export/metadata/BILLING_PERIOD={billing_period}/part-00000.parquet",
                    "size": 1000000,
                    "checksum": "abc123"
                },
                {
                    "key": f"test-prefix/test-export/metadata/BILLING_PERIOD={billing_period}/part-00001.parquet",
                    "size": 2000000,
                    "checksum": "def456"
                }
            ],
            "compression": "GZIP",
            "format": "Parquet"
        }

        key = f"test-prefix/test-export/metadata/BILLING_PERIOD={billing_period}/test-export-Manifest.json"
        self.s3.put_object(
            Bucket='test-bucket',
            Key=key,
            Body=json.dumps(manifest_content)
        )
        return key, manifest_content

    @mock_aws
    def test_discover_manifests_empty(self):
        """Test manifest discovery with no manifests."""
        self.setup_method(None)
        manifests = self.discovery.discover_manifests()
        assert len(manifests) == 0

    @mock_aws
    def test_discover_v1_manifest(self):
        """Test discovery of CUR v1 manifest."""
        self.setup_method(None)
        key, content = self._create_v1_manifest()

        manifests = self.discovery.discover_manifests()
        assert len(manifests) == 1

        manifest = manifests[0]
        assert manifest.assembly_id == "12345-v1"
        assert manifest.cur_version == CURVersion.V1
        assert manifest.billing_period == "20240101-20240131"
        assert manifest.billing_year_month == "2024-01"
        assert len(manifest.files) == 2

    @mock_aws
    def test_discover_v2_manifest(self):
        """Test discovery of CUR v2 manifest."""
        self.setup_method(None)
        key, content = self._create_v2_manifest()

        manifests = self.discovery.discover_manifests()
        assert len(manifests) == 1

        manifest = manifests[0]
        assert manifest.assembly_id == "67890-v2"
        assert manifest.cur_version == CURVersion.V2
        assert manifest.billing_period == "2024-01"
        assert manifest.billing_year_month == "2024-01"
        assert len(manifest.files) == 2
        assert manifest.files[0].size == 1000000

    @mock_aws
    def test_discover_manifests_date_filtering(self):
        """Test manifest discovery with date filtering."""
        self.setup_method(None)

        # Create manifests for different months
        self._create_v1_manifest("20240101-20240131")  # 2024-01
        self._create_v1_manifest("20240201-20240229")  # 2024-02
        self._create_v2_manifest("2024-03")            # 2024-03

        # Test start date filtering
        manifests = self.discovery.discover_manifests(start_date="2024-02")
        assert len(manifests) == 2  # Feb and Mar

        # Test end date filtering
        manifests = self.discovery.discover_manifests(end_date="2024-02")
        assert len(manifests) == 2  # Jan and Feb

        # Test range filtering
        manifests = self.discovery.discover_manifests(start_date="2024-02", end_date="2024-02")
        assert len(manifests) == 1  # Only Feb

    @mock_aws
    def test_cur_version_detection(self):
        """Test CUR version detection from key patterns."""
        self.setup_method(None)

        # Test v1 detection
        v1_key = "test-prefix/test-export/20240101-20240131/test-export-Manifest.json"
        assert self.discovery._detect_cur_version(v1_key) == CURVersion.V1

        # Test v2 detection
        v2_key = "test-prefix/test-export/metadata/BILLING_PERIOD=2024-01/test-export-Manifest.json"
        assert self.discovery._detect_cur_version(v2_key) == CURVersion.V2

        # Test fallback to v1
        unknown_key = "some/other/path/test-export-Manifest.json"
        assert self.discovery._detect_cur_version(unknown_key) == CURVersion.V1

    @mock_aws
    def test_billing_period_extraction(self):
        """Test billing period extraction from keys and content."""
        self.setup_method(None)

        # Test v1 extraction
        v1_key = "test-prefix/test-export/20240101-20240131/test-export-Manifest.json"
        period = self.discovery._extract_billing_period(v1_key, {}, CURVersion.V1)
        assert period == "20240101-20240131"

        # Test v2 extraction
        v2_key = "test-prefix/test-export/metadata/BILLING_PERIOD=2024-01/test-export-Manifest.json"
        period = self.discovery._extract_billing_period(v2_key, {}, CURVersion.V2)
        assert period == "2024-01"

    @mock_aws
    def test_manifest_parsing_error_handling(self):
        """Test manifest parsing with malformed data."""
        self.setup_method(None)

        # Create manifest with invalid JSON
        self.s3.put_object(
            Bucket='test-bucket',
            Key='test-prefix/test-export/20240101-20240131/test-export-Manifest.json',
            Body='invalid json'
        )

        # Should handle the error gracefully and return empty list
        manifests = self.discovery.discover_manifests()
        assert len(manifests) == 0


class TestCLIIntegration:
    """Test CLI integration with AWS vendor modules."""

    def test_import_statements(self):
        """Test that imports work correctly."""
        # This test ensures our module structure is correct
        from finops.vendors.aws.client import AWSClient
        from finops.vendors.aws.manifest import ManifestDiscovery
        from finops.vendors.aws.types import Manifest, ManifestFile, CURVersion

        assert AWSClient is not None
        assert ManifestDiscovery is not None
        assert Manifest is not None
        assert ManifestFile is not None
        assert CURVersion is not None