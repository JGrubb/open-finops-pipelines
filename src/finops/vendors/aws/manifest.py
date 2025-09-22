"""CUR manifest discovery and parsing logic."""

import json
import re
from typing import List, Optional
from botocore.exceptions import ClientError

from .client import AWSClient
from .types import Manifest, ManifestFile, CURVersion
from finops.config.schema import AWSConfig


class ManifestDiscovery:
    """Discovers and parses AWS CUR manifest files."""

    def __init__(self, aws_client: AWSClient):
        """Initialize manifest discovery with AWS client.

        Args:
            aws_client: Configured AWS client for S3 operations
        """
        self.client = aws_client
        self.config = aws_client.config

    def discover_manifests(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Manifest]:
        """Discover CUR manifest files in S3 bucket.

        Args:
            start_date: Start date filter in YYYY-MM format
            end_date: End date filter in YYYY-MM format

        Returns:
            List of parsed manifest objects

        Raises:
            RuntimeError: If S3 operations fail
        """
        try:
            manifest_keys = self._find_manifest_keys()
            manifests = []

            for key in manifest_keys:
                try:
                    manifest = self._parse_manifest(key)
                    if self._should_include_manifest(manifest, start_date, end_date):
                        manifests.append(manifest)
                except Exception as e:
                    print(f"Warning: Failed to parse manifest {key}: {e}")
                    continue

            return manifests

        except ClientError as e:
            raise RuntimeError(f"Failed to discover manifests: {e}") from e

    def _find_manifest_keys(self) -> List[str]:
        """Find all manifest file keys in the S3 bucket."""
        manifest_keys = []
        paginator = self.client.s3.get_paginator('list_objects_v2')

        # Search for manifest files with pattern *-Manifest.json
        prefix = f"{self.config.prefix}/{self.config.export_name}" if self.config.prefix else self.config.export_name

        page_iterator = paginator.paginate(
            Bucket=self.config.bucket,
            Prefix=prefix
        )

        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('-Manifest.json'):
                        manifest_keys.append(key)

        return manifest_keys

    def _parse_manifest(self, key: str) -> Manifest:
        """Parse a single manifest file from S3.

        Args:
            key: S3 key of the manifest file

        Returns:
            Parsed manifest object
        """
        # Download manifest content
        response = self.client.s3.get_object(Bucket=self.config.bucket, Key=key)
        manifest_content = json.loads(response['Body'].read().decode('utf-8'))

        # Detect CUR version from key pattern
        cur_version = self._detect_cur_version(key)

        # Parse manifest files
        files = []
        for file_info in manifest_content.get('reportKeys', []):
            if isinstance(file_info, str):
                # Simple string format
                files.append(ManifestFile(key=file_info, size=0))
            else:
                # Object format with metadata
                files.append(ManifestFile(
                    key=file_info['key'],
                    size=file_info.get('size', 0),
                    checksum=file_info.get('checksum')
                ))

        # Extract billing period (format depends on CUR version)
        billing_period = self._extract_billing_period(key, manifest_content, cur_version)

        return Manifest(
            assembly_id=manifest_content.get('assemblyId', ''),
            billing_period=billing_period,
            bucket=self.config.bucket,
            report_name=self.config.export_name,
            report_keys=manifest_content.get('reportKeys', []),
            files=files,
            cur_version=cur_version,
            compression=manifest_content.get('compression'),
            format=manifest_content.get('format'),
            schema_version=manifest_content.get('schemaVersion')
        )

    def _detect_cur_version(self, key: str) -> CURVersion:
        """Detect CUR version from manifest key pattern.

        Args:
            key: S3 key of the manifest file

        Returns:
            Detected CUR version
        """
        # CUR v2 pattern: .../metadata/BILLING_PERIOD=YYYY-MM/...-Manifest.json
        if '/metadata/BILLING_PERIOD=' in key:
            return CURVersion.V2

        # CUR v1 pattern: .../YYYYMMDD-YYYYMMDD/...-Manifest.json
        v1_pattern = r'/\d{8}-\d{8}/'
        if re.search(v1_pattern, key):
            return CURVersion.V1

        # Default to v1 if pattern is unclear
        return CURVersion.V1

    def _extract_billing_period(self, key: str, manifest_content: dict, cur_version: CURVersion) -> str:
        """Extract billing period from manifest key or content.

        Args:
            key: S3 key of the manifest file
            manifest_content: Parsed manifest JSON content
            cur_version: Detected CUR version

        Returns:
            Billing period string
        """
        if cur_version == CURVersion.V2:
            # Extract from BILLING_PERIOD= in key
            match = re.search(r'BILLING_PERIOD=([^/]+)', key)
            if match:
                return match.group(1)

        # For V1, extract from date range in key
        match = re.search(r'/(\d{8}-\d{8})/', key)
        if match:
            return match.group(1)

        # Fallback: try to extract from manifest content
        return manifest_content.get('billingPeriod', '')

    def _should_include_manifest(
        self,
        manifest: Manifest,
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> bool:
        """Check if manifest should be included based on date filters.

        Args:
            manifest: Parsed manifest
            start_date: Start date filter in YYYY-MM format
            end_date: End date filter in YYYY-MM format

        Returns:
            True if manifest should be included
        """
        if not start_date and not end_date:
            return True

        billing_ym = manifest.billing_year_month

        if start_date and billing_ym < start_date:
            return False

        if end_date and billing_ym > end_date:
            return False

        return True