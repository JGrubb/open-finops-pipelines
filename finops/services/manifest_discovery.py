import re
import json
import boto3
from typing import List, Optional
from botocore.exceptions import ClientError, NoCredentialsError

from finops.models.manifest import CURManifest
from finops.config import AWSConfig
from finops.services.state_db import StateDB
from finops.services.state_checker import StateChecker


class ManifestDiscoveryService:
    """Service for discovering AWS CUR manifest files."""

    def __init__(self, aws_config: AWSConfig, state_db: StateDB, state_checker: Optional[StateChecker] = None):
        self.aws_config = aws_config
        self.state_db = state_db
        self.state_checker = state_checker
        self._s3_client = None

    @property
    def s3_client(self):
        """Lazy initialization of S3 client."""
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3",
                region_name=self.aws_config.region,
                aws_access_key_id=self.aws_config.aws_access_key_id,
                aws_secret_access_key=self.aws_config.aws_secret_access_key
            )
        return self._s3_client

    def discover_manifests(self) -> List[CURManifest]:
        """Discover CUR manifest files and persist them to state database."""
        version = self.aws_config.cur_version

        if version == "v1":
            manifests = self._discover_v1_manifests()
        elif version == "v2":
            manifests = self._discover_v2_manifests()
        else:
            raise ValueError(f"Unsupported CUR version: {version}")

        # Filter out already-loaded manifests using StateChecker
        if self.state_checker:
            loaded_execution_ids = self.state_checker.get_loaded_execution_ids("aws")
            print(f"Found {len(loaded_execution_ids)} billing periods already loaded in destination database")

            filtered_manifests = []
            for manifest in manifests:
                billing_period = manifest.billing_period
                execution_id = manifest.id

                if loaded_execution_ids.get(billing_period) == execution_id:
                    print(f"  Skipping {billing_period} (execution_id {execution_id} already loaded)")
                else:
                    filtered_manifests.append(manifest)

            print(f"Found {len(filtered_manifests)} new manifests to process")
            manifests = filtered_manifests

        # Persist discovered manifests to state database
        for manifest in manifests:
            self.state_db.save_manifest(manifest, state="discovered")

        return manifests

    def _discover_v1_manifests(self) -> List[CURManifest]:
        """Discover v1 CUR manifest files."""
        pattern = self._get_v1_pattern()
        return self._find_manifests_by_pattern(pattern, "v1")

    def _discover_v2_manifests(self) -> List[CURManifest]:
        """Discover v2 CUR manifest files."""
        pattern = self._get_v2_pattern()
        return self._find_manifests_by_pattern(pattern, "v2")

    def _get_v1_pattern(self) -> str:
        """Get regex pattern for v1 manifest files."""
        return rf"{self.aws_config.prefix}/{self.aws_config.export_name}/\d{{8}}-\d{{8}}/{self.aws_config.export_name}-Manifest\.json"

    def _get_v2_pattern(self) -> str:
        """Get regex pattern for v2 manifest files."""
        return rf"{self.aws_config.prefix}/{self.aws_config.export_name}/metadata/BILLING_PERIOD=\d{{4}}-\d{{2}}/{self.aws_config.export_name}-Manifest\.json"

    def _find_manifests_by_pattern(self, pattern: str, version: str) -> List[CURManifest]:
        """Find manifest files matching the given pattern."""
        try:
            # List objects with the base prefix
            base_prefix = f"{self.aws_config.prefix}/{self.aws_config.export_name}/"

            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.aws_config.bucket,
                Prefix=base_prefix
            )

            manifest_keys = []
            regex_pattern = re.compile(pattern)

            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        if regex_pattern.match(key):
                            manifest_keys.append(key)

            # Download and parse each manifest
            manifests = []
            for key in manifest_keys:
                try:
                    manifest = self._parse_manifest_file(key, version)
                    if manifest:
                        manifests.append(manifest)
                except Exception as e:
                    print(f"Warning: Failed to parse manifest {key}: {e}")
                    continue

            # Sort by billing period (newest first)
            manifests.sort(key=lambda m: m.get_billing_month_sort_key(), reverse=True)

            return manifests

        except NoCredentialsError:
            raise Exception("AWS credentials not found. Please configure your credentials.")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                raise Exception(f"S3 bucket '{self.aws_config.bucket}' does not exist or is not accessible")
            elif error_code == 'AccessDenied':
                raise Exception(f"Access denied to S3 bucket '{self.aws_config.bucket}'")
            else:
                raise Exception(f"AWS error: {e}")

    def _parse_manifest_file(self, s3_key: str, version: str) -> Optional[CURManifest]:
        """Download and parse a manifest file."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.aws_config.bucket,
                Key=s3_key
            )

            content = response["Body"].read().decode("utf-8")
            manifest_data = json.loads(content)

            return CURManifest.from_manifest_data(
                manifest_data=manifest_data,
                s3_key=s3_key,
                bucket=self.aws_config.bucket,
                version=version
            )

        except ClientError as e:
            print(f"Error downloading manifest {s3_key}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error parsing manifest JSON {s3_key}: {e}")
            return None

    def get_manifest_summary(self, manifests: List[CURManifest]) -> str:
        """Generate a summary of discovered manifests."""
        if not manifests:
            return "No manifests found"

        lines = [f"Found {len(manifests)} manifest(s):"]

        for manifest in manifests:
            file_count = len(manifest.files)
            lines.append(f"  • {manifest.billing_period} ({manifest.version}) - {file_count} files - {manifest.id}")

        return "\n".join(lines)