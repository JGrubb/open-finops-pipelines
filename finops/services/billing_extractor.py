import boto3
import json
from pathlib import Path
from typing import List, Dict
from botocore.exceptions import ClientError, NoCredentialsError

from finops.config import AWSConfig
from finops.services.state_db import StateDB


class BillingExtractorService:
    """Service for downloading billing CSV files from S3."""

    def __init__(self, aws_config: AWSConfig, state_db: StateDB):
        self.aws_config = aws_config
        self.state_db = state_db
        self.s3_client = self._create_s3_client()

    def get_staged_execution_ids(self, staging_dir: str, billing_period: str) -> List[str]:
        """Get list of execution_ids that are already in staging directory for a billing period."""
        staging_path = Path(staging_dir) / billing_period
        if not staging_path.exists():
            return []

        # List subdirectories (each is an execution_id)
        execution_ids = []
        for item in staging_path.iterdir():
            if item.is_dir():
                execution_ids.append(item.name)

        return execution_ids

    def clean_old_execution_ids(self, staging_dir: str, billing_period: str, keep_execution_id: str) -> int:
        """Remove old execution_id directories for a billing period, keeping only the specified one."""
        staging_path = Path(staging_dir) / billing_period
        if not staging_path.exists():
            return 0

        removed_count = 0
        for item in staging_path.iterdir():
            if item.is_dir() and item.name != keep_execution_id:
                print(f"  Removing old execution_id: {item.name}")
                import shutil
                shutil.rmtree(item)
                removed_count += 1

        return removed_count

    def _create_s3_client(self):
        """Create S3 client with AWS credentials."""
        try:
            return boto3.client(
                's3',
                aws_access_key_id=self.aws_config.aws_access_key_id,
                aws_secret_access_key=self.aws_config.aws_secret_access_key,
                region_name=self.aws_config.region or 'us-east-1'
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create S3 client: {e}")

    def extract_billing_files(
        self,
        start_date: str = None,
        end_date: str = None,
        staging_dir: str = "./staging"
    ) -> Dict[str, int]:
        """Extract billing CSV files for discovered manifests in date range."""

        # Get discovered manifests in date range
        manifests = self.state_db.get_discovered_manifests_by_date_range(start_date, end_date)

        if not manifests:
            return {"manifests_processed": 0, "files_downloaded": 0, "errors": 0}

        # Create staging directory
        staging_path = Path(staging_dir)
        staging_path.mkdir(parents=True, exist_ok=True)

        stats = {"manifests_processed": 0, "files_downloaded": 0, "errors": 0}

        for manifest in manifests:
            try:
                execution_id = manifest['manifest_id']
                billing_period = manifest['billing_period']

                # Check if already staged in filesystem
                staged_ids = self.get_staged_execution_ids(staging_dir, billing_period)
                if execution_id in staged_ids:
                    print(f"  Skipping {billing_period} ({execution_id}) - already in staging")
                    stats["manifests_processed"] += 1
                    continue

                # Update state to downloading
                self.state_db.update_manifest_state(manifest['manifest_id'], 'downloading')

                # Create subdirectory for billing period and execution_id
                period_dir = staging_path / billing_period / execution_id
                period_dir.mkdir(parents=True, exist_ok=True)

                # Parse CSV files from JSON
                csv_files = json.loads(manifest['csv_files'])

                downloaded_files = []
                for csv_file_key in csv_files:
                    try:
                        # Download file from S3 (keep original filename including .gz)
                        local_filename = Path(csv_file_key).name
                        local_path = period_dir / local_filename

                        print(f"  Downloading {csv_file_key} -> {local_path}")

                        self.s3_client.download_file(
                            manifest['s3_bucket'],
                            csv_file_key,
                            str(local_path)
                        )

                        downloaded_files.append(str(local_path))
                        stats["files_downloaded"] += 1

                    except Exception as file_error:
                        print(f"    Error downloading {csv_file_key}: {file_error}")
                        stats["errors"] += 1
                        continue

                # Update state to staged if all files downloaded successfully
                if len(downloaded_files) == len(csv_files):
                    self.state_db.update_manifest_state(manifest['manifest_id'], 'staged')
                    print(f"  ✓ All {len(csv_files)} files staged for {billing_period}")

                    # Clean up old execution_ids for this billing period
                    removed = self.clean_old_execution_ids(staging_dir, billing_period, execution_id)
                    if removed > 0:
                        print(f"  Cleaned up {removed} old execution_id(s) for {billing_period}")
                else:
                    error_msg = f"Only {len(downloaded_files)}/{len(csv_files)} files downloaded"
                    self.state_db.update_manifest_state(manifest['manifest_id'], 'failed', error_msg)
                    print(f"  ✗ Partial download for {manifest['billing_period']}: {error_msg}")
                    stats["errors"] += 1

                stats["manifests_processed"] += 1

            except Exception as manifest_error:
                error_msg = f"Error processing manifest: {manifest_error}"
                self.state_db.update_manifest_state(manifest['manifest_id'], 'failed', error_msg)
                print(f"  ✗ Failed {manifest['billing_period']}: {error_msg}")
                stats["errors"] += 1

        return stats