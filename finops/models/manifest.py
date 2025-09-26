from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime


@dataclass
class CURManifest:
    """Represents a CUR manifest file with version-aware handling."""
    id: str  # assembly_id (v1) or execution_id (v2)
    version: str  # "v1" or "v2"
    billing_period: str  # "2024-01" format
    billing_period_start: str  # ISO date string
    billing_period_end: str  # ISO date string
    s3_key: str  # S3 path to manifest file
    bucket: str
    files: List[str]  # List of CSV file S3 keys
    columns: List[Dict[str, str]]  # Column definitions from manifest
    compression: str  # GZIP, etc.

    @classmethod
    def from_manifest_data(cls, manifest_data: dict, s3_key: str, bucket: str, version: str) -> "CURManifest":
        """Create CURManifest from parsed manifest JSON data."""
        if version == "v1":
            manifest_id = manifest_data.get("assemblyId")
            billing_period_start = manifest_data.get("billingPeriod", {}).get("start")
            billing_period_end = manifest_data.get("billingPeriod", {}).get("end")
        else:  # v2
            manifest_id = manifest_data.get("executionId")
            billing_period_start = manifest_data.get("billingPeriod", {}).get("start")
            billing_period_end = manifest_data.get("billingPeriod", {}).get("end")

        # Extract billing period in YYYY-MM format
        if billing_period_start:
            # Handle different date formats - extract YYYY-MM from start date
            if billing_period_start.startswith('20'):  # ISO format like "20250901T000000.000Z"
                year = billing_period_start[:4]
                month = billing_period_start[4:6]
                billing_period = f"{year}-{month}"
            else:  # Already in YYYY-MM-DD format
                billing_period = billing_period_start[:7]  # "2024-01-01" -> "2024-01"
        else:
            billing_period = "unknown"

        # Extract file list
        files = []
        for report_key in manifest_data.get("reportKeys", []):
            files.append(report_key)

        # Extract column definitions
        columns = manifest_data.get("columns", [])

        # Extract compression
        compression = manifest_data.get("compression", "GZIP")

        return cls(
            id=manifest_id,
            version=version,
            billing_period=billing_period,
            billing_period_start=billing_period_start,
            billing_period_end=billing_period_end,
            s3_key=s3_key,
            bucket=bucket,
            files=files,
            columns=columns,
            compression=compression
        )

    def get_billing_month_sort_key(self) -> str:
        """Get sort key for billing month (newest first)."""
        return self.billing_period

    def __str__(self) -> str:
        return f"CURManifest(id={self.id}, version={self.version}, period={self.billing_period}, files={len(self.files)})"