"""AWS-specific data types and models."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from enum import Enum


class CURVersion(Enum):
    """CUR (Cost and Usage Report) version."""
    V1 = "v1"
    V2 = "v2"


@dataclass
class ManifestFile:
    """Represents a single data file referenced in a CUR manifest."""
    key: str
    size: int
    checksum: Optional[str] = None


@dataclass
class Manifest:
    """Represents a parsed CUR manifest with metadata and file list."""
    assembly_id: str
    billing_period: str
    bucket: str
    report_name: str
    report_keys: List[str]
    files: List[ManifestFile]
    cur_version: CURVersion
    compression: Optional[str] = None
    format: Optional[str] = None
    schema_version: Optional[str] = None

    @property
    def billing_year_month(self) -> str:
        """Extract YYYY-MM from billing period for filtering."""
        # Handle both v1 format (20240101-20240131) and v2 format (2024-01)
        # NOTE: V2 manifest format may differ - will need to adapt when implementing V2 parsing
        if "-" in self.billing_period and len(self.billing_period) > 7:
            # v1 format: extract from date range
            start_date = self.billing_period.split("-")[0]
            return f"{start_date[:4]}-{start_date[4:6]}"
        else:
            # v2 format: assumed to be in YYYY-MM format, but may need adjustment
            return self.billing_period