import sqlite3
import json
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from finops.models.manifest import CURManifest


class StateDB:
    """SQLite database for tracking pipeline state and manifest processing."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._ensure_database()

    def _ensure_database(self):
        """Create database and tables if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS manifests (
                    manifest_id TEXT PRIMARY KEY,
                    vendor TEXT NOT NULL,
                    billing_period TEXT NOT NULL,
                    billing_period_start TEXT NOT NULL,
                    billing_period_end TEXT NOT NULL,
                    cur_version TEXT NOT NULL,
                    s3_bucket TEXT NOT NULL,
                    s3_manifest_key TEXT NOT NULL,
                    csv_files TEXT NOT NULL,  -- JSON array of S3 keys
                    columns_schema TEXT NOT NULL,  -- JSON array of column definitions
                    compression TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error_message TEXT
                )
            """)

            # Create exports table for tracking export operations
            conn.execute("""
                CREATE TABLE IF NOT EXISTS exports (
                    export_id TEXT PRIMARY KEY,
                    vendor TEXT NOT NULL,
                    billing_period TEXT NOT NULL,
                    export_type TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error_message TEXT
                )
            """)

            # Create index for efficient querying
            conn.execute("CREATE INDEX IF NOT EXISTS idx_manifests_state ON manifests(state)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_manifests_billing_period ON manifests(billing_period)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_manifests_vendor ON manifests(vendor)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exports_state ON exports(state)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exports_billing_period ON exports(billing_period)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exports_vendor ON exports(vendor)")

    def save_manifest(self, manifest: CURManifest, state: str = "discovered") -> None:
        """Save or update a manifest record."""
        now = datetime.utcnow().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO manifests (
                    manifest_id, vendor, billing_period, billing_period_start,
                    billing_period_end, cur_version, s3_bucket, s3_manifest_key,
                    csv_files, columns_schema, compression, state,
                    created_at, updated_at, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                manifest.id,
                "aws",  # hardcoded for now, could be extracted later
                manifest.billing_period,
                manifest.billing_period_start,
                manifest.billing_period_end,
                manifest.version,
                manifest.bucket,
                manifest.s3_key,
                json.dumps(manifest.files),  # Store as JSON array
                json.dumps(manifest.columns),  # Store as JSON array
                manifest.compression,
                state,
                now,
                now,
                None
            ))

    def update_manifest_state(self, manifest_id: str, new_state: str, error_message: Optional[str] = None) -> None:
        """Update the state of a manifest."""
        now = datetime.utcnow().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE manifests
                SET state = ?, updated_at = ?, error_message = ?
                WHERE manifest_id = ?
            """, (new_state, now, error_message, manifest_id))

    def get_manifests_by_state(self, state: str, vendor: str = "aws") -> List[Dict]:
        """Get all manifests in a specific state."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row  # Enable column access by name
            cursor = conn.execute("""
                SELECT * FROM manifests
                WHERE state = ? AND vendor = ?
                ORDER BY billing_period DESC
            """, (state, vendor))

            return [dict(row) for row in cursor.fetchall()]

    def get_manifest_summary(self, vendor: str = "aws") -> Dict:
        """Get summary statistics of manifests by state."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT state, COUNT(*) as count
                FROM manifests
                WHERE vendor = ?
                GROUP BY state
                ORDER BY
                    CASE state
                        WHEN 'discovered' THEN 1
                        WHEN 'downloading' THEN 2
                        WHEN 'staged' THEN 3
                        WHEN 'loading' THEN 4
                        WHEN 'loaded' THEN 5
                        WHEN 'failed' THEN 6
                        ELSE 7
                    END
            """, (vendor,))

            summary = {}
            for row in cursor.fetchall():
                summary[row[0]] = row[1]

            return summary

    def get_manifest_details(self, manifest_id: str) -> Optional[Dict]:
        """Get detailed information about a specific manifest."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM manifests WHERE manifest_id = ?
            """, (manifest_id,))

            row = cursor.fetchone()
            return dict(row) if row else None

    def clear_manifests(self, vendor: str = "aws") -> int:
        """Clear all manifest records for a vendor. Returns number of deleted records."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM manifests WHERE vendor = ?", (vendor,))
            return cursor.rowcount

    def get_latest_manifests(self, limit: int = 10, vendor: str = "aws") -> List[Dict]:
        """Get the most recently discovered manifests."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM manifests
                WHERE vendor = ?
                ORDER BY billing_period DESC, created_at DESC
                LIMIT ?
            """, (vendor, limit))

            return [dict(row) for row in cursor.fetchall()]

    def get_discovered_manifests_by_date_range(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None, vendor: str = "aws"
    ) -> List[Dict]:
        """Get discovered manifests within a date range (YYYY-MM format)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            base_query = """
                SELECT * FROM manifests
                WHERE vendor = ? AND state = 'discovered'
            """
            params = [vendor]

            if start_date:
                base_query += " AND billing_period >= ?"
                params.append(start_date)

            if end_date:
                base_query += " AND billing_period <= ?"
                params.append(end_date)

            base_query += " ORDER BY billing_period ASC"

            cursor = conn.execute(base_query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_manifests_by_billing_period(self, billing_period: str, vendor: str = "aws") -> List[Dict]:
        """Get all manifests for a specific billing period."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM manifests
                WHERE vendor = ? AND billing_period = ?
                ORDER BY created_at DESC
            """, (vendor, billing_period))

            return [dict(row) for row in cursor.fetchall()]

    def save_export(self, export_id: str, vendor: str, billing_period: str,
                   export_type: str, file_path: str, state: str = "pending") -> None:
        """Save a new export record."""
        now = datetime.utcnow().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO exports (
                    export_id, vendor, billing_period, export_type,
                    file_path, state, created_at, updated_at, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (export_id, vendor, billing_period, export_type,
                  file_path, state, now, now, None))

    def update_export_state(self, export_id: str, new_state: str, error_message: Optional[str] = None) -> None:
        """Update the state of an export."""
        now = datetime.utcnow().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE exports
                SET state = ?, updated_at = ?, error_message = ?
                WHERE export_id = ?
            """, (new_state, now, error_message, export_id))

    def get_export_status(self, vendor: str, billing_period: str, export_type: str) -> Optional[Dict]:
        """Get export status for a specific billing period and type."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM exports
                WHERE vendor = ? AND billing_period = ? AND export_type = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (vendor, billing_period, export_type))

            row = cursor.fetchone()
            return dict(row) if row else None

    def get_exports_by_state(self, state: str, vendor: str = "aws") -> List[Dict]:
        """Get all exports in a specific state."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM exports
                WHERE state = ? AND vendor = ?
                ORDER BY billing_period DESC
            """, (state, vendor))

            return [dict(row) for row in cursor.fetchall()]

    def get_loaded_billing_periods(self, vendor: str = "aws") -> List[str]:
        """Get billing periods that have been successfully loaded into DuckDB."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT DISTINCT billing_period
                FROM manifests
                WHERE vendor = ? AND state = 'loaded'
                ORDER BY billing_period DESC
            """, (vendor,))

            return [row[0] for row in cursor.fetchall()]