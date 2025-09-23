"""State manager for tracking pipeline execution state."""

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime


@dataclass
class StateRecord:
    """State record for pipeline tracking."""
    vendor: str
    billing_version_id: str
    billing_month: str
    export_name: str
    state: str
    is_current: bool
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class StateManager:
    """Manages pipeline state operations using SQLite."""

    def __init__(self, database_path: str):
        """Initialize state manager with database path."""
        self.database_path = database_path
        # Initialize database on creation
        from .database import initialize_database
        initialize_database(database_path)

    def is_already_processed(self, vendor: str, billing_version_id: str) -> bool:
        """Check if a billing version has been processed successfully."""
        from .database import get_connection

        with get_connection(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM billing_state
                WHERE vendor = ? AND billing_version_id = ? AND state = 'loaded'
            """, (vendor, billing_version_id))

            count = cursor.fetchone()[0]
            return count > 0

    def is_already_seen(self, vendor: str, billing_version_id: str) -> bool:
        """Check if a billing version has been seen before (in any state)."""
        from .database import get_connection

        with get_connection(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM billing_state
                WHERE vendor = ? AND billing_version_id = ?
            """, (vendor, billing_version_id))

            count = cursor.fetchone()[0]
            return count > 0

    def record_discovered(self, vendor: str, billing_version_id: str, billing_month: str, export_name: str) -> StateRecord:
        """Record a newly discovered manifest."""
        from .database import get_connection

        with get_connection(self.database_path) as conn:
            cursor = conn.cursor()

            # Check if this version already exists
            cursor.execute("""
                SELECT * FROM billing_state
                WHERE vendor = ? AND billing_version_id = ?
            """, (vendor, billing_version_id))

            existing_row = cursor.fetchone()
            if existing_row:
                # Return existing record
                return self._row_to_record(existing_row)

            # Insert new record
            cursor.execute("""
                INSERT INTO billing_state (
                    vendor, billing_version_id, billing_month, export_name, state, is_current
                ) VALUES (?, ?, ?, ?, 'discovered', 1)
            """, (vendor, billing_version_id, billing_month, export_name))

            record_id = cursor.lastrowid
            conn.commit()

            # Return the created record
            cursor.execute("SELECT * FROM billing_state WHERE id = ?", (record_id,))
            row = cursor.fetchone()
            return self._row_to_record(row)

    def mark_completed(self, vendor: str, billing_version_id: str):
        """Mark a manifest as completed/loaded."""
        from .database import get_connection

        with get_connection(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE billing_state
                SET state = 'loaded', updated_at = CURRENT_TIMESTAMP
                WHERE vendor = ? AND billing_version_id = ?
            """, (vendor, billing_version_id))
            conn.commit()

    def get_manifests_to_process(self, vendor: str) -> List[StateRecord]:
        """Get manifests that are ready for processing (in 'discovered' state)."""
        from .database import get_connection

        with get_connection(self.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM billing_state
                WHERE vendor = ? AND state = 'discovered'
                ORDER BY created_at
            """, (vendor,))

            rows = cursor.fetchall()
            return [self._row_to_record(row) for row in rows]

    def get_pipeline_ready_manifests(self, vendor: str, include_failed: bool = False) -> List[StateRecord]:
        """Get latest manifest per billing month in 'discovered' (and optionally 'failed') state."""
        from .database import get_connection

        with get_connection(self.database_path) as conn:
            cursor = conn.cursor()

            # Build the state filter based on include_failed parameter
            if include_failed:
                state_filter = "state IN ('discovered', 'failed')"
                state_params = (vendor,)
            else:
                state_filter = "state = 'discovered'"
                state_params = (vendor,)

            # Use window function to get only the latest manifest per billing month
            query = f"""
                WITH ranked_manifests AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY billing_month
                               ORDER BY created_at DESC, billing_version_id DESC
                           ) as rn
                    FROM billing_state
                    WHERE vendor = ? AND {state_filter}
                )
                SELECT * FROM ranked_manifests
                WHERE rn = 1
                ORDER BY billing_month
            """

            cursor.execute(query, state_params)
            rows = cursor.fetchall()
            return [self._row_to_record(row) for row in rows]

    def _row_to_record(self, row) -> StateRecord:
        """Convert SQLite row to StateRecord."""
        return StateRecord(
            id=row['id'],
            vendor=row['vendor'],
            billing_version_id=row['billing_version_id'],
            billing_month=row['billing_month'],
            export_name=row['export_name'],
            state=row['state'],
            is_current=bool(row['is_current']),
            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
            updated_at=datetime.fromisoformat(row['updated_at']) if row['updated_at'] else None
        )