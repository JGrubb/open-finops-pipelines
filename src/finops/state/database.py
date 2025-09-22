"""Database initialization and utilities for pipeline state tracking."""

import sqlite3
import os
from pathlib import Path


def initialize_database(database_path: str) -> None:
    """Initialize the pipeline state database with required schema.

    Args:
        database_path: Path to the SQLite database file
    """
    # Ensure parent directory exists
    db_path = Path(database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(database_path) as conn:
        cursor = conn.cursor()

        # Create billing_state table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS billing_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendor TEXT NOT NULL,
                billing_version_id TEXT NOT NULL,
                billing_month TEXT NOT NULL,
                export_name TEXT NOT NULL,
                state TEXT NOT NULL,
                is_current INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(vendor, billing_version_id)
            )
        """)

        # Create index for common queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_billing_state_vendor_month
            ON billing_state(vendor, billing_month)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_billing_state_current
            ON billing_state(vendor, billing_month, is_current)
            WHERE is_current = 1
        """)

        conn.commit()


def get_connection(database_path: str) -> sqlite3.Connection:
    """Get a connection to the pipeline state database.

    Args:
        database_path: Path to the SQLite database file

    Returns:
        SQLite connection object
    """
    # Initialize database if it doesn't exist
    if not os.path.exists(database_path):
        initialize_database(database_path)

    conn = sqlite3.connect(database_path)
    # Enable row factory for dict-like access
    conn.row_factory = sqlite3.Row
    return conn