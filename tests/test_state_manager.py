"""Tests for pipeline state management - TDD approach focusing on daily pipeline runs."""

import pytest
import tempfile
import os
from finops.state.manager import StateManager


class TestStateManagerBasics:
    """Test basic StateManager functionality."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database file for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            yield tmp.name
        # Clean up
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)

    @pytest.fixture
    def state_manager(self, temp_db_path):
        """Create a StateManager instance with temporary database."""
        return StateManager(temp_db_path)

    def test_can_create_state_manager(self, temp_db_path):
        """Test that we can create a StateManager instance."""
        manager = StateManager(temp_db_path)
        assert manager is not None
        assert manager.database_path == temp_db_path


class TestDailyPipelineWorkflow:
    """Test the daily pipeline workflow: list-manifests -> check state -> insert new manifests."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database file for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            yield tmp.name
        # Clean up
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)

    @pytest.fixture
    def state_manager(self, temp_db_path):
        """Create a StateManager instance with temporary database."""
        return StateManager(temp_db_path)

    def test_new_manifest_discovery_workflow(self, state_manager):
        """
        Test: Daily run finds NEW manifests that aren't in state DB yet.
        Scenario: list-manifests finds assembly_id "new-123" for first time.
        Expected: Insert as "discovered" state, ready for future processing.
        """
        # Simulate: list-manifests found a new manifest
        assembly_id = "new-assembly-123"

        # First check: this manifest has never been seen before
        assert state_manager.is_already_processed("aws", assembly_id) is False

        # Record it as discovered (what list-manifests would do)
        result = state_manager.record_discovered(
            vendor="aws",
            billing_version_id=assembly_id,
            billing_month="2024-01",
            export_name="my-cur-export"
        )

        # Verify it was recorded correctly
        assert result.vendor == "aws"
        assert result.billing_version_id == assembly_id
        assert result.billing_month == "2024-01"
        assert result.export_name == "my-cur-export"
        assert result.state == "discovered"  # Ready for processing
        assert result.is_current is True

        # Still not processed (discovered != loaded)
        assert state_manager.is_already_processed("aws", assembly_id) is False

    def test_already_processed_manifest_workflow(self, state_manager):
        """
        Test: Daily run finds OLD manifests that are already loaded.
        Scenario: list-manifests finds assembly_id "old-456" that was processed yesterday.
        Expected: is_already_processed() returns True, skip this manifest.
        """
        assembly_id = "old-assembly-456"

        # Simulate: This manifest was processed in a previous run
        # (Record it and mark as loaded to simulate previous processing)
        state_manager.record_discovered("aws", assembly_id, "2024-01", "my-cur-export")
        state_manager.mark_completed("aws", assembly_id)  # This will need to be implemented

        # Daily run checks: should skip this manifest
        assert state_manager.is_already_processed("aws", assembly_id) is True

    def test_mixed_old_and_new_manifests(self, state_manager):
        """
        Test: Daily run finds MIX of old (loaded) and new (not in DB) manifests.
        Scenario: list-manifests returns 3 manifests - 1 old (loaded), 2 new.
        Expected: Skip the old one, record the 2 new ones as "discovered".
        """
        # Simulate previous day: one manifest was already processed
        old_assembly = "old-assembly-001"
        state_manager.record_discovered("aws", old_assembly, "2024-01", "my-cur-export")
        state_manager.mark_completed("aws", old_assembly)

        # Today's list-manifests finds 3 manifests total
        manifests_from_s3 = [
            {"assembly_id": "old-assembly-001", "month": "2024-01"},  # Already loaded
            {"assembly_id": "new-assembly-002", "month": "2024-01"},  # New
            {"assembly_id": "new-assembly-003", "month": "2024-02"},  # New
        ]

        newly_discovered = []
        for manifest in manifests_from_s3:
            assembly_id = manifest["assembly_id"]

            # Check if already processed (daily workflow)
            if not state_manager.is_already_processed("aws", assembly_id):
                # Record new manifest as discovered
                result = state_manager.record_discovered(
                    vendor="aws",
                    billing_version_id=assembly_id,
                    billing_month=manifest["month"],
                    export_name="my-cur-export"
                )
                newly_discovered.append(result)

        # Should have skipped the old one, recorded 2 new ones
        assert len(newly_discovered) == 2
        assert newly_discovered[0].billing_version_id == "new-assembly-002"
        assert newly_discovered[1].billing_version_id == "new-assembly-003"

        # Verify states
        assert newly_discovered[0].state == "discovered"
        assert newly_discovered[1].state == "discovered"


class TestFutureProcessingQueries:
    """Test queries that future pipeline implementation will use."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database file for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            yield tmp.name
        # Clean up
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)

    @pytest.fixture
    def state_manager(self, temp_db_path):
        """Create a StateManager instance with temporary database."""
        return StateManager(temp_db_path)

    def test_get_manifests_ready_for_processing(self, state_manager):
        """
        Test: Query for manifests that need to be processed.
        Scenario: After daily runs, we want to find all "discovered" manifests to load.
        Expected: Only return manifests in "discovered" state.
        """
        # Set up various manifest states
        state_manager.record_discovered("aws", "ready-001", "2024-01", "export-1")
        state_manager.record_discovered("aws", "ready-002", "2024-02", "export-1")

        # This one is already loaded (should be excluded)
        state_manager.record_discovered("aws", "done-003", "2024-01", "export-1")
        state_manager.mark_completed("aws", "done-003")

        # Query for manifests ready to process
        ready_manifests = state_manager.get_manifests_to_process("aws")

        # Should only return the "discovered" ones
        assert len(ready_manifests) == 2
        ready_ids = [m.billing_version_id for m in ready_manifests]
        assert "ready-001" in ready_ids
        assert "ready-002" in ready_ids
        assert "done-003" not in ready_ids

    def test_get_pipeline_ready_manifests_latest_per_month(self, state_manager):
        """
        Test: Get only the latest manifest per billing month for pipeline processing.
        Scenario: Multiple manifests exist for same billing month (newer versions).
        Expected: Return only the latest manifest per month based on created_at timestamp.
        """
        # Set up multiple manifests for same billing month
        # First manifest for 2024-01
        state_manager.record_discovered("aws", "assembly-001", "2024-01", "export-1")

        # Second manifest for same month (newer)
        state_manager.record_discovered("aws", "assembly-002", "2024-01", "export-1")

        # Third manifest for same month (newest)
        state_manager.record_discovered("aws", "assembly-003", "2024-01", "export-1")

        # One manifest for different month
        state_manager.record_discovered("aws", "assembly-004", "2024-02", "export-1")

        # Query for pipeline-ready manifests (latest per month only)
        pipeline_manifests = state_manager.get_pipeline_ready_manifests("aws")

        # Should return only 2 manifests: latest from 2024-01 and the one from 2024-02
        assert len(pipeline_manifests) == 2

        # Extract details for verification
        pipeline_ids = [m.billing_version_id for m in pipeline_manifests]
        pipeline_months = [m.billing_month for m in pipeline_manifests]

        # Should include latest from 2024-01 (assembly-003) and 2024-02 (assembly-004)
        assert "assembly-003" in pipeline_ids  # Latest for 2024-01
        assert "assembly-004" in pipeline_ids  # Only one for 2024-02
        assert "assembly-001" not in pipeline_ids  # Older version for 2024-01
        assert "assembly-002" not in pipeline_ids  # Older version for 2024-01

        # Verify both months are represented
        assert "2024-01" in pipeline_months
        assert "2024-02" in pipeline_months

    def test_get_pipeline_ready_manifests_different_months(self, state_manager):
        """
        Test: Get manifests from different billing months.
        Scenario: One manifest per month across multiple months.
        Expected: Return all manifests since each is latest for its month.
        """
        # Set up manifests for different months
        state_manager.record_discovered("aws", "jan-assembly", "2024-01", "export-1")
        state_manager.record_discovered("aws", "feb-assembly", "2024-02", "export-1")
        state_manager.record_discovered("aws", "mar-assembly", "2024-03", "export-1")

        # Query for pipeline-ready manifests
        pipeline_manifests = state_manager.get_pipeline_ready_manifests("aws")

        # Should return all 3 since each is the only/latest for its month
        assert len(pipeline_manifests) == 3

        pipeline_ids = [m.billing_version_id for m in pipeline_manifests]
        pipeline_months = [m.billing_month for m in pipeline_manifests]

        # All should be included
        assert "jan-assembly" in pipeline_ids
        assert "feb-assembly" in pipeline_ids
        assert "mar-assembly" in pipeline_ids

        # All months should be represented
        assert "2024-01" in pipeline_months
        assert "2024-02" in pipeline_months
        assert "2024-03" in pipeline_months

    def test_get_pipeline_ready_manifests_empty_discovered(self, state_manager):
        """
        Test: Get manifests when no manifests are in discovered state.
        Scenario: Database has manifests but all are in 'loaded' state.
        Expected: Return empty list.
        """
        # Set up manifests that are already processed
        state_manager.record_discovered("aws", "processed-001", "2024-01", "export-1")
        state_manager.mark_completed("aws", "processed-001")

        state_manager.record_discovered("aws", "processed-002", "2024-02", "export-1")
        state_manager.mark_completed("aws", "processed-002")

        # Query for pipeline-ready manifests
        pipeline_manifests = state_manager.get_pipeline_ready_manifests("aws")

        # Should return empty list since no manifests are in "discovered" state
        assert len(pipeline_manifests) == 0
        assert pipeline_manifests == []

    def test_get_pipeline_ready_manifests_include_failed(self, state_manager):
        """
        Test: Get manifests including failed states for retry capability.
        Scenario: Mix of discovered, loaded, and failed manifests.
        Expected:
        - include_failed=False (default): return only discovered
        - include_failed=True: return both discovered and failed
        """
        # Set up manifests in various states
        state_manager.record_discovered("aws", "discovered-001", "2024-01", "export-1")
        state_manager.record_discovered("aws", "discovered-002", "2024-02", "export-1")

        # One completed (should never be included)
        state_manager.record_discovered("aws", "completed-003", "2024-03", "export-1")
        state_manager.mark_completed("aws", "completed-003")

        # One failed (should be included only when include_failed=True)
        # Note: We'll need to implement mark_failed method, for now simulate with direct state update
        failed_record = state_manager.record_discovered("aws", "failed-004", "2024-04", "export-1")
        # TODO: Replace with proper mark_failed method when implemented
        from finops.state.database import get_connection
        with get_connection(state_manager.database_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE billing_state SET state = 'failed' WHERE billing_version_id = ?",
                ("failed-004",)
            )
            conn.commit()

        # Test default behavior (include_failed=False)
        discovered_only = state_manager.get_pipeline_ready_manifests("aws")
        assert len(discovered_only) == 2
        discovered_ids = [m.billing_version_id for m in discovered_only]
        assert "discovered-001" in discovered_ids
        assert "discovered-002" in discovered_ids
        assert "completed-003" not in discovered_ids
        assert "failed-004" not in discovered_ids

        # Test include_failed=True
        with_failed = state_manager.get_pipeline_ready_manifests("aws", include_failed=True)
        assert len(with_failed) == 3
        with_failed_ids = [m.billing_version_id for m in with_failed]
        assert "discovered-001" in with_failed_ids
        assert "discovered-002" in with_failed_ids
        assert "failed-004" in with_failed_ids  # Now included for retry
        assert "completed-003" not in with_failed_ids  # Still excluded