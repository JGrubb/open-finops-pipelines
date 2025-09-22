"""Integration tests for CLI commands with StateManager."""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch
from finops.cli.aws import list_manifests_command
from finops.config.schema import FinopsConfig, AWSConfig, StateConfig
from finops.vendors.aws.types import Manifest, CURVersion


def test_list_manifests_with_state_integration():
    """Test that list-manifests integrates properly with StateManager."""

    # Create temporary state database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        temp_db_path = tmp.name

    try:
        # Create mock config
        config = FinopsConfig(
            aws=AWSConfig(
                bucket="test-bucket",
                export_name="test-export",
                prefix="test-prefix"
            ),
            state=StateConfig(database_path=temp_db_path)
        )

        # Create mock args
        args = Mock()
        args.start_date = None
        args.end_date = None
        args.include_processed = False

        # Create mock manifests that would come from S3
        mock_manifest1 = Mock()
        mock_manifest1.assembly_id = "test-assembly-123"
        mock_manifest1.billing_period = "20240101-20240131"
        mock_manifest1.billing_year_month = "2024-01"
        mock_manifest1.cur_version = CURVersion.V1
        mock_manifest1.files = []
        mock_manifest1.format = "CSV"

        mock_manifest2 = Mock()
        mock_manifest2.assembly_id = "test-assembly-456"
        mock_manifest2.billing_period = "20240201-20240229"
        mock_manifest2.billing_year_month = "2024-02"
        mock_manifest2.cur_version = CURVersion.V1
        mock_manifest2.files = []
        mock_manifest2.format = "CSV"

        # Mock AWS client and discovery
        with patch('finops.cli.aws.AWSClient') as mock_aws_client, \
             patch('finops.cli.aws.ManifestDiscovery') as mock_discovery:

            # Mock AWS client
            mock_aws_instance = Mock()
            mock_aws_client.return_value = mock_aws_instance

            # Mock discovery
            mock_discovery_instance = Mock()
            mock_discovery_instance.discover_manifests.return_value = [mock_manifest1, mock_manifest2]
            mock_discovery.return_value = mock_discovery_instance

            # Capture output
            import io
            import sys
            captured_output = io.StringIO()

            with patch('sys.stdout', captured_output):
                # First run: should discover both manifests
                result = list_manifests_command(config, args)

            output1 = captured_output.getvalue()

            # Verify first run
            assert result is None  # Success
            assert "Found 2 manifest(s) in S3" in output1
            assert "Recorded 2 new manifest(s) as 'discovered'" in output1
            assert "test-assembly-123" in output1
            assert "test-assembly-456" in output1
            assert "Newly discovered (ready for processing)" in output1

            # Second run: manifests have been seen before, should be skipped
            captured_output = io.StringIO()
            with patch('sys.stdout', captured_output):
                result = list_manifests_command(config, args)

            output2 = captured_output.getvalue()

            # Verify second run - manifests have been seen before
            assert result is None  # Success
            assert "Found 2 manifest(s) in S3" in output2
            # Should show no new manifests because they were already discovered
            assert ("No new manifests found" in output2 or
                    "Showing 0 new manifest(s)" in output2)

            # Third run with --include-processed: should show all manifests
            args.include_processed = True
            captured_output = io.StringIO()
            with patch('sys.stdout', captured_output):
                result = list_manifests_command(config, args)

            output3 = captured_output.getvalue()

            # Verify third run
            assert result is None  # Success
            assert "Found 2 manifest(s) in S3" in output3
            assert "Showing all 2 manifest(s)" in output3

    finally:
        # Clean up
        if os.path.exists(temp_db_path):
            os.unlink(temp_db_path)


def test_daily_workflow_simulation():
    """Simulate the actual daily workflow: new manifests appear over time."""

    # Create temporary state database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        temp_db_path = tmp.name

    try:
        # Create mock config
        config = FinopsConfig(
            aws=AWSConfig(
                bucket="test-bucket",
                export_name="test-export",
                prefix="test-prefix"
            ),
            state=StateConfig(database_path=temp_db_path)
        )

        args = Mock()
        args.start_date = None
        args.end_date = None
        args.include_processed = False

        # Day 1: Only January manifest exists
        mock_jan_manifest = Mock()
        mock_jan_manifest.assembly_id = "jan-assembly-123"
        mock_jan_manifest.billing_year_month = "2024-01"
        mock_jan_manifest.billing_period = "20240101-20240131"
        mock_jan_manifest.cur_version = CURVersion.V1
        mock_jan_manifest.files = []
        mock_jan_manifest.format = "CSV"

        with patch('finops.cli.aws.AWSClient') as mock_aws_client, \
             patch('finops.cli.aws.ManifestDiscovery') as mock_discovery:

            mock_aws_instance = Mock()
            mock_aws_client.return_value = mock_aws_instance

            mock_discovery_instance = Mock()
            mock_discovery_instance.discover_manifests.return_value = [mock_jan_manifest]
            mock_discovery.return_value = mock_discovery_instance

            # Day 1 run
            import io
            captured_output = io.StringIO()
            with patch('sys.stdout', captured_output):
                list_manifests_command(config, args)

            day1_output = captured_output.getvalue()
            assert "Recorded 1 new manifest(s) as 'discovered'" in day1_output

        # Day 2: February manifest appears
        mock_feb_manifest = Mock()
        mock_feb_manifest.assembly_id = "feb-assembly-456"
        mock_feb_manifest.billing_year_month = "2024-02"
        mock_feb_manifest.billing_period = "20240201-20240229"
        mock_feb_manifest.cur_version = CURVersion.V1
        mock_feb_manifest.files = []
        mock_feb_manifest.format = "CSV"

        with patch('finops.cli.aws.AWSClient') as mock_aws_client, \
             patch('finops.cli.aws.ManifestDiscovery') as mock_discovery:

            mock_aws_instance = Mock()
            mock_aws_client.return_value = mock_aws_instance

            mock_discovery_instance = Mock()
            # Now S3 returns both January (old) and February (new)
            mock_discovery_instance.discover_manifests.return_value = [mock_jan_manifest, mock_feb_manifest]
            mock_discovery.return_value = mock_discovery_instance

            # Day 2 run
            captured_output = io.StringIO()
            with patch('sys.stdout', captured_output):
                list_manifests_command(config, args)

            day2_output = captured_output.getvalue()
            assert "Found 2 manifest(s) in S3" in day2_output
            assert "Recorded 1 new manifest(s) as 'discovered'" in day2_output  # Only February is new
            assert ("Skipped 1 already" in day2_output or  # January was already seen
                   "feb-assembly-456" in day2_output)  # February should appear as new

    finally:
        # Clean up
        if os.path.exists(temp_db_path):
            os.unlink(temp_db_path)