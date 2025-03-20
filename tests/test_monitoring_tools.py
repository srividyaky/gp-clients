import pytest
import os
import logging
from utils import config

logger = logging.getLogger(__name__)


class TestMonitoringTools:
    """Tests for Greenplum monitoring tools"""

    def test_gpstate(self, ssh_connection):
        """Test gpstate functionality"""
        # Set environment variable
        logger.info("Testing gpstate")
        command = f"source ~/.bashrc && export COORDINATOR_DATA_DIRECTORY={config.COORDINATOR_DATA_DIRECTORY} && gpstate"
        success, output, error = ssh_connection.execute_command(command)
        assert success, f"Failed to run gpstate: {error}"

        # Verify output contains expected sections
        assert "coordinator status" in output.lower(), "gpstate output missing coordinator status"
        assert "segment status" in output.lower(), "gpstate output missing segment status"

    def test_gpss_available(self, ssh_connection):
        """Test if gpss is available"""
        # Check if gpss is installed
        logger.info("Testing if gpss is available")
        success, output, error = ssh_connection.execute_command("which gpss")

        # This is a soft check since gpss might not be installed in all environments
        if not success:
            logger.warning("gpss not found - skipping detailed test")
            pytest.skip("gpss not available")

        # If available, test basic execution
        success, output, error = ssh_connection.execute_command("gpss --version")
        assert success, "gpss command failed"

    def test_gpsscli_available(self, ssh_connection):
        """Test if gpsscli is available"""
        # Check if gpsscli is installed
        logger.info("Testing if gpsscli is available")
        success, output, error = ssh_connection.execute_command("which gpsscli")

        # This is a soft check since gpsscli might not be installed in all environments
        if not success:
            logger.warning("gpsscli not found - skipping detailed test")
            pytest.skip("gpsscli not available")

        # If available, test basic execution
        success, output, error = ssh_connection.execute_command("gpsscli --help")
        assert success, "gpsscli command failed"
        assert "usage:" in output.lower(), "gpsscli help output missing usage information"