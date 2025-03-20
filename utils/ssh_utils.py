import paramiko
import logging
from . import config

logger = logging.getLogger(__name__)


class SSHConnection:
    def __init__(self, host=None, username=None, password=None):
        self.host = host or config.HOST
        self.username = username or config.USERNAME
        self.password = password or config.PASSWORD
        self.ssh_client = None

    def connect(self):
        """Establish SSH connection to the Greenplum server"""
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                hostname=self.host,
                username=self.username,
                password=self.password
            )
            logger.info(f"Successfully connected to {self.host}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to {self.host}: {str(e)}")
            return False

    def execute_command(self, command):
        """Execute a command on the Greenplum server"""
        if not self.ssh_client:
            if not self.connect():
                return False, "Not connected to server"

        try:
            logger.debug(f"Executing command: {command}")
            stdin, stdout, stderr = self.ssh_client.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')

            logger.debug(f"Command output: {output}")
            if error:
                logger.warning(f"Command error: {error}")

            return exit_code == 0, output, error
        except Exception as e:
            logger.error(f"Error executing command: {str(e)}")
            return False, "", str(e)

    def check_greenplum_status(self):
        """Check if Greenplum is running"""
        command = "gpstate -a"
        success, output, error = self.execute_command(command)
        logger.info(f"Greenplum status check output: {output}")
        logger.info(f"Greenplum status check error: {error}")

        # Check for various possible active status indicators
        coordinator_active = any([
            "coordinator instance = active" in output.lower(),
            "coordinator active" in output.lower(),
            "coordinator instance                                      = active" in output.lower()
        ])

        # For debugging
        logger.info(f"Coordinator active status detected: {coordinator_active}")

        return success and coordinator_active

    def close(self):
        """Close the SSH connection"""
        if self.ssh_client:
            self.ssh_client.close()
            logger.info("SSH connection closed")