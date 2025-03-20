import pytest
import os
import logging
import tempfile
from utils import config
from utils.helpers import run_psql_query, create_test_csv_file, create_gpload_yaml

logger = logging.getLogger(__name__)


class TestDataLoadTools:
    """Tests for Greenplum data loading tools"""

    def test_gpload_basic(self, ssh_connection, test_table, test_csv_file, unique_name):
        """Test basic gpload functionality"""
        # Unpack test_table fixture
        test_database, table_name = test_table

        # Create YAML configuration file
        yaml_file = f"gpload_config_{unique_name}.yaml"
        yaml_content = f"""
---
VERSION: 1.0.0.1
DATABASE: {test_database}
USER: {config.USERNAME}
HOST: {config.HOST}
PORT: {config.PORT}
GPLOAD:
  INPUT:
    - SOURCE:
        LOCAL_HOSTNAME:
          - localhost
        PORT: 8080
        FILE:
          - {os.path.basename(test_csv_file)}
    - FORMAT: CSV
    - DELIMITER: ','
    - QUOTE: '"'
    - ERROR_LIMIT: 25
    - ERROR_TABLE: err_{table_name}
  OUTPUT:
    - TABLE: {table_name}
    - MODE: INSERT
"""

        # Create YAML file on the remote server
        remote_yaml_path = f"/tmp/{yaml_file}"
        ssh_connection.execute_command(f"echo '{yaml_content}' > {remote_yaml_path}")

        # Copy CSV file to remote server
        remote_csv_path = f"/tmp/{os.path.basename(test_csv_file)}"
        with open(test_csv_file, 'r') as f:
            csv_content = f.read()
        ssh_connection.execute_command(f"echo '{csv_content}' > {remote_csv_path}")

        # Run gpload
        logger.info(f"Testing gpload with config: {remote_yaml_path}")
        success, output, error = ssh_connection.execute_command(
            f"source ~/.bashrc && gpload -f {remote_yaml_path}"
        )
        assert success, f"Failed to run gpload: {error}"

        # Verify data was loaded
        success, output, error = ssh_connection.execute_command(
            f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -t -c \"SELECT count(*) FROM {table_name}\""
        )
        assert success and "3" in output, "Data not loaded correctly"

        # Clean up
        ssh_connection.execute_command(f"rm -f {remote_yaml_path} {remote_csv_path}")

    def test_gpfdist_basic(self, ssh_connection, test_table, test_csv_file, unique_name):
        """Test basic gpfdist functionality"""
        # Unpack test_table fixture
        test_database, table_name = test_table

        # Copy CSV file to remote server
        remote_csv_path = f"/tmp/{os.path.basename(test_csv_file)}"
        with open(test_csv_file, 'r') as f:
            csv_content = f.read()
        ssh_connection.execute_command(f"echo '{csv_content}' > {remote_csv_path}")

        # Start gpfdist in background
        port = 8080
        logger.info(f"Starting gpfdist on port {port}")
        ssh_connection.execute_command(
            f"source ~/.bashrc && nohup gpfdist -d /tmp -p {port} > /tmp/gpfdist_{unique_name}.log 2>&1 &"
        )

        # Wait for gpfdist to start
        import time
        time.sleep(5)

        # Check if gpfdist is running
        success, output, error = ssh_connection.execute_command(f"pgrep -f 'gpfdist -d /tmp -p {port}'")
        assert success and output.strip(), "gpfdist not running"

        # Create external table
        ext_table_name = f"ext_{table_name}"
        create_ext_table_query = f"""
        CREATE EXTERNAL TABLE {ext_table_name} (
            id INT,
            name VARCHAR(50),
            value FLOAT
        )
        LOCATION ('gpfdist://{config.HOST}:{port}/{os.path.basename(remote_csv_path)}')
        FORMAT 'CSV' (DELIMITER ',');
        """

        success, output, error = ssh_connection.execute_command(
            f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -c \"{create_ext_table_query}\""
        )
        assert success, f"Failed to create external table: {error}"

        # Query external table
        success, output, error = ssh_connection.execute_command(
            f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -t -c \"SELECT count(*) FROM {ext_table_name}\""
        )
        assert success and "3" in output, "Data not accessible through external table"

        # Load data into regular table
        load_query = f"INSERT INTO {table_name} SELECT * FROM {ext_table_name};"
        success, output, error = ssh_connection.execute_command(
            f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -c \"{load_query}\""
        )
        assert success, f"Failed to load data from external table: {error}"

        # Verify data was loaded
        success, output, error = ssh_connection.execute_command(
            f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -t -c \"SELECT count(*) FROM {table_name}\""
        )
        assert success and "3" in output, "Data not loaded correctly"

        # Stop gpfdist
        ssh_connection.execute_command(f"pkill -f 'gpfdist -d /tmp -p {port}'")

        # Clean up
        ssh_connection.execute_command(f"rm -f {remote_csv_path}")
        ssh_connection.execute_command(
            f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -c \"DROP EXTERNAL TABLE IF EXISTS {ext_table_name}\""
        )