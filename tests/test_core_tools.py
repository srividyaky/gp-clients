import pytest
import os
import logging
from utils import config
from utils.helpers import run_psql_query

logger = logging.getLogger(__name__)


class TestCoreTools:
    """Tests for core Greenplum client tools"""

    # def test_createdb_dropdb(self, ssh_connection, unique_name):
    #     """Test createdb and dropdb functionality"""
    #     # Test variables
    #     db_name = f"test_db_{unique_name}"
    #
    #     # Test createdb
    #     logger.info(f"Testing createdb with database: {db_name}")
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && createdb -h {config.HOST} -U {config.USERNAME} {db_name}"
    #     )
    #     assert success, f"Failed to create database: {error}"
    #
    #     # Verify database exists
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -l | grep {db_name}"
    #     )
    #     assert success and db_name in output, f"Database {db_name} not found after creation"
    #
    #     # Test dropdb
    #     logger.info(f"Testing dropdb with database: {db_name}")
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && dropdb -h {config.HOST} -U {config.USERNAME} {db_name}"
    #     )
    #     assert success, f"Failed to drop database: {error}"
    #
    #     # Verify database no longer exists
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -l | grep {db_name}"
    #     )
    #     assert db_name not in output, f"Database {db_name} still exists after drop"
    #
    # def test_createuser_dropuser(self, ssh_connection, unique_name):
    #     """Test createuser and dropuser functionality"""
    #     # Test variables
    #     username = f"test_user_{unique_name}"
    #     password = "Test123!"
    #
    #     # Test createuser
    #     logger.info(f"Testing createuser with username: {username}")
    #     command = f"source ~/.bashrc && echo '{password}\\n{password}' | createuser -h {config.HOST} -U {config.USERNAME} -P {username}"
    #     success, output, error = ssh_connection.execute_command(command)
    #     assert success, f"Failed to create user: {error}"
    #
    #     # Verify user exists
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -t -c \"SELECT 1 FROM pg_roles WHERE rolname='{username}'\""
    #     )
    #     assert success and "1" in output, f"User {username} not found after creation"
    #
    #     # Test dropuser
    #     logger.info(f"Testing dropuser with username: {username}")
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && dropuser -h {config.HOST} -U {config.USERNAME} {username}"
    #     )
    #     assert success, f"Failed to drop user: {error}"
    #
    #     # Verify user no longer exists
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -t -c \"SELECT 1 FROM pg_roles WHERE rolname='{username}'\""
    #     )
    #     assert "1" not in output, f"User {username} still exists after drop"

    def test_psql_basic_query(self, ssh_connection):
        """Test basic psql query functionality"""
        # Test running a simple query
        logger.info("Testing psql with a basic query")
        query = "SELECT version();"
        success, output, error = ssh_connection.execute_command(
            f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -c \"{query}\""
        )

        assert success, f"Failed to execute psql query: {error}"
        assert "Greenplum" in output, "Greenplum version not found in output"
        # Print the query result (output)
        logger.info(f"Query result: {output}")
        # Or you can use print (for debugging purposes)
        print(f"Query result: {output}")

    # def test_pg_dump_restore(self, ssh_connection, test_database, unique_name):
    #     """Test pg_dump and pg_restore functionality"""
    #     # Create a test table and insert data
    #     table_name = f"test_table_{unique_name}"
    #     create_table_query = f"CREATE TABLE {table_name} (id INT, name TEXT);"
    #     insert_query = f"INSERT INTO {table_name} VALUES (1, 'Test');"
    #
    #     ssh_connection.execute_command(
    #         f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -c \"{create_table_query}\""
    #     )
    #     ssh_connection.execute_command(
    #         f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -c \"{insert_query}\""
    #     )
    #
    #     # Test pg_dump
    #     dump_file = f"/tmp/dump_{unique_name}.sql"
    #     logger.info(f"Testing pg_dump with database: {test_database}")
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && pg_dump -h {config.HOST} -U {config.USERNAME} -f {dump_file} {test_database}"
    #     )
    #     assert success, f"Failed to dump database: {error}"
    #
    #     # Verify dump file exists
    #     success, output, error = ssh_connection.execute_command(f"ls -l {dump_file}")
    #     assert success, f"Dump file {dump_file} not found"
    #
    #     # Create a new database for restore
    #     restore_db = f"restore_db_{unique_name}"
    #     ssh_connection.execute_command(
    #         f"source ~/.bashrc && createdb -h {config.HOST} -U {config.USERNAME} {restore_db}"
    #     )
    #
    #     # Test restore
    #     logger.info(f"Testing restore with dump file: {dump_file}")
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {restore_db} -f {dump_file}"
    #     )
    #     assert success, f"Failed to restore database: {error}"
    #
    #     # Verify data exists in restored database
    #     success, output, error = ssh_connection.execute_command(
    #         f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {restore_db} -t -c \"SELECT count(*) FROM {table_name}\""
    #     )
    #     assert success and "1" in output, "Data not found in restored database"
    #
    #     # Clean up
    #     ssh_connection.execute_command(f"rm -f {dump_file}")
    #     ssh_connection.execute_command(
    #         f"source ~/.bashrc && dropdb -h {config.HOST} -U {config.USERNAME} {restore_db}"
    #     )
    