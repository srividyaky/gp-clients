import pytest
import os
import uuid
import logging
from utils.ssh_utils import SSHConnection
from utils.helpers import setup_logging
from utils import config

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def ssh_connection():
    """Provide an SSH connection to the Greenplum server"""
    connection = SSHConnection()
    if not connection.connect():
        pytest.fail("Failed to establish SSH connection to Greenplum server")

    # Check if Greenplum is running
    if not connection.check_greenplum_status():
        pytest.fail("Greenplum database is not running")

    yield connection

    # Close connection at the end of the session
    connection.close()


@pytest.fixture(scope="function")
def unique_name():
    """Generate a unique name for test objects"""
    return f"test_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="function")
def test_database(ssh_connection, unique_name):
    """Create and drop a test database for each test"""
    db_name = f"db_{unique_name}"

    # Create database
    success, output, error = ssh_connection.execute_command(
        f"source ~/.bashrc && createdb -h {config.HOST} -U {config.USERNAME} {db_name}"
    )

    if not success:
        pytest.fail(f"Failed to create test database: {error}")

    yield db_name

    # Drop database after test
    ssh_connection.execute_command(
        f"source ~/.bashrc && dropdb -h {config.HOST} -U {config.USERNAME} {db_name}"
    )


@pytest.fixture(scope="function")
def test_user(ssh_connection, unique_name):
    """Create and drop a test user for each test"""
    username = f"user_{unique_name}"
    password = "Test123!"

    # Create user
    success, output, error = ssh_connection.execute_command(
        f"source ~/.bashrc && echo '{password}\\n{password}' | createuser -h {config.HOST} -U {config.USERNAME} -P {username}"
    )

    if not success:
        pytest.fail(f"Failed to create test user: {error}")

    yield username, password

    # Drop user after test
    ssh_connection.execute_command(
        f"source ~/.bashrc && dropuser -h {config.HOST} -U {config.USERNAME} {username}"
    )


@pytest.fixture(scope="function")
def test_table(ssh_connection, test_database, unique_name):
    """Create and drop a test table for each test"""
    table_name = f"table_{unique_name}"

    # Create table
    query = f"CREATE TABLE {table_name} (id INT, name VARCHAR(50), value FLOAT)"
    success, output, error = ssh_connection.execute_command(
        f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -c \"{query}\""
    )

    if not success:
        pytest.fail(f"Failed to create test table: {error}")

    yield test_database, table_name

    # Drop table after test
    ssh_connection.execute_command(
        f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {test_database} -c \"DROP TABLE IF EXISTS {table_name}\""
    )


@pytest.fixture(scope="function")
def test_csv_file(unique_name):
    """Create a temporary CSV file for testing"""
    filename = f"test_data_{unique_name}.csv"
    filepath = os.path.join(config.TEST_DATA_DIR, filename)

    # Create directory if it doesn't exist
    os.makedirs(config.TEST_DATA_DIR, exist_ok=True)

    # Create CSV file with test data
    with open(filepath, 'w') as f:
        f.write("1,John,100.5\n")
        f.write("2,Jane,200.75\n")
        f.write("3,Bob,300.25\n")

    yield filepath

    # Clean up after test
    if os.path.exists(filepath):
        os.remove(filepath)