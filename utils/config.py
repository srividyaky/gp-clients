import os

# Default connection details
DEFAULT_HOST = "10.160.52.227"
DEFAULT_USERNAME = "gpadmin"
DEFAULT_PASSWORD = "deleteme123!"
DEFAULT_PORT = 5432
DEFAULT_DATABASE = "postgres"

# Environment variables will override defaults if set
HOST = os.environ.get("GPDB_HOST", DEFAULT_HOST)
USERNAME = os.environ.get("GPDB_USERNAME", DEFAULT_USERNAME)
PASSWORD = os.environ.get("GPDB_PASSWORD", DEFAULT_PASSWORD)
PORT = int(os.environ.get("GPDB_PORT", DEFAULT_PORT))
DATABASE = os.environ.get("GPDB_DATABASE", DEFAULT_DATABASE)

# Greenplum specific settings
COORDINATOR_DATA_DIRECTORY = "/gpdata/coordinator/gpseg-1/"

# Test data paths
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_data")

# Logging configuration
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "test_run.log")