import logging
import os
import subprocess
import tempfile
import csv
from . import config
from .ssh_utils import SSHConnection

logger = logging.getLogger(__name__)


def setup_logging():
    """Set up logging configuration"""
    log_dir = os.path.dirname(config.LOG_FILE)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(config.LOG_FILE),
            logging.StreamHandler()
        ]
    )


def run_local_command(command, env=None):
    """Run a command locally"""
    logger.debug(f"Running local command: {command}")
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env
    )
    stdout, stderr = process.communicate()
    exit_code = process.returncode

    output = stdout.decode('utf-8') if stdout else ""
    error = stderr.decode('utf-8') if stderr else ""

    if exit_code != 0:
        logger.warning(f"Command failed with exit code {exit_code}: {error}")

    return exit_code == 0, output, error


def create_test_database(db_name, ssh_connection=None):
    """Create a test database using createdb"""
    if ssh_connection:
        # Remote execution
        command = f"source ~/.bashrc && createdb -h {config.HOST} -U {config.USERNAME} {db_name}"
        return ssh_connection.execute_command(command)
    else:
        # Local execution
        command = f"createdb -h {config.HOST} -U {config.USERNAME} {db_name}"
        return run_local_command(command)


def drop_test_database(db_name, ssh_connection=None):
    """Drop a test database using dropdb"""
    if ssh_connection:
        # Remote execution
        command = f"source ~/.bashrc && dropdb -h {config.HOST} -U {config.USERNAME} {db_name}"
        return ssh_connection.execute_command(command)
    else:
        # Local execution
        command = f"dropdb -h {config.HOST} -U {config.USERNAME} {db_name}"
        return run_local_command(command)


def create_test_user(username, password, ssh_connection=None):
    """Create a test user using createuser"""
    if ssh_connection:
        # Remote execution
        command = f"source ~/.bashrc && createuser -h {config.HOST} -U {config.USERNAME} -P -s {username}"
        return ssh_connection.execute_command(command)
    else:
        # Local execution
        command = f"createuser -h {config.HOST} -U {config.USERNAME} -P -s {username}"
        return run_local_command(command)


def drop_test_user(username, ssh_connection=None):
    """Drop a test user using dropuser"""
    if ssh_connection:
        # Remote execution
        command = f"source ~/.bashrc && dropuser -h {config.HOST} -U {config.USERNAME} {username}"
        return ssh_connection.execute_command(command)
    else:
        # Local execution
        command = f"dropuser -h {config.HOST} -U {config.USERNAME} {username}"
        return run_local_command(command)


def run_psql_query(query, database=None, ssh_connection=None):
    """Run a SQL query using psql"""
    db = database or config.DATABASE

    if ssh_connection:
        # Remote execution
        command = f"source ~/.bashrc && psql -h {config.HOST} -U {config.USERNAME} -d {db} -c \"{query}\""
        return ssh_connection.execute_command(command)
    else:
        # Local execution
        command = f"psql -h {config.HOST} -U {config.USERNAME} -d {db} -c \"{query}\""
        return run_local_command(command)


def create_test_csv_file(filename, rows):
    """Create a test CSV file for data loading tests"""
    os.makedirs(config.TEST_DATA_DIR, exist_ok=True)
    filepath = os.path.join(config.TEST_DATA_DIR, filename)

    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in rows:
            writer.writerow(row)

    return filepath


def create_gpload_yaml(filename, database, table, csv_file):
    """Create a YAML configuration file for gpload"""
    os.makedirs(config.TEST_DATA_DIR, exist_ok=True)
    filepath = os.path.join(config.TEST_DATA_DIR, filename)

    yaml_content = f"""
---
VERSION: 1.0.0.1
DATABASE: {database}
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
          - {csv_file}
    - FORMAT: CSV
    - DELIMITER: ','
    - QUOTE: '"'
    - ERROR_LIMIT: 25
    - ERROR_TABLE: err_{table}
  OUTPUT:
    - TABLE: {table}
    - MODE: INSERT
"""

    with open(filepath, 'w') as yaml_file:
        yaml_file.write(yaml_content)

    return filepath