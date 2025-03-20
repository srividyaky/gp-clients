# Greenplum Client Tools Test Framework

This repository contains automated tests for Greenplum client tools.

## Prerequisites

- Python 3.9+
- Access to a Greenplum database server
- Greenplum client tools installed locally or on a remote server

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/your-username/greenplum-client-tests.git
   cd greenplum-client-tests
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure connection settings:
   
   Either update the defaults in `utils/config.py` or set environment variables:
   ```
   export GPDB_HOST=10.160.52.227
   export GPDB_USERNAME=gpadmin
   export GPDB_PASSWORD=deleteme123!
   export COORDINATOR_DATA_DIRECTORY=/gpdata/coordinator/gpseg-1/
   ```

## Running Tests Locally

### Running all tests:
```
pytest -v
```

### Running specific test categories:
```
# Core tools tests (createdb, dropdb, etc.)
pytest -v tests/test_core_tools.py

# Data loading tools tests (gpload, gpfdist)
pytest -v tests/test_data_load_tools.py

# Monitoring tools tests (gpstate, gpss)
pytest -v tests/test_monitoring_tools.py
```

### Generate HTML report:
```
pytest --html=report.html --self-contained-html
```

## GitHub Actions Integration

This repository includes GitHub Actions workflows that automatically run tests on:
- Every push to the main branch
- Every pull request to the main branch
- Daily scheduled runs

To set up GitHub Actions:

1. Add your Greenplum server credentials as GitHub secrets:
   - `GPDB_HOST`: Your Greenplum host address
   - `GPDB_USERNAME`: Your Greenplum username
   - `GPDB_PASSWORD`: Your Greenplum password

2. Push your code to GitHub and the pipeline will run automatically.

## Test Structure

The test suite is organized as follows:

- `tests/test_core_tools.py`: Tests for basic PostgreSQL-compatible tools (createdb, dropdb, psql, etc.)
- `tests/test_data_load_tools.py`: Tests for data loading tools (gpload, gpfdist)
- `tests/test_kafka_tools.py`: Tests for Kafka integration tools (gpkafka, kafkacat)
- `tests/test_monitoring_tools.py`: Tests for monitoring and admin tools (gpstate, gpss)

## Adding New Tests

To add new tests:

1. Identify the appropriate test file based on the tool category
2. Add a new test method in the corresponding test class
3. Follow the existing patterns for SSH execution, assertions, and cleanup

## Troubleshooting

- **Connection issues**: Verify that your Greenplum server is accessible from the test environment
- **Missing tools**: Ensure all client tools are installed and in the PATH
- **Environment issues**: Check that environment variables are set correctly, especially `COORDINATOR_DATA_DIRECTORY`

## Cross-Platform Testing

The test framework is designed to work across different operating systems:

- For Linux-based tests, the SSH connection approach works well
- For Windows, you may need to adjust command execution patterns in the helper functions