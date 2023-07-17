# Ingestion

This component takes care of downloading new data from the KNMI API.

## Dependencies

### Prerequisites

* Python 3

### Development Dependencies

* An API key is required which can be obtained from the KNMI Developer Portal.
  The `donwload_data.sh` script has a dependency on the 1Password CLI to
  retrieve the API key from a personal vault.

# TODO

1. Finish script (see TODOs at top of ingestion.py)
2. Create Airflow DAG
