# py_load_uniprot

**py_load_uniprot** is a high-performance, robust, and extensible Python package for Extract, Transform, and Load (ETL) processing of the UniProt Knowledgebase (UniProtKB) into a relational database. It is designed to handle the massive scale of UniProt data efficiently.

## Key Features

- **High-Performance Loading:** Utilizes native database bulk loading (e.g., PostgreSQL `COPY` command) to maximize ingestion speed.
- **Memory-Efficient Parsing:** Employs a streaming XML parser (`lxml.etree.iterparse`) to process massive UniProt XML files (like TrEMBL) with a minimal memory footprint.
- **Parallel Transformation:** Leverages multiprocessing to significantly accelerate the CPU-bound task of transforming XML data into a relational format.
- **Extensible Architecture:** Built using a `DatabaseAdapter` pattern, allowing for future extension to other database systems (e.g., Redshift, BigQuery).
- **Full and Delta Loads:** Supports both full database rebuilds (via an atomic schema swap) and incremental (delta) updates to keep data current with new UniProt releases.
- **Robust Configuration:** Uses Pydantic for type-safe configuration via YAML files and environment variables.
- **Modern Tooling:** Developed with modern Python standards, including a `src` layout, `pdm` for dependency management, and a full suite of code quality tools (`ruff`, `mypy`, `black`).
- **Comprehensive CLI:** Includes a command-line interface built with Typer for all major operations.

## Installation

This project is managed with `pdm`.

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd py_load_uniprot
    ```

2.  **Install dependencies:**
    PDM will create a virtual environment and install all required dependencies from `pdm.lock`.
    ```bash
    pdm install
    ```

## Configuration

The application is configured via a YAML file and/or environment variables.

1.  **Create a configuration file:**
    Copy the example configuration file to create your own:
    ```bash
    cp config.example.yaml config.yaml
    ```

2.  **Edit `config.yaml`:**
    Update the file with your database connection details.

    ```yaml
    # The directory where downloaded UniProt files will be stored.
    data_dir: "data"

    # Your target PostgreSQL database connection settings.
    db:
      host: "your-db-host"
      port: 5432
      user: "your-db-user"
      password: "your-db-password"
      dbname: "uniprot"
    ```

3.  **Environment Variables (Optional):**
    All settings can be overridden by environment variables. The variable name is constructed by prefixing with `PY_LOAD_UNIPROT_`, followed by the section and key separated by double underscores.
    For example:
    ```bash
    export PY_LOAD_UNIPROT_DB__HOST=your-db-host.example.com
    export PY_LOAD_UNIPROT_DB__PASSWORD="a-secure-password"
    ```

## Usage

The primary interface is the CLI. Ensure you have activated the `pdm` environment (`pdm shell`) or are running commands with `pdm run`.

### 1. Initialize the Database (First-Time Setup)

Before the first run, initialize the database schema. This creates the production schema (`uniprot_public`) and the necessary metadata tables. This command is idempotent.

```bash
pdm run py_load_uniprot --config config.yaml initialize
```

### 2. Run the ETL Pipeline

Execute the main ETL process. You can run a `full` load or a `delta` load for `swissprot`, `trembl`, or `all` datasets.

**Example: Full load of Swiss-Prot**
```bash
pdm run py_load_uniprot --config config.yaml run --dataset=swissprot --mode=full
```

**Example: Delta load of all datasets**
```bash
pdm run py_load_uniprot --config config.yaml run --dataset=all --mode=delta
```

### 3. Check Database Status

Check the version of the UniProt release currently loaded in the database.

```bash
pdm run py_load_uniprot --config config.yaml status
```

### 4. Check Configuration

Validate your `config.yaml` file and test the database connection.

```bash
pdm run py_load_uniprot --config config.yaml check-config
```

## Programmatic API

The package can also be used as a library within other Python applications or workflow managers like Airflow. The `PyLoadUniprotPipeline` class provides a high-level interface.

```python
from pathlib import Path
from py_load_uniprot.core import PyLoadUniprotPipeline

try:
    # Create a pipeline instance directly from a configuration file
    config_path = Path("config.yaml")
    pipeline = PyLoadUniprotPipeline.from_config_file(config_path)

    # Run the pipeline
    pipeline.run(mode="delta", dataset="all")

    print("py_load_uniprot pipeline completed successfully.")

except Exception as e:
    print(f"Pipeline failure: {e}")
    # Handle error (e.g., send alert)
```
