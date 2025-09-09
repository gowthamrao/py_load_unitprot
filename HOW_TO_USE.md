# How to Use py-load-uniprot

This guide provides a detailed walkthrough of the `py-load-uniprot` package, a powerful tool for loading UniProt data into a PostgreSQL database. Here, you will find step-by-step instructions and examples for the most common tasks:

*   **Sourcing Data:** How to download the latest UniProt releases.
*   **Full Load:** How to build a new database from scratch.
*   **Incremental Load:** How to update your database with the latest changes from UniProt.

By the end of this guide, you will be able to confidently use `py-load-uniprot` to manage your UniProt database.

## 1. Sourcing the Data

The first step in any ETL process is to get the data. `py-load-uniprot` simplifies this process with a dedicated `download` command. This command fetches the specified UniProt datasets from the official UniProt FTP server, verifies their integrity using checksums, and stores them in a local directory.

### Configuration

Before you can download the data, you need to tell `py-load-uniprot` where to store it. This is done through the `data_dir` setting in your `config.yaml` file.

```yaml
# config.yaml
data_dir: "data"
```

This configuration tells the tool to save all downloaded files into a `data` directory relative to your project's root. You can change this to any path you prefer.

### Usage

To download the data, use the `download` command, specifying which dataset you want. You have three choices:

*   `swissprot`: Downloads the manually annotated and reviewed Swiss-Prot dataset.
*   `trembl`: Downloads the computationally annotated TrEMBL dataset.
*   `all`: Downloads both Swiss-Prot and TrEMBL.

**Example: Downloading Swiss-Prot**

```bash
pdm run py_load_uniprot --config config.yaml download --dataset=swissprot
```

**Example: Downloading all datasets**

```bash
pdm run py_load_uniprot --config config.yaml download --dataset=all
```

The tool will display the current UniProt release version and show the progress of the download and verification steps. If the files are already present and verified, the tool will skip the download to save time and bandwidth.

## 2. How to Perform a Full Load

A "full load" is the process of building your UniProt database from scratch. This is the mode you will use the first time you populate your database, or whenever you want to completely refresh the data. `py-load-uniprot` is designed to make this process both safe and efficient.

### The Atomic Schema Swap

To ensure zero downtime and data integrity, `py-load-uniprot` performs a full load using an "atomic schema swap" strategy. Here's how it works:

1.  **Load into a Staging Schema:** Instead of writing directly to the main `uniprot_public` schema, the tool creates a temporary, "staging" schema (e.g., `uniprot_staging_2024_03`).
2.  **ETL Process:** All the UniProt data is extracted, transformed, and loaded into the tables within this staging schema.
3.  **Atomic Swap:** Once the load is complete and all data has been verified, the tool performs a series of `ALTER SCHEMA ... RENAME TO` commands within a single transaction. This atomically renames the old `uniprot_public` schema to an archive name and renames the new staging schema to `uniprot_public`.

This approach ensures that your application can continue to query the old data without interruption while the new data is being loaded. The switchover is instantaneous.

### Step-by-Step Guide

#### Step 1: Initialize the Database

Before you can run a full load, you must initialize the database. This is a one-time setup that creates the main `uniprot_public` schema and some metadata tables that the pipeline needs.

```bash
pdm run py_load_uniprot --config config.yaml initialize
```

This command is idempotent, meaning you can run it multiple times without causing any harm.

#### Step 2: Run the Full Load

Once the database is initialized, you can run the full load using the `run` command with `--mode=full`.

**Example: Full load of the Swiss-Prot dataset**

```bash
pdm run py_load_uniprot --config config.yaml run --dataset=swissprot --mode=full
```

This command will:
1.  Download the Swiss-Prot data (if not already present).
2.  Create a new staging schema.
3.  Load the data into the staging schema.
4.  Perform the atomic swap to make the new data live.
5.  Clean up the old schema.

## 3. How to Perform an Incremental Load

An "incremental load" (or "delta load") is the process of updating your existing database with the latest changes from a new UniProt release. This is much faster than a full load and is the recommended way to keep your database up-to-date.

### How it Works

The incremental load process is designed to be both efficient and robust. It identifies new, updated, and deprecated entries in the latest UniProt release and applies these changes to your database.

1.  **Identify Changes:** The tool compares the new UniProt data file with the existing data in your database to identify what has changed.
2.  **Apply Changes:**
    *   **New Entries:** New protein entries are inserted into the database.
    *   **Updated Entries:** Existing entries that have been modified are updated.
    *   **Deprecated Entries:** Entries that are no longer in the new release are handled according to UniProt's deprecation policies.
3.  **Transactional Updates:** All changes are applied within a transaction to ensure that the database remains in a consistent state.

### Usage

To perform an incremental load, you use the same `run` command as for a full load, but with `--mode=delta`.

**Example: Incremental load of all datasets**

```bash
pdm run py_load_uniprot --config config.yaml run --dataset=all --mode=delta
```

This command will:
1.  Download the latest Swiss-Prot and TrEMBL datasets (if not already present).
2.  Compare the new data with the existing data in the `uniprot_public` schema.
3.  Apply any new and updated entries.
4.  Update the release version metadata in the database.

## 4. Complete Example Workflow

This section provides a complete, end-to-end workflow for setting up and maintaining a UniProt database using `py-load-uniprot`.

### Prerequisites

*   You have cloned the repository and installed the dependencies with `pdm install`.
*   You have a running PostgreSQL server.

### Step 1: Configure the Application

1.  **Copy the example configuration:**

    ```bash
    cp config.example.yaml config.yaml
    ```

2.  **Edit `config.yaml` with your database details:**

    ```yaml
    # config.yaml
    data_dir: "data"

    db:
      host: "localhost"
      port: 5432
      user: "your_user"
      password: "your_password"
      dbname: "uniprot"
    ```

### Step 2: Check Configuration and Connectivity

Before running the pipeline, it's a good practice to validate your configuration and test the database connection.

```bash
pdm run py_load_uniprot --config config.yaml check-config
```

If everything is configured correctly, you will see a success message with your settings and a confirmation of database connectivity.

### Step 3: Initialize the Database

Prepare the database for the first load.

```bash
pdm run py_load_uniprot --config config.yaml initialize
```

### Step 4: Perform the Initial Full Load

Load the Swiss-Prot dataset into your database for the first time.

```bash
pdm run py_load_uniprot --config config.yaml run --dataset=swissprot --mode=full
```

This process may take some time, depending on the size of the dataset and the performance of your hardware.

### Step 5: Check the Database Status

After the load is complete, you can check the status to see the loaded UniProt release version.

```bash
pdm run py_load_uniprot --config config.yaml status
```

You should see output similar to this:

```
[bold]Currently loaded UniProt Release Version:[/bold] [green]2024_03[/green]
```

### Step 6: Perform an Incremental Load (Simulating a Future Update)

When a new UniProt release becomes available, you can update your database with an incremental load. The process is the same as the initial load, but with `--mode=delta`.

```bash
pdm run py_load_uniprot --config config.yaml run --dataset=swissprot --mode=delta
```

This will download the new data, compare it with your existing database, and apply only the changes. This is significantly faster than performing another full load.
