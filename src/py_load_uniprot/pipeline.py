"""
This module contains the main ETL pipeline orchestration logic.
"""
import tempfile
import shutil
from pathlib import Path
from rich import print
import uuid
import traceback

from py_load_uniprot import extractor, transformer
from py_load_uniprot.config import get_settings
from py_load_uniprot.db_manager import PostgresAdapter, TABLE_LOAD_ORDER


class PyLoadUniprotPipeline:
    """
    Orchestrates the entire UniProt ETL process, from data extraction to database loading.
    This class is designed to be used programmatically, for example in workflow managers.
    """
    def __init__(self):
        """
        Initializes the pipeline and the database adapter.
        """
        print("[bold blue]Pipeline initialized.[/bold blue]")
        self.db_adapter = PostgresAdapter()

    def run(self, dataset: str, mode: str):
        """
        Executes the full ETL pipeline for a specified dataset(s) and load mode.
        Includes comprehensive status logging to the `load_history` table.

        Args:
            dataset: The dataset to load ('swissprot', 'trembl', or 'all').
            mode: The load mode ('full' or 'delta').

        Raises:
            ValueError: If the dataset or mode is invalid.
            Exception: Propagates exceptions from underlying ETL steps after logging.
        """
        run_id = str(uuid.uuid4())
        print(f"[bold blue]Starting ETL pipeline run (ID: {run_id})...[/bold blue]")
        print(f"Dataset: [cyan]{dataset}[/cyan], Mode: [cyan]{mode}[/cyan]")

        try:
            if mode not in ['full', 'delta']:
                raise ValueError(f"Load mode '{mode}' is not valid. Choose 'full' or 'delta'.")

            valid_datasets = ['swissprot', 'trembl', 'all']
            if dataset not in valid_datasets:
                raise ValueError(f"Dataset '{dataset}' is not valid. Choose from {valid_datasets}.")

            # Log the start of the run
            self.db_adapter.log_run_start(run_id, mode, dataset)

            datasets_to_process = ['swissprot', 'trembl'] if dataset == 'all' else [dataset]

            # Step 1: Extraction
            print("\n[bold]Step 1: Running data extraction...[/bold]")
            release_info = extractor.run_extraction()
            print("[green]Extraction complete.[/green]")

            # Step 2: Initialize Schema
            print("\n[bold]Step 2: Initializing database schema...[/bold]")
            self.db_adapter.initialize_schema(mode=mode)
            print("[green]Schema initialization complete.[/green]")

            # Step 3: Transform and Load each dataset
            for ds in datasets_to_process:
                self._transform_and_load_single_dataset(ds)

            # Step 4: Finalize Load
            print("\n[bold]Step 4: Finalizing database load...[/bold]")
            self.db_adapter.finalize_load(mode=mode)
            print("[green]Finalization complete.[/green]")

            # Step 5: Update Metadata
            print("\n[bold]Step 5: Updating release metadata...[/bold]")
            self.db_adapter.update_metadata(release_info)
            print("[green]Metadata update complete.[/green]")

            # If we reach here, the pipeline was successful
            self.db_adapter.log_run_end(run_id, "COMPLETED")
            print("\n[bold green]ETL pipeline completed successfully![/bold green]")

        except Exception as e:
            # Capture the full error traceback for detailed logging
            error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            print(f"[bold red]\nETL pipeline failed: {error_msg}[/bold red]")
            # Log the failure and then re-raise the exception
            self.db_adapter.log_run_end(run_id, "FAILED", error_message=error_msg)
            raise

    def _transform_and_load_single_dataset(self, dataset: str):
        """
        Runs the Transformation and Loading steps for a single dataset.
        This method assumes the staging schema has already been initialized.
        """
        settings = get_settings()
        print(f"\n[bold magenta]Processing dataset: {dataset}...[/bold magenta]")

        xml_filename = f"uniprot_{dataset}.xml.gz"
        source_xml_path = settings.data_dir / xml_filename
        temp_dir = None

        if not source_xml_path.exists():
            raise FileNotFoundError(f"Source file not found for dataset '{dataset}': {source_xml_path}")

        try:
            # Transformation (XML -> TSV.gz)
            print(f"  - Running data transformation for {dataset}...")
            temp_dir = Path(tempfile.mkdtemp(prefix=f"uniprot_{dataset}_"))
            print(f"    Intermediate files will be stored in: {temp_dir}")
            transformer.transform_xml_to_tsv(source_xml_path, temp_dir)
            print(f"  - Transformation complete for {dataset}.")

            # Database Load into Staging
            print(f"  - Loading {dataset} data into staging schema...")
            for table_name in TABLE_LOAD_ORDER:
                file_path = temp_dir / f"{table_name}.tsv.gz"
                if file_path.exists():
                    print(f"    Loading {table_name}...")
                    self.db_adapter.bulk_load_intermediate(file_path, table_name)
                else:
                    print(f"    [yellow]Warning: No data file for '{table_name}'. Skipping.[/yellow]")
            print(f"  - Staging load complete for {dataset}.")

        finally:
            # Clean up the temporary directory for the current dataset
            if temp_dir and temp_dir.exists():
                print(f"Cleaning up temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir)
