"""
This module contains the main ETL pipeline orchestration logic.
"""
import tempfile
import shutil
from pathlib import Path
from rich import print

from py_load_uniprot import extractor, transformer
from py_load_uniprot.config import settings
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
        Executes the full ETL pipeline for a specified dataset and load mode.

        Args:
            dataset: The dataset to load ('swissprot' or 'trembl').
            mode: The load mode ('full' or 'delta').

        Raises:
            ValueError: If the dataset or mode is invalid.
            FileNotFoundError: If the source XML file is not found after extraction.
            Exception: Propagates exceptions from underlying ETL steps.
        """
        print(f"[bold blue]Starting ETL pipeline run...[/bold blue]")
        print(f"Dataset: [cyan]{dataset}[/cyan], Mode: [cyan]{mode}[/cyan]")

        if mode not in ['full', 'delta']:
            raise ValueError(f"Load mode '{mode}' is not valid. Choose 'full' or 'delta'.")

        if dataset not in ['swissprot', 'trembl']:
            raise ValueError(f"Dataset '{dataset}' is not valid. Choose 'swissprot' or 'trembl'.")

        xml_filename = f"uniprot_{dataset}.xml.gz"
        source_xml_path = settings.DATA_DIR / xml_filename
        temp_dir = None

        try:
            # Step 1: Extraction (fetches metadata, downloads data, verifies checksums)
            print("\n[bold]Step 1: Running data extraction...[/bold]")
            release_info = extractor.run_extraction()
            if not source_xml_path.exists():
                raise FileNotFoundError(f"Source file not found after extraction: {source_xml_path}")
            print("[green]Extraction check complete.[/green]")

            # Step 2: Transformation (XML -> TSV.gz)
            print("\n[bold]Step 2: Running data transformation...[/bold]")
            temp_dir = Path(tempfile.mkdtemp(prefix="uniprot_etl_"))
            print(f"Intermediate files will be stored in: {temp_dir}")
            transformer.transform_xml_to_tsv(source_xml_path, temp_dir)
            print("[green]Transformation complete.[/green]")

            # Step 3: Database Load (Staging)
            print("\n[bold]Step 3: Loading data into staging schema...[/bold]")
            self.db_adapter.initialize_schema(mode=mode)

            for table_name in TABLE_LOAD_ORDER:
                file_path = temp_dir / f"{table_name}.tsv.gz"
                if file_path.exists():
                    print(f"Loading {table_name}...")
                    self.db_adapter.bulk_load_intermediate(file_path, table_name)
                else:
                    print(f"[yellow]Warning: No data file for '{table_name}'. Skipping.[/yellow]")
            print("[green]Staging load complete.[/green]")

            # Step 4: Finalize Load (Index, Analyze, Swap/Merge)
            print("\n[bold]Step 4: Finalizing database load...[/bold]")
            self.db_adapter.finalize_load(mode=mode)
            print("[green]Finalization complete.[/green]")

            # Step 5: Update Metadata
            print("\n[bold]Step 5: Updating release metadata...[/bold]")
            self.db_adapter.update_metadata(release_info)
            print("[green]Metadata update complete.[/green]")

            print("\n[bold green]ETL pipeline completed successfully![/bold green]")

        finally:
            # Clean up the temporary directory
            if temp_dir and temp_dir.exists():
                print(f"Cleaning up temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir)
