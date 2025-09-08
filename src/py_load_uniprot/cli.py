import typer
from rich import print
import tempfile
from pathlib import Path
import shutil

from py_load_uniprot import extractor, transformer
from py_load_uniprot.config import settings
from py_load_uniprot.db_manager import PostgresAdapter

app = typer.Typer(
    name="py-load-uniprot",
    help="A high-performance Python package for ETL processing of UniProtKB data.",
    add_completion=False,
)

@app.command()
def download():
    """
    Downloads UniProtKB data (Swiss-Prot and TrEMBL) and verifies file integrity.
    This command is idempotent.
    """
    print("[bold blue]Initiating UniProt download process...[/bold blue]")
    try:
        extractor.run_extraction()
        print("\n[bold green]CLI command 'download' completed successfully.[/bold green]")
    except Exception as e:
        print(f"\n[bold red]An error occurred during the download process: {e}[/bold red]")
        raise typer.Exit(code=1)

@app.command()
def run(
    dataset: str = typer.Option("swissprot", help="Dataset to load ('swissprot' or 'trembl')."),
    mode: str = typer.Option("full", help="Load mode ('full' or 'delta'). Delta not implemented yet."),
):
    """
    Run the full ETL pipeline for a specified dataset and load mode.
    """
    print(f"[bold blue]Starting ETL pipeline run...[/bold blue]")
    print(f"Dataset: [cyan]{dataset}[/cyan], Mode: [cyan]{mode}[/cyan]")

    if mode != 'full':
        print(f"[bold red]Error: Load mode '{mode}' is not yet implemented.[/bold red]")
        raise typer.Exit(code=1)

    if dataset not in ['swissprot', 'trembl']:
        print(f"[bold red]Error: Dataset '{dataset}' is not valid. Choose 'swissprot' or 'trembl'.[/bold red]")
        raise typer.Exit(code=1)

    xml_filename = f"uniprot_{dataset}.xml.gz"
    source_xml_path = settings.DATA_DIR / xml_filename

    db_adapter = PostgresAdapter()
    temp_dir = None

    try:
        # Step 1: Extraction (ensures file is downloaded and valid)
        print("\n[bold]Step 1: Running data extraction...[/bold]")
        extractor.run_extraction() # This is idempotent, so it's safe to run
        if not source_xml_path.exists():
            raise FileNotFoundError(f"Source file not found after extraction: {source_xml_path}")
        print("[green]Extraction check complete.[/green]")

        # Step 2: Transformation
        print("\n[bold]Step 2: Running data transformation...[/bold]")
        temp_dir = Path(tempfile.mkdtemp(prefix="uniprot_etl_"))
        print(f"Intermediate files will be stored in: {temp_dir}")
        transformer.transform_xml_to_tsv(source_xml_path, temp_dir)
        print("[green]Transformation complete.[/green]")

        # Step 3: Database Load
        print("\n[bold]Step 3: Running database load...[/bold]")
        # 3a: Initialize staging schema
        db_adapter.initialize_schema()
        # 3b: Load intermediate data
        db_adapter.load_transformed_data(temp_dir)
        # 3c: Finalize (index and swap)
        db_adapter.finalize_load(mode=mode)
        print("[green]Database load complete.[/green]")

        print("\n[bold green]ETL pipeline completed successfully![/bold green]")

    except Exception as e:
        print(f"\n[bold red]An error occurred during the ETL pipeline: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        raise typer.Exit(code=1)
    finally:
        # Clean up the temporary directory
        if temp_dir and temp_dir.exists():
            print(f"Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)

def main():
    app()

if __name__ == "__main__":
    main()
