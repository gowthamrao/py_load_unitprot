import typer
from rich import print
import tempfile
from pathlib import Path
import shutil

from py_load_uniprot import extractor, transformer
from py_load_uniprot.config import settings
from py_load_uniprot.db_manager import PostgresAdapter, TABLE_LOAD_ORDER

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
    mode: str = typer.Option("full", help="Load mode ('full' or 'delta')."),
):
    """
    Run the full ETL pipeline for a specified dataset and load mode.
    """
    print(f"[bold blue]Starting ETL pipeline run...[/bold blue]")
    print(f"Dataset: [cyan]{dataset}[/cyan], Mode: [cyan]{mode}[/cyan]")

    if mode not in ['full', 'delta']:
        print(f"[bold red]Error: Load mode '{mode}' is not valid. Choose 'full' or 'delta'.[/bold red]")
        raise typer.Exit(code=1)

    if dataset not in ['swissprot', 'trembl']:
        print(f"[bold red]Error: Dataset '{dataset}' is not valid. Choose 'swissprot' or 'trembl'.[/bold red]")
        raise typer.Exit(code=1)

    xml_filename = f"uniprot_{dataset}.xml.gz"
    source_xml_path = settings.DATA_DIR / xml_filename

    db_adapter = PostgresAdapter()
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
        db_adapter.initialize_schema(mode=mode)

        for table_name in TABLE_LOAD_ORDER:
            file_path = temp_dir / f"{table_name}.tsv.gz"
            if file_path.exists():
                print(f"Loading {table_name}...")
                db_adapter.bulk_load_intermediate(file_path, table_name)
            else:
                print(f"[yellow]Warning: No data file for '{table_name}'. Skipping.[/yellow]")
        print("[green]Staging load complete.[/green]")

        # Step 4: Finalize Load (Index, Analyze, Swap/Merge)
        print("\n[bold]Step 4: Finalizing database load...[/bold]")
        db_adapter.finalize_load(mode=mode)
        print("[green]Finalization complete.[/green]")

        # Step 5: Update Metadata
        print("\n[bold]Step 5: Updating release metadata...[/bold]")
        db_adapter.update_metadata(release_info)
        print("[green]Metadata update complete.[/green]")

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


@app.command()
def check_config():
    """
    Validates the current configuration and checks database connectivity.
    """
    print("[bold blue]Checking configuration and connectivity...[/bold blue]")
    try:
        # 1. Print configuration
        print("\n[bold]Current Settings:[/bold]")
        settings_dict = settings.model_dump()
        if 'db_connection_string' in settings_dict:
            # Mask the password for security
            settings_dict['db_connection_string'] = 'postgresql://user:***@host:port/dbname'

        for key, value in settings_dict.items():
            print(f"  - {key}: [cyan]{value}[/cyan]")

        # 2. Check database connection
        print("\n[bold]Checking database connectivity...[/bold]")
        db_adapter = PostgresAdapter()
        db_adapter.check_connection()
        print("[green]Database connection successful.[/green]")

        print("\n[bold green]Configuration and connectivity check passed.[/bold green]")

    except Exception as e:
        print(f"\n[bold red]An error occurred during the check: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        raise typer.Exit(code=1)


@app.command()
def initialize():
    """
    Initializes the production database schema (uniprot_public) for first-time setup.
    This command is idempotent and will not harm an existing schema.
    """
    print("[bold blue]Initializing database schema for first-time use...[/bold blue]")
    try:
        db_adapter = PostgresAdapter()
        db_adapter.create_production_schema()
        print("\n[bold green]CLI command 'initialize' completed successfully.[/bold green]")
        print(f"Production schema '{db_adapter.production_schema}' is ready.")
    except Exception as e:
        print(f"\n[bold red]An error occurred during schema initialization: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def status():
    """
    Checks and displays the currently loaded UniProt release version in the database.
    """
    print("[bold blue]Checking database status...[/bold blue]")
    try:
        db_adapter = PostgresAdapter()
        version = db_adapter.get_current_release_version()
        if version:
            print(f"  [bold]Currently loaded UniProt Release Version:[/bold] [green]{version}[/green]")
        else:
            print("  [yellow]No UniProt release is currently loaded in the database.[/yellow]")
    except Exception as e:
        print(f"\n[bold red]An error occurred while checking the status: {e}[/bold red]")
        raise typer.Exit(code=1)

def main():
    app()

if __name__ == "__main__":
    main()
