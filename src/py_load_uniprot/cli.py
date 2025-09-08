import typer
from rich import print
from pathlib import Path
from typing_extensions import Annotated
import json

from py_load_uniprot import extractor
from py_load_uniprot.config import initialize_settings, get_settings, Settings
from py_load_uniprot.db_manager import PostgresAdapter
from py_load_uniprot.pipeline import PyLoadUniprotPipeline

app = typer.Typer(
    name="py-load-uniprot",
    help="A high-performance Python package for ETL processing of UniProtKB data.",
    add_completion=False,
)

# This callback will run before any command, initializing the settings
@app.callback(invoke_without_command=True)
def main_callback(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="Path to a YAML configuration file.",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
):
    """
    Main entrypoint for the CLI. Initializes configuration.
    """
    try:
        initialize_settings(config_file=config)
    except FileNotFoundError as e:
        print(f"[bold red]Configuration Error: {e}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        print(f"[bold red]An unexpected error occurred during initialization: {e}[/bold red]")
        raise typer.Exit(code=1)


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
    dataset: str = typer.Option("swissprot", help="Dataset to load ('swissprot', 'trembl', or 'all')."),
    mode: str = typer.Option("full", help="Load mode ('full' or 'delta')."),
):
    """
    Run the full ETL pipeline for a specified dataset and load mode.
    """
    try:
        pipeline = PyLoadUniprotPipeline()
        pipeline.run(dataset=dataset, mode=mode)
    except (ValueError, FileNotFoundError) as e:
        print(f"\n[bold red]Configuration Error: {e}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        print(f"\n[bold red]An unexpected error occurred during the ETL pipeline: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        raise typer.Exit(code=1)


@app.command()
def check_config():
    """
    Validates the current configuration and checks database connectivity.
    """
    print("[bold blue]Checking configuration and connectivity...[/bold blue]")
    try:
        settings = get_settings()
        # 1. Print configuration
        print("\n[bold]Current Settings:[/bold]")

        # Use Pydantic's serialization and custom logic to mask the password
        settings_dict = settings.model_dump()
        if 'db' in settings_dict and 'password' in settings_dict['db']:
            settings_dict['db']['password'] = '***'

        # Pretty print the JSON
        print(json.dumps(settings_dict, indent=2, default=str))


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
