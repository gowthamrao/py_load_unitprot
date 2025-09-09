import json
from pathlib import Path
from typing import Optional

import typer
from rich import print
from rich.markup import escape
from typing_extensions import Annotated

from py_load_uniprot import extractor
from py_load_uniprot.config import load_settings
from py_load_uniprot.core import PyLoadUniprotPipeline
from py_load_uniprot.db_manager import PostgresAdapter

app = typer.Typer(
    name="py-load-uniprot",
    help="A high-performance Python package for ETL processing of UniProtKB data.",
    add_completion=False,
)

# Create a common context object to hold the config file path
class AppContext:
    def __init__(self, config_file: Optional[Path]):
        self.config_file = config_file

@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    config: Annotated[
        Optional[Path],
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
) -> None:
    """
    Main entrypoint for the CLI. Manages the configuration context.
    """
    ctx.obj = AppContext(config_file=config)


@app.command()
def download(
    ctx: typer.Context,
    dataset: str = typer.Option(
        "swissprot", help="Dataset to download ('swissprot', 'trembl', or 'all')."
    ),
) -> None:
    """
    Downloads a specified UniProtKB dataset(s) and verifies file integrity.
    """
    print(f"[bold blue]Initiating download for '{dataset}' dataset(s)...[/bold blue]")

    valid_datasets = ["swissprot", "trembl"]
    datasets_to_download = []

    if dataset == "all":
        datasets_to_download = valid_datasets
    elif dataset in valid_datasets:
        datasets_to_download.append(dataset)
    else:
        print(
            f"[bold red]Error: Invalid dataset '{dataset}'. Choose 'swissprot', 'trembl', or 'all'.[/bold red]"
        )
        raise typer.Exit(code=1)

    try:
        settings = load_settings(config_file=ctx.obj.config_file)
        data_extractor = extractor.Extractor(settings)
        failed_downloads = []

        # Fetch release info and checksums once
        release_info = data_extractor.get_release_info()
        print(
            f"Downloading for UniProt Release: {release_info['version']} ({release_info.get('date', 'N/A')})"
        )
        data_extractor.fetch_checksums()

        for ds in datasets_to_download:
            print(f"\n[bold]----- Processing {ds} ----- [/bold]")
            filename = f"uniprot_{'sprot' if ds == 'swissprot' else 'trembl'}.xml.gz"

            try:
                file_path = data_extractor.download_file(filename)
                is_valid = data_extractor.verify_checksum(file_path)

                if is_valid:
                    print(
                        f"[bold green]'{ds}' downloaded and verified successfully.[/bold green]"
                    )
                else:
                    print(
                        f"[bold red]Checksum verification failed for '{ds}'.[/bold red]"
                    )
                    failed_downloads.append(ds)
            except Exception as e:
                print(
                    f"[bold red]An error occurred while downloading {ds}: {escape(str(e))}[/bold red]"
                )
                failed_downloads.append(ds)

        if failed_downloads:
            print(
                f"\n[bold red]Download process finished with errors. Failed datasets: {', '.join(failed_downloads)}[/bold red]"
            )
            raise typer.Exit(code=2)
        else:
            print(
                "\n[bold green]All specified datasets downloaded successfully.[/bold green]"
            )

    except FileNotFoundError as e:
        print(f"\n[bold red]Configuration Error: {escape(str(e))}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        print(
            f"\n[bold red]An unexpected error occurred during the download process: {escape(str(e))}[/bold red]"
        )
        import traceback

        traceback.print_exc()
        raise typer.Exit(code=1)


@app.command()
def run(
    ctx: typer.Context,
    dataset: str = typer.Option(
        "swissprot", help="Dataset to load ('swissprot', 'trembl', or 'all')."
    ),
    mode: str = typer.Option("full", help="Load mode ('full' or 'delta')."),
) -> None:
    """
    Run the full ETL pipeline for a specified dataset and load mode.
    """
    try:
        settings = load_settings(config_file=ctx.obj.config_file)
        pipeline = PyLoadUniprotPipeline(settings)
        pipeline.run(dataset=dataset, mode=mode)
    except (ValueError, FileNotFoundError) as e:
        print(f"\n[bold red]Configuration Error: {escape(str(e))}[/bold red]")
        raise typer.Exit(code=1)
    except Exception as e:
        print(
            f"\n[bold red]An unexpected error occurred during the ETL pipeline: {escape(str(e))}[/bold red]"
        )
        import traceback

        traceback.print_exc()
        raise typer.Exit(code=1)


@app.command()
def check_config(ctx: typer.Context) -> None:
    """
    Validates the current configuration and checks database connectivity.
    """
    print("[bold blue]Checking configuration and connectivity...[/bold blue]")
    try:
        settings = load_settings(config_file=ctx.obj.config_file)
        # 1. Print configuration
        print("\n[bold]Current Settings:[/bold]")

        # Use Pydantic's serialization and custom logic to mask the password
        settings_dict = settings.model_dump()
        if "db" in settings_dict and "password" in settings_dict["db"]:
            settings_dict["db"]["password"] = "***"

        # Pretty print the JSON
        print(json.dumps(settings_dict, indent=2, default=str))

        # 2. Check database connection
        print("\n[bold]Checking database connectivity...[/bold]")
        db_adapter = PostgresAdapter(settings)
        db_adapter.check_connection()
        print("[green]Database connection successful.[/green]")

        print("\n[bold green]Configuration and connectivity check passed.[/bold green]")

    except Exception as e:
        print(
            f"\n[bold red]An error occurred during the check: {escape(str(e))}[/bold red]"
        )
        import traceback

        traceback.print_exc()
        raise typer.Exit(code=1)


@app.command()
def initialize(ctx: typer.Context) -> None:
    """
    Initializes the production database schema (uniprot_public) for first-time setup.
    This command is idempotent and will not harm an existing schema.
    """
    print("[bold blue]Initializing database schema for first-time use...[/bold blue]")
    try:
        settings = load_settings(config_file=ctx.obj.config_file)
        db_adapter = PostgresAdapter(settings)
        db_adapter.create_production_schema()
        print(
            "\n[bold green]CLI command 'initialize' completed successfully.[/bold green]"
        )
        print(f"Production schema '{db_adapter.production_schema}' is ready.")
    except Exception as e:
        print(
            f"\n[bold red]An error occurred during schema initialization: {escape(str(e))}[/bold red]"
        )
        raise typer.Exit(code=1)


@app.command()
def status(ctx: typer.Context) -> None:
    """
    Checks and displays the currently loaded UniProt release version in the database.
    """
    print("[bold blue]Checking database status...[/bold blue]")
    try:
        settings = load_settings(config_file=ctx.obj.config_file)
        db_adapter = PostgresAdapter(settings)
        version = db_adapter.get_current_release_version()
        if version:
            print(
                f"  [bold]Currently loaded UniProt Release Version:[/bold] [green]{version}[/green]"
            )
        else:
            print(
                "  [yellow]No UniProt release is currently loaded in the database.[/yellow]"
            )
    except Exception as e:
        print(
            f"\n[bold red]An error occurred while checking the status: {escape(str(e))}[/bold red]"
        )
        raise typer.Exit(code=1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
