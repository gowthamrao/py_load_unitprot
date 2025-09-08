import typer
from rich import print

from py_load_uniprot import extractor

app = typer.Typer(
    name="py-load-uniprot",
    help="A high-performance Python package for ETL processing of UniProtKB data.",
    add_completion=False,
)

@app.command()
def download():
    """
    Downloads UniProtKB data (Swiss-Prot and TrEMBL) and verifies file integrity.

    This command is idempotent. If the files already exist in the data directory
    and their checksums are valid, they will not be re-downloaded.
    """
    print("[bold blue]Initiating UniProt download process...[/bold blue]")
    try:
        extractor.run_extraction()
        print("\n[bold green]CLI command 'download' completed successfully.[/bold green]")
    except Exception as e:
        print(f"\n[bold red]An error occurred during the download process: {e}[/bold red]")
        raise typer.Exit(code=1)

def main():
    app()

if __name__ == "__main__":
    main()
