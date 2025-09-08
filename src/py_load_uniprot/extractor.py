import hashlib
import requests
from pathlib import Path
from rich.progress import (
    Progress,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
)
from rich import print
import re
from datetime import datetime

from py_load_uniprot.config import settings

def get_release_metadata() -> dict[str, any]:
    """
    Fetches and parses the UniProt release notes file (reldate.txt).

    Returns:
        A dictionary containing release metadata (version, date).

    Raises:
        requests.exceptions.RequestException: If the release notes file cannot be downloaded.
        ValueError: If the release notes format is unexpected.
    """
    print(f"Fetching release metadata from [cyan]{settings.release_notes_url}[/cyan]...")
    response = requests.get(settings.release_notes_url)
    response.raise_for_status()
    content = response.text

    # Example reldate.txt content:
    # Release 2024_02 of 21-Feb-2024
    # UniProt Knowledgebase
    # Copyright (...)
    #
    # Swiss-Prot Release 2024_02 of 21-Feb-2024 contains 571168 sequence entries
    # TrEMBL Release 2024_02 of 21-Feb-2024 contains 269075841 sequence entries

    # Use regex to find the main release line
    match = re.search(r"Release (\d{4}_\d{2}) of (\d{2}-\w{3}-\d{4})", content)
    if not match:
        raise ValueError("Could not parse release version and date from reldate.txt")

    release_version = match.group(1)
    release_date_str = match.group(2)
    release_date = datetime.strptime(release_date_str, "%d-%b-%Y").date()

    # Optional: Extract entry counts
    swissprot_match = re.search(r"Swiss-Prot.*contains (\d+) sequence entries", content)
    trembl_match = re.search(r"TrEMBL.*contains (\d+) sequence entries", content)

    swissprot_count = int(swissprot_match.group(1)) if swissprot_match else None
    trembl_count = int(trembl_match.group(1)) if trembl_match else None

    metadata = {
        "release_version": release_version,
        "release_date": release_date,
        "swissprot_entry_count": swissprot_count,
        "trembl_entry_count": trembl_count,
    }
    print(f"[green]Found UniProt Release: {release_version} ({release_date})[/green]")
    return metadata

def get_release_checksums() -> dict[str, str]:
    """
    Fetches and parses the MD5 checksum file from UniProt.

    Returns:
        A dictionary mapping filenames to their expected MD5 checksums.

    Raises:
        requests.exceptions.RequestException: If the checksum file cannot be downloaded.
    """
    print(f"Fetching checksums from [cyan]{settings.checksums_url}[/cyan]...")
    response = requests.get(settings.checksums_url)
    response.raise_for_status()

    checksums = {}
    for line in response.text.strip().split("\n"):
        if line:
            # The format is "MD5_HASH  FILENAME"
            md5_hash, filename = line.strip().split(maxsplit=1)
            # The filenames in the checksum file might have a ./ prefix, so we remove it
            checksums[filename.lstrip('./')] = md5_hash

    return checksums

def calculate_md5(filepath: Path, chunk_size: int = 8192) -> str:
    """
    Calculates the MD5 checksum for a given file.

    Args:
        filepath: The path to the file.
        chunk_size: The size of chunks to read from the file for memory efficiency.

    Returns:
        The MD5 checksum as a hexadecimal string.
    """
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()

def download_uniprot_file(url: str, destination: Path) -> None:
    """
    Downloads a file from a URL to a destination, showing a rich progress bar.

    Args:
        url: The URL of the file to download.
        destination: The local path to save the file.

    Raises:
        requests.exceptions.RequestException: If the file cannot be downloaded.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))

    with Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        "ETA:",
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task(f"[bold green]Downloading {destination.name}[/]", total=total_size)

        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                progress.update(task, advance=len(chunk))

def run_extraction() -> dict[str, any]:
    """
    Orchestrates the entire data extraction process.

    This function ensures the data directory exists, fetches release metadata,
    fetches official checksums, downloads the required UniProt data files, and
    verifies their integrity. It is idempotent.

    Returns:
        A dictionary containing the release metadata.

    Raises:
        RuntimeError: If metadata/checksums cannot be fetched or if a
                      downloaded file has a checksum mismatch.
    """
    print("[bold blue]Starting UniProt data extraction...[/bold blue]")

    # 1. Get release metadata first, as it's the source of truth for the release version
    try:
        release_metadata = get_release_metadata()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise RuntimeError(f"Failed to get release metadata: {e}") from e

    # 2. Ensure data directory exists
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Data will be stored in: [green]{settings.DATA_DIR.resolve()}[/green]")

    # 3. Get official checksums
    try:
        checksums = get_release_checksums()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch checksums from {settings.checksums_url}") from e

    # 4. Define files to download
    files_to_download = {
        "uniprot_sprot.xml.gz": settings.swissprot_xml_url,
        "uniprot_trembl.xml.gz": settings.trembl_xml_url,
    }

    # 4. Download and verify each file
    for filename, url in files_to_download.items():
        destination = settings.DATA_DIR / filename
        print(f"\n[bold]Processing {filename}...[/bold]")

        expected_checksum = checksums.get(filename)
        if not expected_checksum:
            print(f"[yellow]Warning: No checksum found for {filename}. Cannot verify integrity.[/yellow]")

        # Check if file exists and is valid
        if destination.exists():
            print("File already exists. Verifying checksum...")
            local_checksum = calculate_md5(destination)
            if local_checksum == expected_checksum:
                print(f"[green]Checksum for {filename} is correct. Skipping download.[/green]")
                continue
            else:
                print("[yellow]Checksum mismatch. Re-downloading file.[/yellow]")

        # Download the file
        try:
            download_uniprot_file(url, destination)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to download {filename} from {url}") from e

        # Verify checksum after download
        if expected_checksum:
            print("Verifying checksum...")
            local_checksum = calculate_md5(destination)

            if local_checksum == expected_checksum:
                print(f"[green]Successfully downloaded and verified {filename}.[/green]")
            else:
                raise RuntimeError(
                    f"[bold red]Checksum mismatch for {filename}![/bold red]\n"
                    f"Expected: {expected_checksum}\n"
                    f"Got:      {local_checksum}"
                )

    print("\n[bold blue]Extraction process completed successfully.[/bold blue]")
    return release_metadata
