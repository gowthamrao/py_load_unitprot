"""
Extractor module for py-load-uniprot.

This module is responsible for acquiring data from UniProt's FTP/HTTPS endpoints.
Its key responsibilities include:
- Downloading large data files (e.g., uniprot_sprot.xml.gz) with progress tracking.
- Verifying the integrity of downloaded files using MD5 checksums.
- Fetching and parsing release metadata.
- Handling network errors with retry logic (to be implemented).
"""

import hashlib
import re
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from urllib3.util.retry import Retry

from .config import Settings


class Extractor:
    """
    Handles the extraction of data from UniProt.
    """

    def __init__(self, settings: Settings):
        """
        Initializes the Extractor with application settings.

        Args:
            settings: The application configuration object.
        """
        self.settings = settings
        self._checksums: Optional[Dict[str, str]] = None
        self.session = self._create_retry_session()
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)

    def _create_retry_session(self) -> requests.Session:
        """
        Creates a requests session with a retry strategy.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_progress_bar(self) -> Progress:
        """Returns a pre-configured Rich progress bar."""
        return Progress(
            TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
        )

    def download_file(self, filename: str) -> Path:
        """
        Downloads a file from the UniProt FTP server with progress tracking and support for resumable downloads.
        """
        url = f"{self.settings.urls.uniprot_ftp_base_url}{filename}"
        local_path = self.settings.data_dir / filename
        headers = {}
        file_mode = "wb"
        downloaded_size = 0

        # Check for existing partial file
        if local_path.exists():
            downloaded_size = local_path.stat().st_size
            headers["Range"] = f"bytes={downloaded_size}-"
            file_mode = "ab"
            print(f"Resuming download for {filename} from {downloaded_size} bytes.")
        else:
            print(f"Starting new download for {filename} from {url}")

        try:
            with self.session.get(url, stream=True, headers=headers) as r:
                # Check if the server supports range requests. If not, re-download from scratch.
                if r.status_code not in (200, 206):
                    r.raise_for_status()

                if r.status_code == 200:  # Full download
                    print(
                        "Server does not support range requests. Starting full download."
                    )
                    downloaded_size = 0
                    file_mode = "wb"
                elif r.status_code == 206:  # Partial download
                    print("Server supports range requests. Resuming download.")

                total_size = int(r.headers.get("content-length", 0)) + downloaded_size

                progress = self._get_progress_bar()
                task_id = progress.add_task(
                    "download",
                    total=total_size,
                    completed=downloaded_size,
                    filename=filename,
                )

                with open(local_path, file_mode) as f, progress:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        progress.update(task_id, advance=len(chunk))

            print(f"Successfully downloaded to {local_path}")
            return local_path

        except requests.exceptions.RequestException as e:
            print(f"Error downloading {filename}: {e}")
            raise

    def fetch_checksums(self) -> Dict[str, str]:
        """
        Downloads and parses the MD5 checksums file from UniProt.
        If the file is not found (404), it returns an empty dictionary.

        Returns:
            A dictionary mapping filenames to their MD5 checksums.
        """
        url = f"{self.settings.urls.uniprot_ftp_base_url}{self.settings.urls.checksums_filename}"
        checksum_map = {}

        print(f"Fetching checksums from {url}")
        try:
            response = self.session.get(url)
            if response.status_code == 404:
                print(
                    f"Warning: Checksum file not found at {url}. Skipping checksum verification."
                )
                self._checksums = {}
                return self._checksums

            response.raise_for_status()

            # The file format is: <md5_hash>  <filename>
            for line in response.text.strip().split("\n"):
                match = re.match(r"^\s*([a-f0-9]{32})\s+([\w\.\-\_]+\.gz)\s*$", line)
                if match:
                    checksum, filename = match.groups()
                    checksum_map[filename] = checksum

            self._checksums = checksum_map
            print(f"Successfully fetched and parsed {len(checksum_map)} checksums.")
            return self._checksums

        except requests.exceptions.RequestException as e:
            print(f"Error fetching checksums: {e}")
            # In case of other network errors, we'll also return empty and warn
            print(
                "Warning: Could not fetch checksums due to a network error. Skipping verification."
            )
            self._checksums = {}
            return self._checksums

    def verify_checksum(self, file_path: Path) -> bool:
        """
        Verifies the MD5 checksum of a downloaded file.

        Args:
            file_path: The path to the file to verify.

        Returns:
            True if the checksum is valid, False otherwise.
        """
        if not self._checksums:
            self.fetch_checksums()

        filename = file_path.name
        expected_md5 = None
        if self._checksums:
            expected_md5 = self._checksums.get(filename)

        if not expected_md5:
            print(f"Warning: No checksum found for {filename}. Skipping verification.")
            return True  # Or False, depending on strictness required

        print(f"Verifying checksum for {filename}...")

        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5.update(chunk)

        actual_md5 = md5.hexdigest()

        if actual_md5 == expected_md5:
            print(f"Checksum for {filename} is valid.")
            return True
        else:
            print(f"Error: Checksum mismatch for {filename}.")
            print(f"  Expected: {expected_md5}")
            print(f"  Actual:   {actual_md5}")
            return False

    def get_release_info(self) -> Dict[str, Any]:
        """
        Downloads and parses release notes to get the current version, date,
        and entry counts, then persists this metadata to a JSON file.

        Returns:
            A dictionary with version, date, and entry counts.
        """
        import json
        from datetime import date, datetime

        info: Dict[str, Any] = {}

        # --- Get Version and Date from reldate.txt ---
        reldate_url = f"{self.settings.urls.uniprot_ftp_base_url}{self.settings.urls.release_notes_filename}"
        print(f"Fetching release info from {reldate_url}")
        try:
            response = self.session.get(reldate_url)
            response.raise_for_status()
            match = re.search(r"Release\s+(\S+)\s+of\s+(.*)", response.text)
            if match:
                version, date_str = match.groups()
                info["version"] = version
                # Attempt to parse the date string into a date object
                try:
                    info["date"] = datetime.strptime(date_str, "%d-%b-%Y").date()
                except ValueError:
                    info["date"] = date_str  # Keep as string if parsing fails
            else:
                raise ValueError(
                    "Could not parse release version/date from reldate.txt"
                )
        except requests.exceptions.RequestException as e:
            print(f"Error fetching reldate.txt: {e}")
            raise

        # --- Get Entry Counts from relnotes.txt ---
        relnotes_url = self.settings.urls.relnotes_url
        print(f"Fetching statistics from {relnotes_url}")
        try:
            response = self.session.get(relnotes_url)
            response.raise_for_status()
            # Regex to find the line and capture the numbers
            match = re.search(
                r"UniProtKB/Swiss-Prot:\s+([\d,]+)\s+entries and UniProtKB/TrEMBL:\s+([\d,]+)\s+entries",
                response.text,
            )
            if match:
                # Convert numbers with commas to integers
                info["swissprot_entry_count"] = int(match.group(1).replace(",", ""))
                info["trembl_entry_count"] = int(match.group(2).replace(",", ""))
            else:
                print(
                    "[yellow]Warning: Could not parse entry counts from relnotes.txt. Counts will be set to 0.[/yellow]"
                )
                info["swissprot_entry_count"] = 0
                info["trembl_entry_count"] = 0
        except requests.exceptions.RequestException as e:
            print(
                f"[yellow]Warning: Could not fetch relnotes.txt: {e}. Counts will be set to 0.[/yellow]"
            )
            info["swissprot_entry_count"] = 0
            info["trembl_entry_count"] = 0

        print(f"Found release info: {info}")
        # Persist the metadata
        metadata_path = self.settings.data_dir / "release_metadata.json"
        with open(metadata_path, "w") as f:
            info_copy = info.copy()
            if "date" in info_copy and isinstance(info_copy["date"], date):
                info_copy["date"] = info_copy["date"].isoformat()
            json.dump(info_copy, f, indent=2)
        print(f"Release metadata saved to {metadata_path}")
        return info
