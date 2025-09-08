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
from typing import Dict, Optional

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
            allowed_methods=["HEAD", "GET", "OPTIONS"]
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
        Downloads a file from the UniProt FTP server with progress tracking.

        Args:
            filename: The name of the file to download (e.g., 'uniprot_sprot.xml.gz').

        Returns:
            The path to the downloaded file.
        """
        url = f"{self.settings.urls.uniprot_ftp_base_url}{filename}"
        local_path = self.settings.data_dir / filename

        print(f"Starting download of {filename} from {url}")

        try:
            with self.session.get(url, stream=True) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))

                progress = self._get_progress_bar()
                task_id = progress.add_task("download", total=total_size, filename=filename)

                with open(local_path, 'wb') as f, progress:
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
                print(f"Warning: Checksum file not found at {url}. Skipping checksum verification.")
                self._checksums = {}
                return self._checksums

            response.raise_for_status()

            # The file format is: <md5_hash>  <filename>
            for line in response.text.strip().split('\n'):
                match = re.match(r'^\s*([a-f0-9]{32})\s+([\w\.\-\_]+\.gz)\s*$', line)
                if match:
                    checksum, filename = match.groups()
                    checksum_map[filename] = checksum

            self._checksums = checksum_map
            print(f"Successfully fetched and parsed {len(checksum_map)} checksums.")
            return self._checksums

        except requests.exceptions.RequestException as e:
            print(f"Error fetching checksums: {e}")
            # In case of other network errors, we'll also return empty and warn
            print(f"Warning: Could not fetch checksums due to a network error. Skipping verification.")
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
        expected_md5 = self._checksums.get(filename)

        if not expected_md5:
            print(f"Warning: No checksum found for {filename}. Skipping verification.")
            return True # Or False, depending on strictness required

        print(f"Verifying checksum for {filename}...")

        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
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

    def get_release_info(self) -> Dict[str, str]:
        """
        Downloads and parses release notes to get the current version and date,
        then persists this metadata to a JSON file.

        Returns:
            A dictionary with 'version' and 'date'.
        """
        import json
        url = f"{self.settings.urls.uniprot_ftp_base_url}{self.settings.urls.release_notes_filename}"
        print(f"Fetching release info from {url}")

        try:
            response = self.session.get(url)
            response.raise_for_status()

            # Example format:
            # Release 2025_09 of 08-Sep-2025
            match = re.search(r"Release\s+(\S+)\s+of\s+(.*)", response.text)
            if match:
                version, date = match.groups()
                info = {'version': version, 'date': date}
                print(f"Found release info: {info}")

                # Persist the metadata
                metadata_path = self.settings.data_dir / "release_metadata.json"
                with open(metadata_path, 'w') as f:
                    json.dump(info, f, indent=2)
                print(f"Release metadata saved to {metadata_path}")

                return info
            else:
                raise ValueError("Could not parse release information from reldate.txt")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching release info: {e}")
            raise
