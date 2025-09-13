"""
Unit tests for the Extractor module.
"""

import datetime
import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from py_load_uniprot.config import Settings, load_settings
from py_load_uniprot.extractor import Extractor


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """A fixture to create a temporary data directory."""
    return tmp_path / "data"


@pytest.fixture
def settings(temp_data_dir: Path) -> Settings:
    """A fixture to provide a Settings object for tests."""
    settings = load_settings()
    settings.data_dir = temp_data_dir
    return settings


@pytest.fixture
def extractor(settings: Settings) -> Extractor:
    """A fixture to provide an Extractor instance."""
    return Extractor(settings)


def test_extractor_initialization(settings: Settings):
    """Test that the Extractor initializes correctly and creates the data directory."""
    assert not settings.data_dir.exists()
    Extractor(settings)
    assert settings.data_dir.exists()


@patch("requests.Session.get")
def test_download_file_success(
    mock_get: MagicMock, extractor: Extractor, settings: Settings
):
    """Test successful file download."""
    # --- Arrange ---
    filename = "test.xml.gz"
    file_content = b"gzip compressed data"
    url = f"{extractor.settings.urls.uniprot_ftp_base_url}{filename}"

    # Mock the response from requests.get
    mock_response = MagicMock()
    mock_response.headers.get.return_value = str(len(file_content))
    mock_response.iter_content.return_value = [file_content]
    mock_response.raise_for_status.return_value = None
    mock_get.return_value.__enter__.return_value = mock_response

    # --- Act ---
    downloaded_path = extractor.download_file(filename)

    # --- Assert ---
    mock_get.assert_called_once_with(url, stream=True, headers={})
    mock_response.raise_for_status.assert_called_once()

    assert downloaded_path == settings.data_dir / filename
    assert downloaded_path.read_bytes() == file_content


@patch("requests.Session.get")
def test_download_file_http_error(mock_get: MagicMock, extractor: Extractor):
    """Test that download_file raises an exception on HTTP error."""
    # --- Arrange ---
    mock_get.side_effect = requests.exceptions.RequestException("Test error")

    # --- Act & Assert ---
    with pytest.raises(requests.exceptions.RequestException):
        extractor.download_file("anyfile.gz")


@patch("requests.Session.get")
def test_fetch_checksums_success(mock_get: MagicMock, extractor: Extractor):
    """Test successfully fetching and parsing checksums."""
    # --- Arrange ---
    checksum_content = (
        "d41d8cd98f00b204e9800998ecf8427e  uniprot_sprot.xml.gz\n"
        "098f6bcd4621d373cade4e832627b4f6  uniprot_trembl.xml.gz\n"
    )
    mock_response = MagicMock()
    mock_response.text = checksum_content
    mock_response.raise_for_status.return_value = None
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    # --- Act ---
    checksums = extractor.fetch_checksums()

    # --- Assert ---
    assert len(checksums) == 2
    assert checksums["uniprot_sprot.xml.gz"] == "d41d8cd98f00b204e9800998ecf8427e"
    assert extractor._checksums is not None


def test_verify_checksum_valid(extractor: Extractor, temp_data_dir: Path):
    """Test checksum verification for a valid file."""
    # --- Arrange ---
    filename = "test_file.gz"
    content = b"some data"
    expected_md5 = hashlib.md5(content).hexdigest()

    file_path = temp_data_dir / filename
    file_path.write_bytes(content)

    # Pre-populate the checksums
    extractor._checksums = {filename: expected_md5}

    # --- Act & Assert ---
    assert extractor.verify_checksum(file_path) is True


def test_verify_checksum_invalid(extractor: Extractor, temp_data_dir: Path):
    """Test checksum verification for an invalid file."""
    # --- Arrange ---
    filename = "test_file.gz"
    file_path = temp_data_dir / filename
    file_path.write_bytes(b"some data")

    extractor._checksums = {filename: "invalid_checksum"}

    # --- Act & Assert ---
    assert extractor.verify_checksum(file_path) is False


def test_verify_checksum_not_found(extractor: Extractor, temp_data_dir: Path):
    """Test checksum verification when no checksum is available for the file."""
    # --- Arrange ---
    file_path = temp_data_dir / "another_file.gz"
    file_path.write_bytes(b"some data")
    extractor._checksums = {"some_other_file.gz": "some_hash"}

    # --- Act & Assert ---
    # Should return True and print a warning
    assert extractor.verify_checksum(file_path) is True


@patch("requests.Session.get")
def test_get_release_info_success(
    mock_get: MagicMock, extractor: Extractor, settings: Settings
):
    """Test successfully parsing release info and persisting it."""
    # --- Arrange ---
    release_content = "Release 2025_09 of 08-Sep-2025"
    mock_response = MagicMock()
    mock_response.text = release_content
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    # --- Act ---
    info = extractor.get_release_info()

    # --- Assert ---
    assert info["version"] == "2025_09"
    assert info["date"] == datetime.date(2025, 9, 8)

    # Check that the metadata file was created and is correct
    metadata_path = settings.data_dir / "release_metadata.json"
    assert metadata_path.exists()
    with open(metadata_path, "r") as f:
        persisted_info = json.load(f)
    assert persisted_info["version"] == "2025_09"
    assert persisted_info["date"] == "2025-09-08"


@patch("requests.Session.get")
def test_get_release_info_parse_error(mock_get: MagicMock, extractor: Extractor):
    """Test that an error is raised if release info cannot be parsed."""
    # --- Arrange ---
    mock_response = MagicMock()
    mock_response.text = "Invalid content that doesn't match"
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    # --- Act & Assert ---
    with pytest.raises(
        ValueError, match="Could not parse release version/date from reldate.txt"
    ):
        extractor.get_release_info()


@patch("requests.Session.get")
def test_download_file_resume_success(
    mock_get: MagicMock, extractor: Extractor, settings: Settings
):
    """Test successful resumable file download."""
    # --- Arrange ---
    filename = "test_resume.xml.gz"
    local_path = settings.data_dir / filename
    initial_content = b"initial data"
    resumed_content = b"resumed data"
    downloaded_size = len(initial_content)

    # Simulate existing partial file
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(initial_content)

    # Mock the response for a range request
    mock_response = MagicMock()
    mock_response.status_code = 206  # Partial Content
    mock_response.headers.get.return_value = str(len(resumed_content))
    mock_response.iter_content.return_value = [resumed_content]
    mock_get.return_value.__enter__.return_value = mock_response

    # --- Act ---
    extractor.download_file(filename)

    # --- Assert ---
    # Check that the request was made with the correct Range header
    mock_get.assert_called_once()
    args, kwargs = mock_get.call_args
    assert "headers" in kwargs
    assert kwargs["headers"]["Range"] == f"bytes={downloaded_size}-"

    # Check that the file content is correct
    assert local_path.read_bytes() == initial_content + resumed_content


@patch("requests.Session.get")
def test_fetch_checksums_not_found(mock_get: MagicMock, extractor: Extractor):
    """Test that an empty dict is returned if the checksum file is not found (404)."""
    # --- Arrange ---
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    # --- Act ---
    checksums = extractor.fetch_checksums()

    # --- Assert ---
    assert checksums == {}
    assert extractor._checksums == {}


@patch("requests.Session.get")
def test_get_release_info_no_match(mock_get: MagicMock, extractor: Extractor):
    """Test that an error is raised if the release info has an unexpected format."""
    # --- Arrange ---
    mock_response = MagicMock()
    mock_response.text = "This is not the data you are looking for"
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    # --- Act & Assert ---
    with pytest.raises(ValueError, match="Could not parse release version/date"):
        extractor.get_release_info()


@patch("py_load_uniprot.extractor.Extractor.fetch_checksums", return_value={})
def test_verify_checksum_no_checksums_available(
    mock_fetch_checksums: MagicMock, extractor: Extractor, temp_data_dir: Path
):
    """Test that verification is skipped if no checksums can be fetched."""
    # --- Arrange ---
    file_path = temp_data_dir / "any_file.gz"
    file_path.write_bytes(b"data")
    extractor._checksums = None  # Ensure checksums are not cached

    # --- Act ---
    result = extractor.verify_checksum(file_path)

    # --- Assert ---
    mock_fetch_checksums.assert_called_once()
    assert result is True


def test_verify_checksum_mismatch(extractor: Extractor, temp_data_dir: Path):
    """Test checksum verification for a file with a mismatched checksum."""
    # --- Arrange ---
    filename = "test_file.gz"
    content = b"some data"
    file_path = temp_data_dir / filename
    file_path.write_bytes(content)

    # Pre-populate the checksums with a wrong checksum
    extractor._checksums = {filename: "this is a wrong checksum"}

    # --- Act & Assert ---
    assert extractor.verify_checksum(file_path) is False


@patch("requests.Session.get", side_effect=requests.exceptions.RequestException("Test error"))
def test_fetch_checksums_request_exception(mock_get: MagicMock, extractor: Extractor):
    """Test that an empty dict is returned if a request exception occurs."""
    # --- Act ---
    checksums = extractor.fetch_checksums()

    # --- Assert ---
    assert checksums == {}
    assert extractor._checksums == {}
