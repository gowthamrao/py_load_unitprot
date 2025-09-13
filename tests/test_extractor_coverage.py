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


@patch("requests.Session.get")
def test_download_file_request_exception(mock_get: MagicMock, extractor: Extractor):
    """
    Tests that download_file raises an exception when a RequestException occurs.
    """
    mock_get.side_effect = requests.exceptions.RequestException("Test error")
    with pytest.raises(requests.exceptions.RequestException):
        extractor.download_file("test.xml.gz")


@patch("requests.Session.get")
def test_fetch_checksums_404(mock_get: MagicMock, extractor: Extractor):
    """
    Tests that fetch_checksums returns an empty dict on a 404 response.
    """
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response
    checksums = extractor.fetch_checksums()
    assert checksums == {}


@patch("requests.Session.get")
def test_fetch_checksums_request_exception(mock_get: MagicMock, extractor: Extractor):
    """
    Tests that fetch_checksums returns an empty dict on a RequestException.
    """
    mock_get.side_effect = requests.exceptions.RequestException("Test error")
    checksums = extractor.fetch_checksums()
    assert checksums == {}


def test_verify_checksum_no_expected_md5(extractor: Extractor, temp_data_dir: Path):
    """
    Tests that verify_checksum returns True when no expected MD5 is found.
    """
    file_path = temp_data_dir / "test.txt"
    file_path.write_text("test")
    extractor._checksums = {}
    assert extractor.verify_checksum(file_path) is True


@patch("requests.Session.get")
def test_get_release_info_date_parse_error(mock_get: MagicMock, extractor: Extractor):
    """
    Tests get_release_info when the date string is in an invalid format.
    """
    mock_reldate_response = MagicMock()
    mock_reldate_response.text = "Release 2025_09 of 08/09/2025"  # Invalid date format
    mock_reldate_response.raise_for_status.return_value = None

    mock_relnotes_response = MagicMock()
    mock_relnotes_response.text = "UniProtKB/Swiss-Prot: 1,234 entries and UniProtKB/TrEMBL: 5,678 entries"
    mock_relnotes_response.raise_for_status.return_value = None

    mock_get.side_effect = [mock_reldate_response, mock_relnotes_response]

    info = extractor.get_release_info()
    assert info["date"] == "08/09/2025"  # Should keep the original string


@patch("requests.Session.get")
def test_get_release_info_relnotes_parse_error(mock_get: MagicMock, extractor: Extractor):
    """
    Tests get_release_info when the relnotes.txt has an unexpected format.
    """
    mock_reldate_response = MagicMock()
    mock_reldate_response.text = "Release 2025_09 of 08-Sep-2025"
    mock_reldate_response.raise_for_status.return_value = None

    mock_relnotes_response = MagicMock()
    mock_relnotes_response.text = "Invalid format"
    mock_relnotes_response.raise_for_status.return_value = None

    mock_get.side_effect = [mock_reldate_response, mock_relnotes_response]

    info = extractor.get_release_info()
    assert info["swissprot_entry_count"] == 0
    assert info["trembl_entry_count"] == 0


@patch("requests.Session.get")
def test_get_release_info_relnotes_request_exception(mock_get: MagicMock, extractor: Extractor):
    """
    Tests get_release_info when fetching relnotes.txt fails.
    """
    mock_reldate_response = MagicMock()
    mock_reldate_response.text = "Release 2025_09 of 08-Sep-2025"
    mock_reldate_response.raise_for_status.return_value = None

    mock_get.side_effect = [
        mock_reldate_response,
        requests.exceptions.RequestException("Test error"),
    ]

    info = extractor.get_release_info()
    assert info["swissprot_entry_count"] == 0
    assert info["trembl_entry_count"] == 0
