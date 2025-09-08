import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import requests

from py_load_uniprot import extractor
from py_load_uniprot.config import settings

# A known MD5 hash for the content "hello world"
HELLO_WORLD_CONTENT = b"hello world"
HELLO_WORLD_MD5 = "5eb63bbbe01eeed093cb22bb8f5acdc3"

MOCK_CHECKSUM_DATA = """\
5eb63bbbe01eeed093cb22bb8f5acdc3  uniprot_sprot.xml.gz
bad_hash_value  uniprot_trembl.xml.gz
"""

@pytest.fixture
def mock_settings(tmp_path):
    """Fixture to override the DATA_DIR setting to use a temporary directory."""
    original_data_dir = settings.DATA_DIR
    settings.DATA_DIR = tmp_path / "data"
    yield settings
    settings.DATA_DIR = original_data_dir

def test_calculate_md5(tmp_path):
    """
    Tests the calculate_md5 function with a known file content and hash.
    """
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(HELLO_WORLD_CONTENT)

    # Act
    calculated_hash = extractor.calculate_md5(test_file)

    # Assert
    assert calculated_hash == HELLO_WORLD_MD5

@patch('requests.get')
def test_get_release_checksums_success(mock_get):
    """
    Tests that get_release_checksums correctly parses valid checksum data.
    """
    # Arrange
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = MOCK_CHECKSUM_DATA
    mock_get.return_value = mock_response

    # Act
    checksums = extractor.get_release_checksums()

    # Assert
    assert "uniprot_sprot.xml.gz" in checksums
    assert checksums["uniprot_sprot.xml.gz"] == HELLO_WORLD_MD5
    assert "uniprot_trembl.xml.gz" in checksums
    assert checksums["uniprot_trembl.xml.gz"] == "bad_hash_value"
    mock_get.assert_called_once_with(settings.checksums_url)

@patch('requests.get')
def test_get_release_checksums_failure(mock_get):
    """
    Tests that get_release_checksums raises an exception on network failure.
    """
    # Arrange
    mock_get.side_effect = requests.exceptions.RequestException("Network Error")

    # Act & Assert
    with pytest.raises(requests.exceptions.RequestException):
        extractor.get_release_checksums()

@patch('py_load_uniprot.extractor.get_release_checksums')
@patch('py_load_uniprot.extractor.download_uniprot_file')
@patch('py_load_uniprot.extractor.calculate_md5')
def test_run_extraction_success(mock_calculate_md5, mock_download, mock_get_checksums, mock_settings):
    """
    Tests the success scenario for run_extraction.
    """
    # Arrange
    mock_get_checksums.return_value = {
        "uniprot_sprot.xml.gz": "hash1",
        "uniprot_trembl.xml.gz": "hash2",
    }
    mock_calculate_md5.side_effect = ["hash1", "hash2"]

    # Act
    extractor.run_extraction()

    # Assert
    assert mock_download.call_count == 2
    assert mock_calculate_md5.call_count == 2
    mock_get_checksums.assert_called_once()

@patch('py_load_uniprot.extractor.get_release_checksums')
@patch('py_load_uniprot.extractor.download_uniprot_file')
@patch('py_load_uniprot.extractor.calculate_md5')
def test_run_extraction_checksum_mismatch(mock_calculate_md5, mock_download, mock_get_checksums, mock_settings):
    """
    Tests that run_extraction raises an error on checksum mismatch.
    """
    # Arrange
    mock_get_checksums.return_value = {"uniprot_sprot.xml.gz": "correct_hash"}
    mock_calculate_md5.return_value = "wrong_hash"

    # Act & Assert
    with pytest.raises(RuntimeError, match="Checksum mismatch"):
        extractor.run_extraction()

@patch('py_load_uniprot.extractor.get_release_checksums')
@patch('py_load_uniprot.extractor.download_uniprot_file')
@patch('py_load_uniprot.extractor.calculate_md5')
def test_run_extraction_skips_existing_valid_file(mock_calculate_md5, mock_download, mock_get_checksums, mock_settings):
    """
    Tests that run_extraction skips one file and re-downloads another on mismatch.
    """
    # Arrange
    sprot_file = mock_settings.DATA_DIR / "uniprot_sprot.xml.gz"
    trembl_file = mock_settings.DATA_DIR / "uniprot_trembl.xml.gz"
    mock_settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    sprot_file.touch()
    trembl_file.touch()

    mock_get_checksums.return_value = {
        "uniprot_sprot.xml.gz": "correct_hash1",
        "uniprot_trembl.xml.gz": "correct_hash2",
    }
    # 1. Check sprot (valid) -> 'correct_hash1'
    # 2. Check trembl (invalid) -> 'wrong_hash'
    # 3. Check trembl after download (valid) -> 'correct_hash2'
    mock_calculate_md5.side_effect = ["correct_hash1", "wrong_hash", "correct_hash2"]

    # Act
    extractor.run_extraction()

    # Assert
    # Download should be called only for the trembl file.
    assert mock_download.call_count == 1
    mock_download.assert_called_once_with(settings.trembl_xml_url, trembl_file)

    # Checksum calculated for sprot (pass), then trembl (fail), then trembl again (pass).
    assert mock_calculate_md5.call_count == 3
