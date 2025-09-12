from unittest.mock import MagicMock, patch

import psycopg2
from typer.testing import CliRunner

from py_load_uniprot.cli import app

runner = CliRunner()


@patch("py_load_uniprot.cli.extractor.Extractor")
@patch("py_load_uniprot.cli.load_settings")
def test_download_command_success(mock_load_settings, mock_extractor_cls):
    """Tests the download command with a valid dataset."""
    mock_extractor_instance = MagicMock()
    mock_extractor_instance.get_release_info.return_value = {"version": "2024_03"}
    mock_extractor_instance.download_file.return_value = "/fake/path"
    mock_extractor_instance.verify_checksum.return_value = True
    mock_extractor_cls.return_value = mock_extractor_instance

    result = runner.invoke(app, ["download", "--dataset", "swissprot"])

    assert result.exit_code == 0
    assert "All specified datasets downloaded successfully" in result.stdout
    mock_extractor_instance.download_file.assert_called_once_with(
        "uniprot_sprot.xml.gz"
    )


@patch("py_load_uniprot.cli.extractor.Extractor")
@patch("py_load_uniprot.cli.load_settings")
def test_download_command_all_datasets(mock_load_settings, mock_extractor_cls):
    """Tests the download command with 'all' datasets."""
    mock_extractor_instance = MagicMock()
    mock_extractor_instance.get_release_info.return_value = {"version": "2024_03"}
    mock_extractor_instance.download_file.side_effect = ["/fake/sprot", "/fake/trembl"]
    mock_extractor_instance.verify_checksum.return_value = True
    mock_extractor_cls.return_value = mock_extractor_instance

    result = runner.invoke(app, ["download", "--dataset", "all"])

    assert result.exit_code == 0
    assert mock_extractor_instance.download_file.call_count == 2


@patch("py_load_uniprot.cli.PyLoadUniprotPipeline")
@patch("py_load_uniprot.cli.load_settings")
def test_run_command(mock_load_settings, mock_pipeline_cls):
    """Tests the run command."""
    mock_pipeline_instance = MagicMock()
    mock_pipeline_cls.return_value = mock_pipeline_instance

    result = runner.invoke(app, ["run", "--dataset", "swissprot", "--mode", "full"])

    assert result.exit_code == 0
    mock_pipeline_instance.run.assert_called_once_with(
        dataset="swissprot", mode="full"
    )


@patch("py_load_uniprot.cli.PostgresAdapter")
@patch("py_load_uniprot.cli.load_settings")
def test_check_config_command(mock_load_settings, mock_adapter_cls):
    """Tests the check-config command."""
    mock_adapter_instance = MagicMock()
    mock_adapter_cls.return_value = mock_adapter_instance

    result = runner.invoke(app, ["check-config"])

    assert result.exit_code == 0
    assert "Configuration and connectivity check passed" in result.stdout
    mock_adapter_instance.check_connection.assert_called_once()


@patch("py_load_uniprot.cli.load_settings")
@patch("psycopg2.connect")
def test_cli_handles_db_connection_error(mock_psycopg2_connect, mock_load_settings):
    """
    Tests that a CLI command fails gracefully with a clear error message
    if the database connection fails. We test this on the 'check-config'
    command as it's the simplest way to trigger a connection.
    """
    # Arrange
    # Mock psycopg2.connect to raise a known database connection error
    mock_psycopg2_connect.side_effect = MagicMock(
        side_effect=psycopg2.OperationalError("could not connect to server: Connection refused")
    )

    # Act
    result = runner.invoke(app, ["check-config"])

    # Assert
    assert result.exit_code != 0, "CLI should exit with a non-zero code on DB error"
    assert "An error occurred during the check" in result.stdout
    assert "could not connect to server" in result.stdout
