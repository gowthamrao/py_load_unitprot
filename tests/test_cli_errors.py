from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from py_load_uniprot.cli import app

runner = CliRunner()


def test_download_invalid_dataset():
    """Tests the download command with an invalid dataset name."""
    result = runner.invoke(app, ["download", "--dataset", "invalid_dataset"])
    assert result.exit_code == 1
    assert "Error: Invalid dataset 'invalid_dataset'" in result.stdout


@patch("py_load_uniprot.cli.extractor.Extractor")
@patch("py_load_uniprot.cli.load_settings")
def test_download_checksum_fails(mock_load_settings, mock_extractor_cls):
    """Tests the download command when checksum verification fails."""
    mock_extractor_instance = MagicMock()
    mock_extractor_instance.get_release_info.return_value = {"version": "2024_03"}
    mock_extractor_instance.download_file.return_value = "/fake/path"
    mock_extractor_instance.verify_checksum.return_value = False  # Simulate failure
    mock_extractor_cls.return_value = mock_extractor_instance

    result = runner.invoke(app, ["download", "--dataset", "swissprot"])

    assert result.exit_code == 1
    assert "Checksum verification failed for 'swissprot'" in result.stdout


@patch("py_load_uniprot.cli.extractor.Extractor")
@patch("py_load_uniprot.cli.load_settings")
def test_download_exception(mock_load_settings, mock_extractor_cls):
    """Tests the download command when an exception occurs during download."""
    mock_extractor_instance = MagicMock()
    mock_extractor_instance.get_release_info.return_value = {"version": "2024_03"}
    mock_extractor_instance.download_file.side_effect = Exception("Download failed")
    mock_extractor_cls.return_value = mock_extractor_instance

    result = runner.invoke(app, ["download", "--dataset", "swissprot"])

    assert result.exit_code == 1
    assert "An error occurred while downloading swissprot: Download failed" in result.stdout


@patch("py_load_uniprot.cli.load_settings", side_effect=FileNotFoundError("Config not found"))
def test_download_config_not_found(mock_load_settings):
    """Tests the download command when the config file is not found."""
    result = runner.invoke(app, ["download", "--dataset", "swissprot"])
    assert result.exit_code == 1
    assert "Configuration Error: Config not found" in result.stdout


@patch("py_load_uniprot.cli.PyLoadUniprotPipeline")
@patch("py_load_uniprot.cli.load_settings")
def test_run_value_error(mock_load_settings, mock_pipeline_cls):
    """Tests the run command when a ValueError is raised."""
    mock_pipeline_instance = MagicMock()
    mock_pipeline_instance.run.side_effect = ValueError("Invalid mode")
    mock_pipeline_cls.return_value = mock_pipeline_instance

    result = runner.invoke(app, ["run"])
    assert result.exit_code == 1
    assert "Configuration Error: Invalid mode" in result.stdout


@patch("py_load_uniprot.cli.load_settings", side_effect=Exception("Something went wrong"))
def test_check_config_exception(mock_load_settings):
    """Tests the check-config command when an unexpected exception occurs."""
    result = runner.invoke(app, ["check-config"])
    assert result.exit_code == 1
    assert "An error occurred during the check: Something went wrong" in result.stdout


@patch("py_load_uniprot.cli.PostgresAdapter")
@patch("py_load_uniprot.cli.load_settings")
def test_initialize_exception(mock_load_settings, mock_adapter_cls):
    """Tests the initialize command when an exception occurs."""
    mock_adapter_instance = MagicMock()
    mock_adapter_instance.create_production_schema.side_effect = Exception("DB error")
    mock_adapter_cls.return_value = mock_adapter_instance

    result = runner.invoke(app, ["initialize"])
    assert result.exit_code == 1
    assert "An error occurred during schema initialization: DB error" in result.stdout


@patch("py_load_uniprot.cli.PostgresAdapter")
@patch("py_load_uniprot.cli.load_settings")
def test_status_no_version(mock_load_settings, mock_adapter_cls):
    """Tests the status command when no release version is loaded."""
    mock_adapter_instance = MagicMock()
    mock_adapter_instance.get_current_release_version.return_value = None
    mock_adapter_cls.return_value = mock_adapter_instance

    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "No UniProt release is currently loaded" in result.stdout


@patch("py_load_uniprot.cli.PostgresAdapter")
@patch("py_load_uniprot.cli.load_settings")
def test_status_exception(mock_load_settings, mock_adapter_cls):
    """Tests the status command when an exception occurs."""
    mock_adapter_instance = MagicMock()
    mock_adapter_instance.get_current_release_version.side_effect = Exception("DB error")
    mock_adapter_cls.return_value = mock_adapter_instance

    result = runner.invoke(app, ["status"])
    assert result.exit_code == 1
    assert "An error occurred while checking the status: DB error" in result.stdout
