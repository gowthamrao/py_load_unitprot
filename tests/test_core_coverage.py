from unittest.mock import MagicMock, patch

import pytest

from py_load_uniprot.config import Settings
from py_load_uniprot.core import PyLoadUniprotPipeline


@pytest.fixture
def mock_settings() -> Settings:
    """Fixture for a mock Settings object."""
    return Settings()


def test_from_config_file_not_found(tmp_path):
    """
    Tests that from_config_file raises FileNotFoundError for a non-existent file.
    """
    with pytest.raises(FileNotFoundError):
        PyLoadUniprotPipeline.from_config_file(tmp_path / "non_existent_config.yaml")


@patch("py_load_uniprot.core.PostgresAdapter")
def test_run_pipeline_exception(mock_adapter_cls, mock_settings):
    """
    Tests that the pipeline correctly handles a generic exception during a run.
    """
    mock_db_adapter = MagicMock()
    mock_db_adapter.initialize_schema.side_effect = Exception("Test DB error")
    mock_adapter_cls.return_value = mock_db_adapter

    pipeline = PyLoadUniprotPipeline(mock_settings)
    with pytest.raises(Exception, match="Test DB error"):
        pipeline.run(dataset="swissprot", mode="full")

    mock_db_adapter.log_run.assert_called_once()
    # Check that status is FAILED and an error message is present
    call_args = mock_db_adapter.log_run.call_args_list[0]
    args, kwargs = call_args
    assert args[3] == "FAILED"
    assert "Test DB error" in kwargs["error_message"]


@patch("py_load_uniprot.core.print")
@patch("py_load_uniprot.core.transformer.transform_xml_to_tsv")
@patch("py_load_uniprot.core.PostgresAdapter")
def test_transform_and_load_single_dataset_no_file(
    mock_adapter_cls, mock_transformer, mock_print, mock_settings, tmp_path
):
    """
    Tests that a warning is printed when a data file is not found for a table.
    """
    source_file = tmp_path / "uniprot_sprot.xml.gz"
    source_file.touch()
    mock_settings.data_dir = tmp_path

    pipeline = PyLoadUniprotPipeline(mock_settings)
    pipeline._transform_and_load_single_dataset("swissprot")
    mock_print.assert_any_call(
        "    [yellow]Warning: No data file for 'taxonomy'. Skipping.[/yellow]"
    )
