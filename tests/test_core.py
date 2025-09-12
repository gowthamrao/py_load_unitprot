from unittest.mock import MagicMock, patch

import pytest

from py_load_uniprot.config import Settings
from py_load_uniprot.core import PyLoadUniprotPipeline


@pytest.fixture
def mock_settings() -> Settings:
    """Fixture for a mock Settings object."""
    return Settings()


@patch("tempfile.mkdtemp")
@patch("py_load_uniprot.core.transformer.transform_xml_to_tsv")
@patch("py_load_uniprot.core.extractor.Extractor")
@patch("py_load_uniprot.core.PostgresAdapter")
def test_pipeline_run_full_load_success(
    mock_adapter_cls, mock_extractor_cls, mock_transformer, mock_mkdtemp, mock_settings, tmp_path
):
    """Tests a successful full load run of the pipeline."""
    mock_db_adapter = MagicMock()
    mock_adapter_cls.return_value = mock_db_adapter

    mock_extractor = MagicMock()
    mock_extractor.get_release_info.return_value = {"version": "2024_03"}
    mock_extractor_cls.return_value = mock_extractor

    # Mock the temporary directory creation
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    mock_mkdtemp.return_value = str(temp_dir)

    # Create dummy intermediate files
    (temp_dir / "proteins.tsv.gz").touch()
    (temp_dir / "taxonomy.tsv.gz").touch()


    # Create a dummy source file to satisfy the existence check
    mock_settings.data_dir.mkdir(exist_ok=True)
    (mock_settings.data_dir / "uniprot_sprot.xml.gz").touch()

    pipeline = PyLoadUniprotPipeline(mock_settings)
    pipeline.run(dataset="swissprot", mode="full")

    mock_db_adapter.initialize_schema.assert_called_once_with(mode="full")
    mock_transformer.assert_called_once()
    assert mock_db_adapter.bulk_load_intermediate.call_count == 2
    mock_db_adapter.deduplicate_staging_data.assert_called()
    mock_db_adapter.finalize_load.assert_called_once_with(mode="full")
    mock_db_adapter.update_metadata.assert_called_once()
    mock_db_adapter.log_run.assert_called_once()


@patch("py_load_uniprot.core.extractor.Extractor")
@patch("py_load_uniprot.core.PostgresAdapter")
def test_pipeline_run_delta_load_up_to_date(
    mock_adapter_cls, mock_extractor_cls, mock_settings
):
    """Tests that a delta load halts if the database is already up to date."""
    mock_db_adapter = MagicMock()
    mock_db_adapter.get_current_release_version.return_value = "2024_03"
    mock_adapter_cls.return_value = mock_db_adapter

    mock_extractor = MagicMock()
    mock_extractor.get_release_info.return_value = {"version": "2024_03"}
    mock_extractor_cls.return_value = mock_extractor

    pipeline = PyLoadUniprotPipeline(mock_settings)
    pipeline.run(dataset="swissprot", mode="delta")

    mock_db_adapter.initialize_schema.assert_not_called()
    mock_db_adapter.log_run.assert_not_called()


def test_pipeline_run_invalid_mode(mock_settings):
    """Tests that the pipeline raises a ValueError for an invalid mode."""
    pipeline = PyLoadUniprotPipeline(mock_settings)
    with pytest.raises(ValueError, match="Load mode 'invalid' is not valid"):
        pipeline.run(dataset="swissprot", mode="invalid")


def test_pipeline_run_invalid_dataset(mock_settings):
    """Tests that the pipeline raises a ValueError for an invalid dataset."""
    pipeline = PyLoadUniprotPipeline(mock_settings)
    with pytest.raises(ValueError, match="Dataset 'invalid' is not valid"):
        pipeline.run(dataset="invalid", mode="full")
