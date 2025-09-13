from datetime import datetime
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from psycopg2.extensions import connection, cursor

from py_load_uniprot.config import DBSettings, Settings
from py_load_uniprot.db_manager import PostgresAdapter


@pytest.fixture
def mock_settings() -> Settings:
    """Fixture for a mock Settings object."""
    return Settings(
        db=DBSettings(
            host="localhost",
            port=5432,
            user="testuser",
            password="testpassword",
            dbname="testdb",
        ),
        data_dir="data",
    )


@pytest.fixture
def mock_conn() -> MagicMock:
    """Fixture for a mock psycopg2 connection."""
    return MagicMock(spec=connection)


@pytest.fixture
def mock_cur() -> MagicMock:
    """Fixture for a mock psycopg2 cursor."""
    return MagicMock(spec=cursor)


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_deduplicate_staging_data(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """
    Tests the deduplicate_staging_data method when duplicates are found.
    """
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.rowcount = 5  # Simulate 5 deleted rows

    adapter = PostgresAdapter(mock_settings)
    adapter.deduplicate_staging_data("my_table", "my_key")

    assert mock_cur.execute.call_count == 1
    mock_conn.commit.assert_called_once()


@patch("importlib.resources.files")
@patch("py_load_uniprot.db_manager.postgres_connection")
def test_finalize_full_load_with_existing_schema(mock_pg_conn, mock_files, mock_settings, mock_conn, mock_cur):
    """
    Tests the _finalize_full_load method when the production schema already exists.
    """
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = (1,)  # Simulate schema exists

    mock_sql_file = MagicMock()
    mock_sql_file.read_text.return_value = "CREATE INDEX idx ON __{SCHEMA_NAME}__.table;"
    mock_files.return_value.joinpath.return_value = mock_sql_file

    adapter = PostgresAdapter(mock_settings)
    adapter._finalize_full_load()

    rename_call_found = any(
        "ALTER SCHEMA uniprot_public RENAME TO" in call[0][0]
        for call in mock_cur.execute.call_args_list
    )
    assert rename_call_found
    mock_conn.commit.assert_called_once()


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_upsert_proteins_with_changed_accessions(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """
    Tests the _upsert_proteins method when there are proteins with changed primary accessions.
    """
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchall.return_value = [("old_accession",)]

    adapter = PostgresAdapter(mock_settings)
    adapter._upsert_proteins(mock_cur)

    assert mock_cur.execute.call_count == 3
    mock_cur.execute.assert_any_call("DELETE FROM uniprot_public.proteins WHERE primary_accession = ANY(%s);", (["old_accession"],))


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_delete_removed_proteins(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """
    Tests the _delete_removed_proteins method when there are proteins to delete.
    """
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchall.return_value = [("deleted_accession",)]

    adapter = PostgresAdapter(mock_settings)
    adapter._delete_removed_proteins(mock_cur)

    assert mock_cur.execute.call_count == 2
    mock_cur.execute.assert_any_call("DELETE FROM uniprot_public.proteins WHERE primary_accession = ANY(%s);", (["deleted_accession"],))


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_log_run_undefined_table(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """
    Tests the log_run method when the history table does not exist.
    """
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.execute.side_effect = psycopg2.errors.UndefinedTable

    adapter = PostgresAdapter(mock_settings)
    adapter.log_run("run1", "full", "all", "COMPLETED", datetime.now(), datetime.now())
    # No exception should be raised


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_log_run_generic_exception(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """
    Tests the log_run method when a generic exception occurs.
    """
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.execute.side_effect = Exception("Test error")

    adapter = PostgresAdapter(mock_settings)
    adapter.log_run("run1", "full", "all", "COMPLETED", datetime.now(), datetime.now())
    # No exception should be raised


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_cleanup_error(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """
    Tests the cleanup method when a psycopg2.Error occurs.
    """
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.execute.side_effect = psycopg2.Error

    adapter = PostgresAdapter(mock_settings)
    adapter.cleanup()
    # No exception should be raised
