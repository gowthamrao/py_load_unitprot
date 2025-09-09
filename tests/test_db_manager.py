import gzip
import importlib.resources
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from psycopg2.extensions import connection, cursor

from py_load_uniprot.config import DBSettings, Settings
from py_load_uniprot.db_manager import PostgresAdapter, postgres_connection


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


def test_postgres_connection_success(mock_settings, mock_conn, mock_cur):
    """
    Tests that the postgres_connection context manager successfully
    connects, yields a connection, and closes it.
    """
    with patch(
        "psycopg2.connect", return_value=mock_conn
    ) as mock_connect, postgres_connection(mock_settings) as conn:
        assert conn == mock_conn
        mock_connect.assert_called_once_with(mock_settings.db.connection_string)
    mock_conn.close.assert_called_once()


def test_postgres_connection_failure(mock_settings):
    """
    Tests that the postgres_connection context manager raises an
    exception if the connection fails.
    """
    with patch(
        "psycopg2.connect", side_effect=psycopg2.OperationalError("Connection failed")
    ) as mock_connect, pytest.raises(psycopg2.OperationalError):
        with postgres_connection(mock_settings):
            pass  # This block should not be reached
    mock_connect.assert_called_once_with(mock_settings.db.connection_string)


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_check_connection(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """Tests the check_connection method."""
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    adapter = PostgresAdapter(mock_settings)
    adapter.check_connection()

    mock_cur.execute.assert_called_once_with("SELECT 1;")


@patch("importlib.resources.files")
@patch("py_load_uniprot.db_manager.postgres_connection")
def test_initialize_schema(mock_pg_conn, mock_files, mock_settings, mock_conn, mock_cur):
    """Tests the initialize_schema method."""
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_sql_file = MagicMock()
    mock_sql_file.read_text.return_value = "CREATE SCHEMA __{SCHEMA_NAME}__;"
    mock_files.return_value.joinpath.return_value = mock_sql_file

    adapter = PostgresAdapter(mock_settings)
    adapter.initialize_schema(mode="full")

    assert mock_cur.execute.call_count == 2
    mock_cur.execute.assert_any_call("DROP SCHEMA IF EXISTS uniprot_staging CASCADE;")
    mock_cur.execute.assert_any_call("CREATE SCHEMA uniprot_staging;")
    mock_conn.commit.assert_called_once()


@patch("builtins.open", new_callable=MagicMock)
@patch("gzip.open", new_callable=MagicMock)
@patch("py_load_uniprot.db_manager.postgres_connection")
def test_bulk_load_intermediate(mock_pg_conn, mock_gzip_open, mock_open, mock_settings, mock_conn, mock_cur):
    """Tests the bulk_load_intermediate method."""
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_file_handle = MagicMock()
    mock_file_handle.readline.return_value = "col1\tcol2"
    mock_gzip_open.return_value.__enter__.return_value = mock_file_handle

    adapter = PostgresAdapter(mock_settings)
    adapter.bulk_load_intermediate(Path("/fake/path.tsv.gz"), "my_table")

    mock_cur.copy_expert.assert_called_once()
    mock_conn.commit.assert_called_once()


@patch("importlib.resources.files")
@patch("py_load_uniprot.db_manager.postgres_connection")
def test_finalize_full_load(mock_pg_conn, mock_files, mock_settings, mock_conn, mock_cur):
    """Tests the _finalize_full_load method."""
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_sql_file = MagicMock()
    mock_sql_file.read_text.return_value = "CREATE INDEX idx ON __{SCHEMA_NAME}__.table;"
    mock_files.return_value.joinpath.return_value = mock_sql_file

    adapter = PostgresAdapter(mock_settings)
    adapter._finalize_full_load()

    assert mock_cur.execute.call_count > 3 # create indexes, analyze, check schema, rename, rename
    mock_conn.commit.assert_called_once()


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_update_metadata(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """Tests the update_metadata method."""
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    release_info = {
        "version": "2024_03",
        "release_date": datetime.now().date(),
        "swissprot_entry_count": 100,
        "trembl_entry_count": 200,
    }

    adapter = PostgresAdapter(mock_settings)
    adapter.update_metadata(release_info)

    assert mock_cur.execute.call_count >= 2
    mock_cur.execute.assert_any_call("TRUNCATE TABLE uniprot_public.py_load_uniprot_metadata;")
    mock_cur.execute.assert_any_call(
        """
        INSERT INTO uniprot_public.py_load_uniprot_metadata (
            version,
            release_date,
            swissprot_entry_count,
            trembl_entry_count
        ) VALUES (
            %(version)s,
            %(release_date)s,
            %(swissprot_entry_count)s,
            %(trembl_entry_count)s
        );
        """,
        release_info,
    )
    mock_conn.commit.assert_called_once()


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_get_current_release_version_found(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """Tests get_current_release_version when a version is found."""
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = ("2024_03",)

    adapter = PostgresAdapter(mock_settings)
    version = adapter.get_current_release_version()

    assert version == "2024_03"
    mock_cur.execute.assert_called_once_with("SELECT version FROM uniprot_public.py_load_uniprot_metadata ORDER BY load_timestamp DESC LIMIT 1;")


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_get_current_release_version_not_found(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """Tests get_current_release_version when no version is found."""
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = None

    adapter = PostgresAdapter(mock_settings)
    version = adapter.get_current_release_version()

    assert version is None


@patch("py_load_uniprot.db_manager.postgres_connection")
def test_get_current_release_version_no_table(mock_pg_conn, mock_settings, mock_conn, mock_cur):
    """Tests get_current_release_version when the metadata table does not exist."""
    mock_pg_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.execute.side_effect = psycopg2.errors.UndefinedTable

    adapter = PostgresAdapter(mock_settings)
    version = adapter.get_current_release_version()

    assert version is None
