import gzip
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from rich import print

from py_load_uniprot.config import settings


@contextmanager
def postgres_connection():
    """Context manager for PostgreSQL database connections."""
    conn = None
    try:
        conn = psycopg2.connect(settings.db_connection_string)
        yield conn
    except psycopg2.OperationalError as e:
        print(f"[bold red]Database connection error: {e}[/bold red]")
        print("[yellow]Please ensure PostgreSQL is running and the connection settings in your .env file are correct.[/yellow]")
        raise
    finally:
        if conn:
            conn.close()


class DatabaseAdapter(ABC):
    """Abstract Base Class for database adapters, defining the contract for database operations."""

    @abstractmethod
    def initialize_schema(self) -> None:
        """Prepares the database schema (e.g., creates tables)."""
        pass

    @abstractmethod
    def bulk_load_intermediate(self, file_path: Path, table_name: str) -> None:
        """Executes the native bulk load operation for a specific file."""
        pass

    @abstractmethod
    def finalize_load(self, mode: str) -> None:
        """Performs post-load operations (e.g., indexing, analyzing)."""
        pass

    @abstractmethod
    def get_current_release_version(self) -> str | None:
        """Retrieves the version of UniProt currently loaded in the DB."""
        pass


class PostgresAdapter(DatabaseAdapter):
    """Database adapter for PostgreSQL."""

    def initialize_schema(self) -> None:
        """
        Creates the 'proteins' table in the PostgreSQL database.
        Drops the table if it already exists to ensure a clean slate.
        """
        print("Initializing database schema...")
        create_table_sql = """
        DROP TABLE IF EXISTS proteins;
        CREATE TABLE proteins (
            primary_accession VARCHAR(255) PRIMARY KEY,
            uniprot_id VARCHAR(255),
            sequence_length INTEGER,
            molecular_weight INTEGER,
            created_date DATE,
            modified_date DATE,
            sequence TEXT
        );
        """
        with postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
            conn.commit()
        print("[green]Schema 'proteins' initialized successfully.[/green]")

    def bulk_load_intermediate(self, file_path: Path, table_name: str) -> None:
        """
        Loads data from a gzipped TSV file into a PostgreSQL table using the native COPY command.

        Args:
            file_path: The path to the gzipped TSV file.
            table_name: The name of the target table in the database.
        """
        print(f"Starting bulk load of {file_path.name} into '{table_name}' table...")
        if not file_path.exists():
            print(f"[bold red]Error: Intermediate file not found at {file_path}[/bold red]")
            raise FileNotFoundError(f"Intermediate file not found: {file_path}")

        with postgres_connection() as conn:
            with conn.cursor() as cur, gzip.open(file_path, 'rt', encoding='utf-8') as f:
                # The columns in the COPY command must match the order in the TSV file.
                # We skip the header row from the file itself.
                header = f.readline().strip().split('\t')
                copy_sql = f"COPY {table_name} ({','.join(header)}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', HEADER false)"

                cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
        print(f"[green]Bulk load into '{table_name}' completed successfully.[/green]")

    def finalize_load(self, mode: str) -> None:
        """Placeholder for post-load operations."""
        print("[yellow]Finalize step not yet implemented.[/yellow]")
        pass

    def get_current_release_version(self) -> str | None:
        """Placeholder for version retrieval."""
        print("[yellow]Get current release version not yet implemented.[/yellow]")
        return None
