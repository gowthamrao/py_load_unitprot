import gzip
import importlib.resources
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import psycopg2
from psycopg2.extensions import connection, cursor
from rich import print

from py_load_uniprot.config import Settings
from py_load_uniprot.transformer import TABLE_HEADERS

TABLE_LOAD_ORDER = [
    "taxonomy",
    "proteins",
    "sequences",
    "accessions",
    "genes",
    "keywords",
    "protein_to_go",
    "protein_to_taxonomy",
]
TABLES_WITH_UNIQUE_CONSTRAINTS: dict[str, str] = {"taxonomy": "ncbi_taxid"}


@contextmanager
def postgres_connection(settings: Settings) -> Iterator[connection]:
    conn = None
    try:
        conn = psycopg2.connect(settings.db.connection_string)
        yield conn
    except psycopg2.OperationalError as e:
        print(f"[bold red]Database connection error: {e}[/bold red]")
        raise
    finally:
        if conn:
            conn.close()


class DatabaseAdapter(ABC):
    @abstractmethod
    def check_connection(self) -> None:
        """Checks if a connection to the database can be established."""
        pass

    @abstractmethod
    def create_production_schema(self) -> None:
        """Creates the main production schema and tables if they don't exist."""
        pass

    @abstractmethod
    def initialize_schema(self, mode: str) -> None:
        """Prepares the database schema (e.g., main tables or staging schema)."""
        pass

    @abstractmethod
    def bulk_load_intermediate(self, file_path: Path, table_name: str) -> None:
        """Executes the native bulk load operation for a specific file."""
        pass

    @abstractmethod
    def deduplicate_staging_data(self, table_name: str, unique_key: str) -> None:
        """Removes duplicate rows from a staging table based on a unique key."""
        pass

    @abstractmethod
    def finalize_load(self, mode: str) -> None:
        """Performs post-load operations (e.g., indexing, analyzing, schema swapping, MERGE execution)."""
        pass

    @abstractmethod
    def update_metadata(self, release_info: dict[str, Any]) -> None:
        """Updates the internal metadata tables."""
        pass

    @abstractmethod
    def get_current_release_version(self) -> str | None:
        """Retrieves the version of UniProt currently loaded in the DB."""
        pass

    @abstractmethod
    def log_run(
        self,
        run_id: str,
        mode: str,
        dataset: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        error_message: str | None = None,
    ) -> None:
        """Logs a pipeline run to the history table."""
        pass


class PostgresAdapter(DatabaseAdapter):
    def __init__(
        self,
        settings: Settings,
        staging_schema: str = "uniprot_staging",
        production_schema: str = "uniprot_public",
    ) -> None:
        self.settings = settings
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        print(
            f"PostgresAdapter initialized. Staging: [cyan]{self.staging_schema}[/cyan], Production: [cyan]{self.production_schema}[/cyan]"
        )

    def check_connection(self) -> None:
        """Establishes a connection and performs a simple query."""
        with postgres_connection(self.settings) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
        print("[green]Database connection verified.[/green]")

    def create_production_schema(self) -> None:
        """Creates the production schema and tables if they don't exist."""
        with postgres_connection(self.settings) as conn, conn.cursor() as cur:
            self._create_production_schema_if_not_exists(cur)
            conn.commit()

    def _get_schema_ddl(self, schema_name: str) -> str:
        """
        Reads the main schema DDL from the corresponding .sql file and substitutes the schema name.
        """
        sql_template = (
            importlib.resources.files("py_load_uniprot.sql")
            .joinpath("create_schema.sql")
            .read_text()
        )
        return sql_template.replace("__{SCHEMA_NAME}__", schema_name)

    def _get_indexes_ddl(self, schema_name: str) -> str:
        """
        Reads the indexes DDL from the corresponding .sql file and substitutes the schema name.
        """
        sql_template = (
            importlib.resources.files("py_load_uniprot.sql")
            .joinpath("create_indexes.sql")
            .read_text()
        )
        return sql_template.replace("__{SCHEMA_NAME}__", schema_name)

    def initialize_schema(self, mode: str) -> None:
        print(
            f"Initializing database schema in [cyan]'{self.staging_schema}'[/cyan] for mode '{mode}'..."
        )
        with postgres_connection(self.settings) as conn, conn.cursor() as cur:
            # In both full and delta modes, we start with a clean staging schema
            cur.execute(f"DROP SCHEMA IF EXISTS {self.staging_schema} CASCADE;")
            cur.execute(self._get_schema_ddl(self.staging_schema))
            conn.commit()
        print("[green]Staging schema initialized successfully.[/green]")

    def bulk_load_intermediate(self, file_path: Path, table_name: str) -> None:
        """
        Loads a single intermediate TSV.gz file into a table in the staging schema
        using a direct COPY. De-duplication is handled in a separate step.
        """
        self._direct_copy_load(file_path, table_name)

    def _direct_copy_load(self, file_path: Path, table_name: str) -> None:
        target = f"{self.staging_schema}.{table_name}"
        print(f"Performing direct COPY for '{table_name}'...")
        with (
            postgres_connection(self.settings) as conn,
            conn.cursor() as cur,
            gzip.open(file_path, "rt", encoding="utf-8") as f,
        ):
            header = f.readline().strip().split("\t")
            # Use copy_expert for performance and to handle streaming data
            cur.copy_expert(
                f"COPY {target} ({','.join(header)}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', HEADER false)",
                f,
            )
            conn.commit()

    def deduplicate_staging_data(self, table_name: str, unique_key: str) -> None:
        """
        Removes duplicate rows from a staging table based on a unique key,
        keeping the first row encountered.
        """
        print(
            f"De-duplicating data in staging table '{table_name}' on key '{unique_key}'..."
        )
        # This CTE-based DELETE is highly efficient in PostgreSQL for removing duplicates.
        # It identifies all rows but the first one for each unique key and deletes them.
        sql = f"""
        WITH numbered_rows AS (
            SELECT ctid,
                   row_number() OVER (PARTITION BY {unique_key} ORDER BY ctid) as rn
            FROM {self.staging_schema}.{table_name}
        )
        DELETE FROM {self.staging_schema}.{table_name}
        WHERE ctid IN (SELECT ctid FROM numbered_rows WHERE rn > 1);
        """
        with postgres_connection(self.settings) as conn, conn.cursor() as cur:
            cur.execute(sql)
            deleted_count = cur.rowcount
            conn.commit()
        if deleted_count > 0:
            print(
                f"Removed [bold yellow]{deleted_count}[/bold yellow] duplicate row(s) from '{table_name}'."
            )
        else:
            print(f"No duplicate rows found in '{table_name}'.")

    def _create_indexes(self, cur: cursor) -> None:
        print(f"Creating indexes on schema '{self.staging_schema}'...")
        cur.execute(self._get_indexes_ddl(self.staging_schema))
        print("[green]Indexes created successfully.[/green]")

    def _analyze_schema(self, cur: cursor) -> None:
        print(f"Running ANALYZE on schema '{self.staging_schema}'...")
        cur.execute(f"ANALYZE {self.staging_schema}.proteins;")
        cur.execute(f"ANALYZE {self.staging_schema}.taxonomy;")
        # Add other tables as needed for performance
        print("[green]ANALYZE complete.[/green]")

    def finalize_load(self, mode: str) -> None:
        if mode == "full":
            self._finalize_full_load()
        elif mode == "delta":
            self._finalize_delta_load()
        else:
            print(
                f"[bold red]Unsupported load mode '{mode}' in finalize_load.[/bold red]"
            )

    def _finalize_full_load(self) -> None:
        print(
            "[bold blue]Finalizing full load: creating indexes and performing schema swap...[/bold blue]"
        )
        with postgres_connection(self.settings) as conn, conn.cursor() as cur:
            self._create_indexes(cur)
            self._analyze_schema(cur)

            # Perform the atomic swap
            print("Performing atomic schema swap...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_schema_name = (
                f"{self.production_schema}_old_{timestamp}_{uuid.uuid4().hex[:8]}"
            )

            cur.execute(
                "SELECT 1 FROM pg_namespace WHERE nspname = %s",
                (self.production_schema,),
            )
            if cur.fetchone():
                print(
                    f"Archiving existing schema '{self.production_schema}' to '{archive_schema_name}'..."
                )
                cur.execute(
                    f"ALTER SCHEMA {self.production_schema} RENAME TO {archive_schema_name};"
                )

            print(
                f"Activating new schema by renaming '{self.staging_schema}' to '{self.production_schema}'..."
            )
            cur.execute(
                f"ALTER SCHEMA {self.staging_schema} RENAME TO {self.production_schema};"
            )
            # Now that the new production schema is live, create the metadata tables in it
            self._create_metadata_tables(cur, self.production_schema)
            conn.commit()
        print(
            f"[bold green]Schema swap complete. '{self.production_schema}' is now live.[/bold green]"
        )

    def _finalize_delta_load(self) -> None:
        print(
            "[bold blue]Finalizing delta load: merging staging into production...[/bold blue]"
        )
        with postgres_connection(self.settings) as conn, conn.cursor() as cur:
            self._create_production_schema_if_not_exists(cur)
            self._execute_delta_update(cur)
            conn.commit()
        print("[bold green]Delta load complete.[/bold green]")

    def _create_metadata_tables(self, cur: cursor, schema_name: str) -> None:
        """Creates the metadata tables in the specified schema."""
        sql_template = (
            importlib.resources.files("py_load_uniprot.sql")
            .joinpath("create_metadata_tables.sql")
            .read_text()
        )
        cur.execute(sql_template.replace("__{SCHEMA_NAME}__", schema_name))

    def _create_production_schema_if_not_exists(self, cur: cursor) -> None:
        """Creates the production schema and its tables if they don't exist."""
        print(f"Ensuring production schema '{self.production_schema}' exists...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.production_schema};")
        # Swap out the schema name to create the production tables
        production_ddl = self._get_schema_ddl(self.production_schema)
        cur.execute(production_ddl)
        self._create_metadata_tables(cur, self.production_schema)
        print("Production schema is ready.")

    def _execute_delta_update(self, cur: cursor) -> None:
        """Orchestrates the SQL operations for a delta update."""
        print("Starting delta update process...")

        # 1. Upsert core entities
        self._upsert_proteins(cur)
        self._upsert_sequences(cur)
        self._upsert_taxonomy(cur)

        # 2. Sync child tables (Delete old, insert new)
        self._sync_child_table(
            cur, "accessions", ["protein_accession", "secondary_accession"]
        )
        self._sync_child_table(cur, "genes", ["protein_accession", "gene_name"])
        self._sync_child_table(cur, "keywords", ["protein_accession", "keyword_id"])
        self._sync_child_table(
            cur, "protein_to_go", ["protein_accession", "go_term_id"]
        )
        self._sync_child_table(
            cur, "protein_to_taxonomy", ["protein_accession", "ncbi_taxid"]
        )

        # 3. Handle deleted proteins
        self._delete_removed_proteins(cur)

        # 4. Analyze updated tables for performance
        print("Running ANALYZE on production schema...")
        cur.execute(f"ANALYZE {self.production_schema}.proteins;")
        cur.execute(f"ANALYZE {self.production_schema}.sequences;")
        cur.execute(f"ANALYZE {self.production_schema}.taxonomy;")

    def _upsert_proteins(self, cur: cursor) -> None:
        print("Upserting proteins...")
        sql = f"""
        INSERT INTO {self.production_schema}.proteins
        SELECT * FROM {self.staging_schema}.proteins
        ON CONFLICT (primary_accession) DO UPDATE SET
            uniprot_id = EXCLUDED.uniprot_id,
            sequence_length = EXCLUDED.sequence_length,
            molecular_weight = EXCLUDED.molecular_weight,
            modified_date = EXCLUDED.modified_date,
            comments_data = EXCLUDED.comments_data,
            features_data = EXCLUDED.features_data,
            db_references_data = EXCLUDED.db_references_data,
            evidence_data = EXCLUDED.evidence_data;
        """
        cur.execute(sql)
        print(f"{cur.rowcount} proteins upserted.")

    def _upsert_sequences(self, cur: cursor) -> None:
        print("Upserting sequences...")
        sql = f"""
        INSERT INTO {self.production_schema}.sequences
        SELECT * FROM {self.staging_schema}.sequences
        ON CONFLICT (primary_accession) DO UPDATE SET
            sequence = EXCLUDED.sequence;
        """
        cur.execute(sql)
        print(f"{cur.rowcount} sequences upserted.")

    def _upsert_taxonomy(self, cur: cursor) -> None:
        print("Upserting taxonomy...")
        sql = f"""
        INSERT INTO {self.production_schema}.taxonomy
        SELECT * FROM {self.staging_schema}.taxonomy
        ON CONFLICT (ncbi_taxid) DO UPDATE SET
            scientific_name = EXCLUDED.scientific_name,
            lineage = EXCLUDED.lineage;
        """
        cur.execute(sql)
        print(f"{cur.rowcount} taxonomy terms upserted.")

    def _sync_child_table(
        self, cur: cursor, table_name: str, primary_keys: list[str]
    ) -> None:
        print(f"Syncing child table: {table_name}...")
        pk_string = ", ".join(primary_keys)

        # Delete records for updated proteins that are no longer valid
        delete_sql = f"""
        DELETE FROM {self.production_schema}.{table_name} prod
        WHERE prod.protein_accession IN (SELECT primary_accession FROM {self.staging_schema}.proteins)
          AND NOT EXISTS (
            SELECT 1 FROM {self.staging_schema}.{table_name} stage
            WHERE ({pk_string}) = ({", ".join([f'prod.{k}' for k in primary_keys])})
        );
        """
        cur.execute(delete_sql)
        print(f"  - {cur.rowcount} old records deleted.")

        # Insert new records, ignoring ones that already exist
        insert_sql = f"""
        INSERT INTO {self.production_schema}.{table_name}
        SELECT * FROM {self.staging_schema}.{table_name}
        ON CONFLICT ({pk_string}) DO NOTHING;
        """
        cur.execute(insert_sql)
        print(f"  - {cur.rowcount} new records inserted.")

    def _delete_removed_proteins(self, cur: cursor) -> None:
        print("Identifying and deleting removed proteins...")
        # Find proteins in production that are NOT in staging
        find_deleted_sql = f"""
        SELECT prod.primary_accession
        FROM {self.production_schema}.proteins prod
        LEFT JOIN {self.staging_schema}.proteins stage
        ON prod.primary_accession = stage.primary_accession
        WHERE stage.primary_accession IS NULL;
        """
        cur.execute(find_deleted_sql)
        deleted_accessions = [row[0] for row in cur.fetchall()]

        if not deleted_accessions:
            print("No proteins to delete.")
            return

        print(f"Found {len(deleted_accessions)} proteins to delete.")
        # The CASCADE on the foreign key will handle deletion from all child tables
        delete_sql = f"DELETE FROM {self.production_schema}.proteins WHERE primary_accession = ANY(%s);"
        cur.execute(delete_sql, (deleted_accessions,))
        print(
            f"{cur.rowcount} proteins and their related data deleted from production."
        )

    def update_metadata(self, release_info: dict[str, Any]) -> None:
        """
        Updates the metadata table with the new release information.
        This should be called after a successful load and schema swap.
        """
        print(
            f"Updating metadata for release [cyan]{release_info['version']}[/cyan]..."
        )
        # This table should only ever contain one row: the current release.
        # We truncate it before inserting the new record to enforce this.
        truncate_sql = (
            f"TRUNCATE TABLE {self.production_schema}.py_load_uniprot_metadata;"
        )
        insert_sql = f"""
        INSERT INTO {self.production_schema}.py_load_uniprot_metadata (
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
        """
        with postgres_connection(self.settings) as conn, conn.cursor() as cur:
            # Ensure the table exists before trying to truncate/insert
            self._create_metadata_tables(cur, self.production_schema)
            cur.execute(truncate_sql)
            cur.execute(insert_sql, release_info)
            conn.commit()
        print("[green]Metadata table updated successfully.[/green]")

    def get_current_release_version(self) -> str | None:
        """Retrieves the version of UniProt currently loaded in the production DB."""
        print(
            f"Checking for current release version in schema [cyan]'{self.production_schema}'[/cyan]..."
        )
        sql = f"SELECT version FROM {self.production_schema}.py_load_uniprot_metadata ORDER BY load_timestamp DESC LIMIT 1;"
        try:
            with postgres_connection(self.settings) as conn, conn.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchone()
                if result:
                    version = result[0]
                    print(f"[green]Found current release version: {version}[/green]")
                    return str(version)
                else:
                    print(
                        "[yellow]No release version found in metadata table.[/yellow]"
                    )
                    return None
        except psycopg2.errors.UndefinedTable:
            print(
                f"[yellow]Metadata table not found in schema '{self.production_schema}'. Database may not be initialized.[/yellow]"
            )
            return None

    def log_run(
        self,
        run_id: str,
        mode: str,
        dataset: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        error_message: str | None = None,
    ) -> None:
        """
        Inserts a single, complete record into the load_history table.
        """
        color = "green" if status == "COMPLETED" else "red"
        print(
            f"Logging pipeline run for run_id: [cyan]{run_id}[/cyan] with status [{color}][bold]{status}[/bold][/{color}]"
        )
        sql = f"""
        INSERT INTO {self.production_schema}.load_history (
            run_id, status, mode, dataset, start_time, end_time, error_message
        ) VALUES (
            %(run_id)s, %(status)s, %(mode)s, %(dataset)s, %(start_time)s, %(end_time)s, %(error_message)s
        );
        """
        try:
            with postgres_connection(self.settings) as conn, conn.cursor() as cur:
                # Ensure the production schema and table exist before logging
                self._create_production_schema_if_not_exists(cur)
                cur.execute(
                    sql,
                    {
                        "run_id": run_id,
                        "status": status,
                        "mode": mode,
                        "dataset": dataset,
                        "start_time": start_time,
                        "end_time": end_time,
                        "error_message": error_message,
                    },
                )
                conn.commit()
        except Exception as e:
            print(f"[bold red]Failed to log pipeline run: {e}[/bold red]")
