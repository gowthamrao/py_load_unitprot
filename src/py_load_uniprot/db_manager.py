import gzip
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
import psycopg2
from rich import print
import datetime

from py_load_uniprot.config import settings
from py_load_uniprot.transformer import TABLE_HEADERS

TABLE_LOAD_ORDER = [
    'taxonomy', 'proteins', 'sequences', 'accessions', 'genes',
    'keywords', 'protein_to_go', 'protein_to_taxonomy'
]
TABLES_WITH_UNIQUE_CONSTRAINTS = {'taxonomy': 'ncbi_taxid'}

@contextmanager
def postgres_connection():
    conn = None
    try:
        conn = psycopg2.connect(settings.db_connection_string)
        yield conn
    except psycopg2.OperationalError as e:
        print(f"[bold red]Database connection error: {e}[/bold red]")
        raise
    finally:
        if conn:
            conn.close()

class DatabaseAdapter(ABC):
    @abstractmethod
    def initialize_schema(self) -> None: pass
    @abstractmethod
    def load_transformed_data(self, intermediate_dir: Path) -> None: pass
    @abstractmethod
    def finalize_load(self, mode: str) -> None: pass
    @abstractmethod
    def get_current_release_version(self) -> str | None: pass

class PostgresAdapter(DatabaseAdapter):
    def __init__(self, staging_schema: str = "uniprot_staging", production_schema: str = "uniprot_public"):
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        print(f"PostgresAdapter initialized. Staging: [cyan]{self.staging_schema}[/cyan], Production: [cyan]{self.production_schema}[/cyan]")

    def _get_schema_ddl(self) -> str:
        # Same as before, but good to have it defined once
        return f"""
        CREATE SCHEMA IF NOT EXISTS {self.staging_schema};
        CREATE TABLE {self.staging_schema}.py_load_uniprot_metadata ( release_version VARCHAR(255) PRIMARY KEY, release_date DATE, load_timestamp TIMESTAMPTZ DEFAULT NOW(), swissprot_entry_count INTEGER, trembl_entry_count INTEGER );
        CREATE TABLE {self.staging_schema}.proteins ( primary_accession VARCHAR(255) PRIMARY KEY, uniprot_id VARCHAR(255), sequence_length INTEGER, molecular_weight INTEGER, created_date DATE, modified_date DATE, comments_data JSONB, features_data JSONB, db_references_data JSONB );
        CREATE TABLE {self.staging_schema}.sequences ( primary_accession VARCHAR(255) PRIMARY KEY, sequence TEXT, FOREIGN KEY (primary_accession) REFERENCES {self.staging_schema}.proteins(primary_accession) ON DELETE CASCADE );
        CREATE TABLE {self.staging_schema}.accessions ( protein_accession VARCHAR(255), secondary_accession VARCHAR(255), PRIMARY KEY (protein_accession, secondary_accession), FOREIGN KEY (protein_accession) REFERENCES {self.staging_schema}.proteins(primary_accession) ON DELETE CASCADE );
        CREATE TABLE {self.staging_schema}.taxonomy ( ncbi_taxid INTEGER PRIMARY KEY, scientific_name VARCHAR(1023), lineage TEXT );
        CREATE TABLE {self.staging_schema}.genes ( protein_accession VARCHAR(255), gene_name VARCHAR(255), is_primary BOOLEAN, PRIMARY KEY (protein_accession, gene_name), FOREIGN KEY (protein_accession) REFERENCES {self.staging_schema}.proteins(primary_accession) ON DELETE CASCADE );
        CREATE TABLE {self.staging_schema}.keywords ( protein_accession VARCHAR(255), keyword_id VARCHAR(255), keyword_label VARCHAR(255), PRIMARY KEY (protein_accession, keyword_id), FOREIGN KEY (protein_accession) REFERENCES {self.staging_schema}.proteins(primary_accession) ON DELETE CASCADE );
        CREATE TABLE {self.staging_schema}.protein_to_go ( protein_accession VARCHAR(255), go_term_id VARCHAR(255), PRIMARY KEY (protein_accession, go_term_id), FOREIGN KEY (protein_accession) REFERENCES {self.staging_schema}.proteins(primary_accession) ON DELETE CASCADE );
        CREATE TABLE {self.staging_schema}.protein_to_taxonomy ( protein_accession VARCHAR(255), ncbi_taxid INTEGER, PRIMARY KEY (protein_accession, ncbi_taxid), FOREIGN KEY (protein_accession) REFERENCES {self.staging_schema}.proteins(primary_accession) ON DELETE CASCADE, FOREIGN KEY (ncbi_taxid) REFERENCES {self.staging_schema}.taxonomy(ncbi_taxid) );
        """

    def _get_indexes_ddl(self) -> str:
        return f"""
        -- B-Tree Indexes for foreign keys and common lookups
        CREATE INDEX IF NOT EXISTS idx_proteins_uniprot_id ON {self.staging_schema}.proteins (uniprot_id);
        CREATE INDEX IF NOT EXISTS idx_accessions_secondary ON {self.staging_schema}.accessions (secondary_accession);
        CREATE INDEX IF NOT EXISTS idx_genes_name ON {self.staging_schema}.genes (gene_name);
        CREATE INDEX IF NOT EXISTS idx_keywords_label ON {self.staging_schema}.keywords (keyword_label);
        CREATE INDEX IF NOT EXISTS idx_prot_to_go_id ON {self.staging_schema}.protein_to_go (go_term_id);
        CREATE INDEX IF NOT EXISTS idx_prot_to_taxo_id ON {self.staging_schema}.protein_to_taxonomy (ncbi_taxid);

        -- GIN Indexes for JSONB columns
        CREATE INDEX IF NOT EXISTS idx_proteins_comments_gin ON {self.staging_schema}.proteins USING GIN (comments_data);
        CREATE INDEX IF NOT EXISTS idx_proteins_features_gin ON {self.staging_schema}.proteins USING GIN (features_data);
        CREATE INDEX IF NOT EXISTS idx_proteins_db_refs_gin ON {self.staging_schema}.proteins USING GIN (db_references_data);
        """

    def initialize_schema(self) -> None:
        print(f"Initializing database schema in [cyan]'{self.staging_schema}'[/cyan]...")
        with postgres_connection() as conn, conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {self.staging_schema} CASCADE;")
            cur.execute(self._get_schema_ddl())
            conn.commit()
        print("[green]Staging schema initialized successfully.[/green]")

    def load_transformed_data(self, intermediate_dir: Path) -> None:
        for table_name in TABLE_LOAD_ORDER:
            file_path = intermediate_dir / f"{table_name}.tsv.gz"
            if file_path.exists():
                self._bulk_load_intermediate(file_path, table_name)
            else:
                print(f"[yellow]Warning: No data file for '{table_name}'. Skipping.[/yellow]")

    def _bulk_load_intermediate(self, file_path: Path, table_name: str):
        unique_key = TABLES_WITH_UNIQUE_CONSTRAINTS.get(table_name)
        if unique_key:
            self._safe_upsert_load(file_path, table_name, unique_key)
        else:
            self._direct_copy_load(file_path, table_name)

    def _direct_copy_load(self, file_path: Path, table_name: str):
        target = f"{self.staging_schema}.{table_name}"
        print(f"Performing direct COPY for '{table_name}'...")
        with postgres_connection() as conn, conn.cursor() as cur, gzip.open(file_path, 'rt', encoding='utf-8') as f:
            header = f.readline().strip().split('\t')
            cur.copy_expert(f"COPY {target} ({','.join(header)}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', HEADER false)", f)
            conn.commit()

    def _safe_upsert_load(self, file_path: Path, table_name: str, unique_key: str):
        target = f"{self.staging_schema}.{table_name}"
        temp_table = f"temp_{table_name}"
        print(f"Performing safe upsert for '{table_name}'...")
        with postgres_connection() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE TEMP TABLE {temp_table} ON COMMIT DROP AS TABLE {target} WITH NO DATA;")
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                header = f.readline().strip().split('\t')
                cur.copy_expert(f"COPY {temp_table} ({','.join(header)}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', HEADER false)", f)
            cols = TABLE_HEADERS[table_name]
            cur.execute(f"INSERT INTO {target} ({', '.join(cols)}) SELECT * FROM {temp_table} ON CONFLICT ({unique_key}) DO NOTHING;")
            conn.commit()

    def _create_indexes(self, cur):
        print("Creating indexes on staging schema...")
        cur.execute(self._get_indexes_ddl())
        print("[green]Indexes created successfully.[/green]")

    def _analyze_schema(self, cur):
        print(f"Running ANALYZE on schema '{self.staging_schema}'...")
        cur.execute(f"ANALYZE {self.staging_schema}.proteins;")
        cur.execute(f"ANALYZE {self.staging_schema}.taxonomy;")
        # Add other tables as needed for performance
        print("[green]ANALYZE complete.[/green]")

    def finalize_load(self, mode: str) -> None:
        if mode == 'full':
            self._finalize_full_load()
        elif mode == 'delta':
            self._finalize_delta_load()
        else:
            print(f"[bold red]Unsupported load mode '{mode}' in finalize_load.[/bold red]")

    def _finalize_full_load(self) -> None:
        print("[bold blue]Finalizing full load: creating indexes and performing schema swap...[/bold blue]")
        with postgres_connection() as conn, conn.cursor() as cur:
            self._create_indexes(cur)
            self._analyze_schema(cur)

            # Perform the atomic swap
            print("Performing atomic schema swap...")
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_schema_name = f"{self.production_schema}_old_{timestamp}"

            cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (self.production_schema,))
            if cur.fetchone():
                print(f"Archiving existing schema '{self.production_schema}' to '{archive_schema_name}'...")
                cur.execute(f"ALTER SCHEMA {self.production_schema} RENAME TO {archive_schema_name};")

            print(f"Activating new schema by renaming '{self.staging_schema}' to '{self.production_schema}'...")
            cur.execute(f"ALTER SCHEMA {self.staging_schema} RENAME TO {self.production_schema};")
            conn.commit()
        print(f"[bold green]Schema swap complete. '{self.production_schema}' is now live.[/bold green]")

    def _finalize_delta_load(self) -> None:
        print("[bold blue]Finalizing delta load: merging staging into production...[/bold blue]")
        with postgres_connection() as conn, conn.cursor() as cur:
            self._create_production_schema_if_not_exists(cur)
            self._execute_delta_update(cur)
            conn.commit()
        print("[bold green]Delta load complete.[/bold green]")

    def _create_production_schema_if_not_exists(self, cur) -> None:
        """Creates the production schema and its tables if they don't exist."""
        print(f"Ensuring production schema '{self.production_schema}' exists...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.production_schema};")
        # Swap out the schema name to create the production tables
        production_ddl = self._get_schema_ddl().replace(self.staging_schema, self.production_schema)
        cur.execute(production_ddl)
        print("Production schema is ready.")

    def _execute_delta_update(self, cur) -> None:
        """Orchestrates the SQL operations for a delta update."""
        print("Starting delta update process...")

        # 1. Upsert core entities
        self._upsert_proteins(cur)
        self._upsert_sequences(cur)
        self._upsert_taxonomy(cur)

        # 2. Sync child tables (Delete old, insert new)
        self._sync_child_table(cur, 'accessions', ['protein_accession', 'secondary_accession'])
        self._sync_child_table(cur, 'genes', ['protein_accession', 'gene_name'])
        self._sync_child_table(cur, 'keywords', ['protein_accession', 'keyword_id'])
        self._sync_child_table(cur, 'protein_to_go', ['protein_accession', 'go_term_id'])
        self._sync_child_table(cur, 'protein_to_taxonomy', ['protein_accession', 'ncbi_taxid'])

        # 3. Handle deleted proteins
        self._delete_removed_proteins(cur)

        # 4. Analyze updated tables for performance
        print("Running ANALYZE on production schema...")
        cur.execute(f"ANALYZE {self.production_schema}.proteins;")
        cur.execute(f"ANALYZE {self.production_schema}.sequences;")
        cur.execute(f"ANALYZE {self.production_schema}.taxonomy;")

    def _upsert_proteins(self, cur) -> None:
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
            db_references_data = EXCLUDED.db_references_data;
        """
        cur.execute(sql)
        print(f"{cur.rowcount} proteins upserted.")

    def _upsert_sequences(self, cur) -> None:
        print("Upserting sequences...")
        sql = f"""
        INSERT INTO {self.production_schema}.sequences
        SELECT * FROM {self.staging_schema}.sequences
        ON CONFLICT (primary_accession) DO UPDATE SET
            sequence = EXCLUDED.sequence;
        """
        cur.execute(sql)
        print(f"{cur.rowcount} sequences upserted.")

    def _upsert_taxonomy(self, cur) -> None:
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

    def _sync_child_table(self, cur, table_name: str, primary_keys: list[str]):
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

    def _delete_removed_proteins(self, cur) -> None:
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
        print(f"{cur.rowcount} proteins and their related data deleted from production.")

    def get_current_release_version(self) -> str | None:
        print("[yellow]Get current release version not yet implemented.[/yellow]")
        return None
