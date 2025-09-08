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
        if mode != 'full':
            print(f"[yellow]Finalize step for mode '{mode}' not implemented. Skipping.[/yellow]")
            return

        print("[bold blue]Finalizing full load: creating indexes and performing schema swap...[/bold blue]")
        with postgres_connection() as conn, conn.cursor() as cur:
            self._create_indexes(cur)
            self._analyze_schema(cur)

            # Perform the atomic swap
            print("Performing atomic schema swap...")
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_schema_name = f"{self.production_schema}_old_{timestamp}"

            # Check if production schema exists before trying to rename it
            cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (self.production_schema,))
            if cur.fetchone():
                print(f"Archiving existing schema '{self.production_schema}' to '{archive_schema_name}'...")
                cur.execute(f"ALTER SCHEMA {self.production_schema} RENAME TO {archive_schema_name};")

            print(f"Activating new schema by renaming '{self.staging_schema}' to '{self.production_schema}'...")
            cur.execute(f"ALTER SCHEMA {self.staging_schema} RENAME TO {self.production_schema};")

            conn.commit()
            print(f"[bold green]Schema swap complete. '{self.production_schema}' is now live.[/bold green]")

    def get_current_release_version(self) -> str | None:
        print("[yellow]Get current release version not yet implemented.[/yellow]")
        return None
