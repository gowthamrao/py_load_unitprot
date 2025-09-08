import pytest
import gzip
from pathlib import Path
import psycopg2

from py_load_uniprot import transformer
from py_load_uniprot.db_manager import PostgresAdapter, postgres_connection

# A more comprehensive XML sample for integration testing
SAMPLE_XML_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <accession>Q9Y5Y5</accession>
  <name>TEST1_HUMAN</name>
  <protein>
    <recommendedName><fullName>Test protein 1</fullName></recommendedName>
  </protein>
  <gene><name type="primary">TP1</name></gene>
  <organism>
    <name type="scientific">Homo sapiens</name>
    <dbReference type="NCBI Taxonomy" id="9606"/>
    <lineage><taxon>Eukaryota</taxon><taxon>Metazoa</taxon></lineage>
  </organism>
  <dbReference type="GO" id="GO:0005515"/>
  <keyword id="KW-0181">Complete proteome</keyword>
  <comment type="function"><text>Enables testing.</text></comment>
  <feature type="chain" description="Test protein 1" id="PRO_0000021325">
    <location><begin position="1"/><end position="10"/></location>
  </feature>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
</entry>
<entry dataset="TrEMBL" created="2010-10-12" modified="2024-07-18" version="100">
  <accession>P67890</accession>
  <name>TEST2_MOUSE</name>
  <protein>
    <recommendedName><fullName>Test protein 2</fullName></recommendedName>
  </protein>
  <organism>
    <name type="scientific">Mus musculus</name>
    <dbReference type="NCBI Taxonomy" id="10090"/>
    <lineage><taxon>Eukaryota</taxon><taxon>Metazoa</taxon></lineage>
  </organism>
  <sequence length="12" mass="2222">MTESTSEQBBBB</sequence>
</entry>
</uniprot>
"""

@pytest.fixture
def sample_xml_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample XML file for testing."""
    xml_path = tmp_path / "sample.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_CONTENT)
    return xml_path

@pytest.fixture
def db_adapter() -> PostgresAdapter:
    """
    Provides a PostgresAdapter configured for integration testing and handles cleanup.
    """
    adapter = PostgresAdapter(
        staging_schema="integration_test_staging",
        production_schema="integration_test_public"
    )
    yield adapter
    # Cleanup: drop the production schema after the test
    print("Tearing down integration test schema...")
    try:
        with postgres_connection() as conn, conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {adapter.production_schema} CASCADE;")
            conn.commit()
        print("Test schema torn down successfully.")
    except psycopg2.Error as e:
        print(f"Error during teardown: {e}")


@pytest.mark.skip(reason="Requires a running PostgreSQL instance for integration testing.")
def test_full_etl_pipeline(sample_xml_file: Path, db_adapter: PostgresAdapter, tmp_path: Path):
    """
    Tests the full ETL pipeline: Transform -> Initialize -> Load -> Finalize.
    """
    # Arrange
    output_dir = tmp_path / "transformed_output"

    # --- Act ---
    # 1. Transform the XML to intermediate TSV files
    transformer.transform_xml_to_tsv(sample_xml_file, output_dir)

    # 2. Initialize the staging schema
    db_adapter.initialize_schema()

    # 3. Load the intermediate files into the staging schema
    db_adapter.load_transformed_data(output_dir)

    # 4. Finalize the load (create indexes and swap schemas)
    db_adapter.finalize_load(mode='full')

    # --- Assert ---
    # Check that the final production schema and its tables exist
    with postgres_connection() as conn, conn.cursor() as cur:
        # Check if schema exists
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (db_adapter.production_schema,))
        assert cur.fetchone() is not None, "Production schema should exist"

        # Check if staging schema was cleaned up by the rename
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (db_adapter.staging_schema,))
        assert cur.fetchone() is None, "Staging schema should have been renamed"

        # Check row counts in key tables
        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2

        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.accessions")
        assert cur.fetchone()[0] == 1 # Only one secondary accession in the sample

        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.taxonomy")
        assert cur.fetchone()[0] == 2

        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.protein_to_taxonomy")
        assert cur.fetchone()[0] == 2

        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.genes")
        assert cur.fetchone()[0] == 1

        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.protein_to_go")
        assert cur.fetchone()[0] == 1

        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.keywords")
        assert cur.fetchone()[0] == 1

        # Verify specific data points
        cur.execute(f"SELECT uniprot_id FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'")
        assert cur.fetchone()[0] == 'TEST1_HUMAN'

        # Verify JSONB data was loaded
        cur.execute(f"SELECT comments_data -> 0 ->> 'tag' FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'")
        assert cur.fetchone()[0] == 'comment'

        cur.execute(f"SELECT features_data -> 0 ->> 'tag' FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'")
        assert cur.fetchone()[0] == 'feature'
