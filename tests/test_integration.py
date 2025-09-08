import pytest
import gzip
from pathlib import Path
import psycopg2

from typer.testing import CliRunner
from py_load_uniprot import transformer
from py_load_uniprot.cli import app
from py_load_uniprot.db_manager import PostgresAdapter, postgres_connection, TABLE_LOAD_ORDER
import datetime

runner = CliRunner()

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


# V2: P12345 is modified, P67890 is deleted, A0A0A0 is new
SAMPLE_XML_V2_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2025-01-01" version="151">
  <accession>P12345</accession>
  <accession>Q9Y5Y5</accession>
  <name>TEST1_HUMAN_UPDATED</name>
  <protein>
    <recommendedName><fullName>Test protein 1 - Updated</fullName></recommendedName>
  </protein>
  <gene><name type="primary">TP1_UPDATED</name></gene>
  <organism>
    <name type="scientific">Homo sapiens</name>
    <dbReference type="NCBI Taxonomy" id="9606"/>
    <lineage><taxon>Eukaryota</taxon><taxon>Metazoa</taxon></lineage>
  </organism>
  <sequence length="11" mass="1112">MTESTSEQAAX</sequence>
</entry>
<entry dataset="Swiss-Prot" created="2025-01-01" modified="2025-01-01" version="1">
  <accession>A0A0A0</accession>
  <name>TEST3_NEW</name>
  <protein>
    <recommendedName><fullName>Test protein 3 - New</fullName></recommendedName>
  </protein>
  <organism>
    <name type="scientific">Pan troglodytes</name>
    <dbReference type="NCBI Taxonomy" id="9598"/>
    <lineage><taxon>Eukaryota</taxon><taxon>Metazoa</taxon></lineage>
  </organism>
  <sequence length="5" mass="555">MNEWP</sequence>
</entry>
</uniprot>
"""

@pytest.fixture
def sample_xml_v2_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample V2 XML file for delta load testing."""
    xml_path = tmp_path / "sample_v2.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_V2_CONTENT)
    return xml_path


def test_full_etl_pipeline(sample_xml_file: Path, db_adapter: PostgresAdapter, tmp_path: Path):
    """
    Tests the full ETL pipeline: Transform -> Initialize -> Load -> Finalize -> Metadata.
    This test requires a running PostgreSQL instance.
    """
    # Arrange
    output_dir = tmp_path / "transformed_output"
    release_info = {
        "release_version": "2025_TEST",
        "release_date": datetime.date(2025, 1, 31),
        "swissprot_entry_count": 1,
        "trembl_entry_count": 1,
    }

    # --- Act ---
    # 1. Transform
    transformer.transform_xml_to_tsv(sample_xml_file, output_dir)

    # 2. Initialize
    db_adapter.initialize_schema(mode='full')

    # 3. Load
    for table_name in TABLE_LOAD_ORDER:
        file_path = output_dir / f"{table_name}.tsv.gz"
        if file_path.exists():
            db_adapter.bulk_load_intermediate(file_path, table_name)

    # 4. Finalize
    db_adapter.finalize_load(mode='full')

    # 5. Update Metadata
    db_adapter.update_metadata(release_info)


    # --- Assert ---
    with postgres_connection() as conn, conn.cursor() as cur:
        # Assert schema and table structure
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (db_adapter.production_schema,))
        assert cur.fetchone() is not None, "Production schema should exist"
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (db_adapter.staging_schema,))
        assert cur.fetchone() is None, "Staging schema should have been renamed"

        # Assert data integrity
        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2
        cur.execute(f"SELECT uniprot_id FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'")
        assert cur.fetchone()[0] == 'TEST1_HUMAN'

        # Assert Metadata
        cur.execute(f"SELECT release_version, release_date FROM {db_adapter.production_schema}.py_load_uniprot_metadata")
        metadata_row = cur.fetchone()
        assert metadata_row is not None, "Metadata row should exist"
        assert metadata_row[0] == "2025_TEST"
        assert metadata_row[1] == datetime.date(2025, 1, 31)


def test_delta_load_pipeline(sample_xml_file: Path, sample_xml_v2_file: Path, db_adapter: PostgresAdapter, tmp_path: Path):
    """
    Tests the delta load functionality by performing a full load, then a delta load,
    and verifying the state of the database after each step.
    """
    # Arrange
    output_dir_v1 = tmp_path / "transformed_v1"
    output_dir_v2 = tmp_path / "transformed_v2"

    # --- Act 1: Initial Full Load (V1) ---
    print("--- Running Initial Full Load (V1) ---")
    transformer.transform_xml_to_tsv(sample_xml_file, output_dir_v1)
    db_adapter.initialize_schema(mode='full')
    for table_name in TABLE_LOAD_ORDER:
        file_path = output_dir_v1 / f"{table_name}.tsv.gz"
        if file_path.exists():
            db_adapter.bulk_load_intermediate(file_path, table_name)
    db_adapter.finalize_load(mode='full')
    db_adapter.update_metadata({"release_version": "V1_TEST", "release_date": datetime.date(2024, 1, 1), "swissprot_entry_count": 1, "trembl_entry_count": 1})
    print("--- Full Load (V1) Complete ---")

    # --- Assert 1: State after Full Load ---
    with postgres_connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2
        cur.execute(f"SELECT uniprot_id FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'")
        assert cur.fetchone()[0] == 'TEST1_HUMAN'
        cur.execute(f"SELECT 1 FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P67890'")
        assert cur.fetchone() is not None

    # --- Act 2: Delta Load (V2) ---
    print("--- Running Delta Load (V2) ---")
    transformer.transform_xml_to_tsv(sample_xml_v2_file, output_dir_v2)
    db_adapter.initialize_schema(mode='delta') # Re-initialize STAGING schema
    for table_name in TABLE_LOAD_ORDER:
        file_path = output_dir_v2 / f"{table_name}.tsv.gz"
        if file_path.exists():
            db_adapter.bulk_load_intermediate(file_path, table_name)
    db_adapter.finalize_load(mode='delta')
    db_adapter.update_metadata({"release_version": "V2_TEST", "release_date": datetime.date(2025, 1, 1), "swissprot_entry_count": 2, "trembl_entry_count": 0})
    print("--- Delta Load (V2) Complete ---")

    # --- Assert 2: State after Delta Load ---
    with postgres_connection() as conn, conn.cursor() as cur:
        # Check total count: 2 (initial) - 1 (deleted) + 1 (new) = 2
        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2, "Total protein count should be 2 after delta."

        # Check that P12345 was updated
        cur.execute(f"SELECT uniprot_id, sequence_length FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'")
        updated_protein = cur.fetchone()
        assert updated_protein[0] == 'TEST1_HUMAN_UPDATED', "Protein P12345 should have been updated."
        assert updated_protein[1] == 11, "Sequence length for P12345 should have been updated."

        # Check that P67890 was deleted
        cur.execute(f"SELECT 1 FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P67890'")
        assert cur.fetchone() is None, "Protein P67890 should have been deleted."

        # Check that A0A0A0 was inserted
        cur.execute(f"SELECT uniprot_id FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'A0A0A0'")
        assert cur.fetchone()[0] == 'TEST3_NEW', "New protein A0A0A0 should have been inserted."

        # Check that metadata was updated to V2
        cur.execute(f"SELECT release_version FROM {db_adapter.production_schema}.py_load_uniprot_metadata")
        assert cur.fetchone()[0] == 'V2_TEST'


def test_status_command_reporting(db_adapter: PostgresAdapter, sample_xml_file: Path, tmp_path: Path):
    """
    Tests that the get_current_release_version function (used by the status command)
    reports the correct status before and after a load.
    """
    # 1. Before anything is loaded, it should return None
    version = db_adapter.get_current_release_version()
    assert version is None, "Version should be None for an uninitialized database"

    # 2. After a full load, it should return the correct version
    output_dir = tmp_path / "transformed_output"
    release_info = {
        "release_version": "2025_STATUS_TEST",
        "release_date": datetime.date(2025, 2, 1),
        "swissprot_entry_count": 1,
        "trembl_entry_count": 1,
    }
    transformer.transform_xml_to_tsv(sample_xml_file, output_dir)
    db_adapter.initialize_schema(mode='full')
    for table_name in TABLE_LOAD_ORDER:
        file_path = output_dir / f"{table_name}.tsv.gz"
        if file_path.exists():
            db_adapter.bulk_load_intermediate(file_path, table_name)
    db_adapter.finalize_load(mode='full')
    db_adapter.update_metadata(release_info)

    # Now, check the version again
    version = db_adapter.get_current_release_version()
    assert version == "2025_STATUS_TEST", "get_current_release_version should return the loaded version"
