import datetime
import gzip
from pathlib import Path

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer
from typer.testing import CliRunner

from py_load_uniprot import PyLoadUniprotPipeline, extractor, transformer
from py_load_uniprot.cli import app
from py_load_uniprot.config import Settings, load_settings
from py_load_uniprot.db_manager import (
    TABLE_LOAD_ORDER,
    PostgresAdapter,
    postgres_connection,
)

runner = CliRunner()


@pytest.fixture(scope="session")
def postgres_container():
    """
    Spins up a PostgreSQL container for the entire test session.
    """
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def sample_xml_content():
    return """<?xml version="1.0" encoding="UTF-8"?>
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
def sample_xml_file(tmp_path: Path, sample_xml_content: str) -> Path:
    """Creates a gzipped sample XML file for testing."""
    xml_path = tmp_path / "sample.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(sample_xml_content)
    return xml_path


@pytest.fixture
def settings(postgres_container: PostgresContainer) -> Settings:
    """
    Provides a Settings object configured to use the test container.
    """
    # Use load_settings which initializes a new object each time
    settings = load_settings()
    settings.db.host = postgres_container.get_container_host_ip()
    settings.db.port = int(postgres_container.get_exposed_port(5432))
    settings.db.user = postgres_container.username
    settings.db.password = postgres_container.password
    settings.db.dbname = postgres_container.dbname
    return settings


@pytest.fixture
def db_adapter(settings: Settings):
    """
    Provides a PostgresAdapter configured to use the test container.
    This fixture also handles schema cleanup after each test.
    """
    adapter = PostgresAdapter(
        settings,
        staging_schema="integration_test_staging",
        production_schema="integration_test_public",
    )
    yield adapter
    # Cleanup: drop all related schemas after the test
    print("Tearing down integration test schemas...")
    try:
        with postgres_connection(settings) as conn, conn.cursor() as cur:
            # Drop the main schemas
            cur.execute(f"DROP SCHEMA IF EXISTS {adapter.production_schema} CASCADE;")
            cur.execute(f"DROP SCHEMA IF EXISTS {adapter.staging_schema} CASCADE;")
            # Drop any archived schemas left over from full loads
            cur.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname LIKE %s;",
                (f"{adapter.production_schema}_old_%",),
            )
            for row in cur.fetchall():
                print(f"Dropping archived schema: {row[0]}")
                cur.execute(f"DROP SCHEMA IF EXISTS {row[0]} CASCADE;")
            conn.commit()
        print("Test schemas torn down successfully.")
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


def test_full_etl_pipeline_api(
    settings: Settings, sample_xml_file: Path, mocker
):
    """
    Tests the full end-to-end pipeline using the new programmatic API.
    """
    # --- Arrange ---
    # Point data_dir to the temp directory where the sample file is
    settings.data_dir = sample_xml_file.parent
    # The pipeline will look for 'uniprot_sprot.xml.gz', so we rename our sample
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_file.rename(sprot_file)

    # Mock the extractor's get_release_info to avoid network calls
    # and provide a consistent version for the test.
    mock_release_info = {
        "version": "2025_API_TEST",
        "release_date": datetime.date(2025, 1, 31),
        "swissprot_entry_count": 1,
        "trembl_entry_count": 1,
    }
    mocker.patch.object(
        extractor.Extractor, "get_release_info", return_value=mock_release_info
    )

    # --- Act ---
    # Initialize and run the pipeline
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.run(dataset="swissprot", mode="full")

    # --- Assert ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        # Assert schema and table structure
        cur.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (pipeline.db_adapter.production_schema,),
        )
        assert cur.fetchone() is not None, "Production schema should exist"
        cur.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (pipeline.db_adapter.staging_schema,),
        )
        assert cur.fetchone() is None, "Staging schema should have been renamed"

        # Assert data integrity
        cur.execute(f"SELECT COUNT(*) FROM {pipeline.db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2
        cur.execute(
            f"SELECT uniprot_id FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        assert cur.fetchone()[0] == "TEST1_HUMAN"

        # Assert Metadata
        cur.execute(
            f"SELECT version, release_date FROM {pipeline.db_adapter.production_schema}.py_load_uniprot_metadata"
        )
        metadata_row = cur.fetchone()
        assert metadata_row is not None, "Metadata row should exist"
        assert metadata_row[0] == "2025_API_TEST"
        assert metadata_row[1] == datetime.date(2025, 1, 31)


@pytest.fixture(scope="session")
def sample_xml_with_evidence_content():
    return """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <name>TEST1_HUMAN</name>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
  <evidence key="1" type="ECO:0000269">
    <source><dbReference type="PubMed" id="12345"/></source>
  </evidence>
  <feature type="chain"><location><begin position="1"/><end position="10"/></location></feature>
</entry>
</uniprot>
"""


@pytest.fixture
def sample_xml_with_evidence_file(
    tmp_path: Path, sample_xml_with_evidence_content: str
) -> Path:
    xml_path = tmp_path / "sample_with_evidence.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(sample_xml_with_evidence_content)
    return xml_path


def test_evidence_data_is_transformed_and_loaded(
    settings: Settings, sample_xml_with_evidence_file: Path, mocker
):
    """
    Tests that evidence tags are correctly parsed and loaded via the pipeline API.
    """
    # Arrange
    settings.data_dir = sample_xml_with_evidence_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_with_evidence_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "EVIDENCE_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 1,
            "trembl_entry_count": 0,
        },
    )

    # Act
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.run(dataset="swissprot", mode="full")

    # Assert
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT evidence_data FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        evidence_row = cur.fetchone()
        assert evidence_row is not None, "Protein P12345 should be loaded"

        evidence_data = evidence_row[0]
        assert isinstance(evidence_data, list), "Evidence data should be a list"
        assert len(evidence_data) == 1, "Should be one evidence element"

        # Check the content of the parsed JSON
        evidence_item = evidence_data[0]
        assert evidence_item["tag"] == "evidence"
        assert evidence_item["attributes"]["key"] == "1"
        assert evidence_item["attributes"]["type"] == "ECO:0000269"
        assert (
            evidence_item["children"][0]["children"][0]["attributes"]["type"]
            == "PubMed"
        )


def test_delta_load_pipeline(
    settings: Settings, sample_xml_file: Path, sample_xml_v2_file: Path, mocker
):
    """
    Tests the delta load functionality using the high-level pipeline API.
    """
    # --- Arrange ---
    # The pipeline will look for 'uniprot_sprot.xml.gz', so we need to manage
    # which sample file has that name at each stage.
    settings.data_dir = sample_xml_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_file.rename(sprot_file)

    pipeline = PyLoadUniprotPipeline(settings)

    # --- Act 1: Initial Full Load (V1) ---
    print("--- Running Initial Full Load (V1) ---")
    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "V1_TEST",
            "release_date": datetime.date(2024, 1, 1),
            "swissprot_entry_count": 2,
            "trembl_entry_count": 0,
        },
    )
    pipeline.run(dataset="swissprot", mode="full")
    print("--- Full Load (V1) Complete ---")

    # --- Assert 1: State after Full Load ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {pipeline.db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2
        cur.execute(
            f"SELECT uniprot_id FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        assert cur.fetchone()[0] == "TEST1_HUMAN"
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P67890'"
        )
        assert cur.fetchone() is not None

    # --- Act 2: Delta Load (V2) ---
    print("--- Running Delta Load (V2) ---")
    # Rename files so the V2 file is now the source
    sprot_file.rename(settings.data_dir / "uniprot_sprot_v1.xml.gz")
    sample_xml_v2_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "V2_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 2,
            "trembl_entry_count": 0,
        },
    )
    pipeline.run(dataset="swissprot", mode="delta")
    print("--- Delta Load (V2) Complete ---")

    # --- Assert 2: State after Delta Load ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        # Check total count: 2 (initial) - 1 (deleted) + 1 (new) = 2
        cur.execute(f"SELECT COUNT(*) FROM {pipeline.db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2, "Total protein count should be 2 after delta."

        # Check that P12345 was updated
        cur.execute(
            f"SELECT uniprot_id, sequence_length FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        updated_protein = cur.fetchone()
        assert (
            updated_protein[0] == "TEST1_HUMAN_UPDATED"
        ), "Protein P12345 should have been updated."
        assert (
            updated_protein[1] == 11
        ), "Sequence length for P12345 should have been updated."

        # Check that P67890 was deleted
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P67890'"
        )
        assert cur.fetchone() is None, "Protein P67890 should have been deleted."

        # Check that A0A0A0 was inserted
        cur.execute(
            f"SELECT uniprot_id FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'A0A0A0'"
        )
        assert (
            cur.fetchone()[0] == "TEST3_NEW"
        ), "New protein A0A0A0 should have been inserted."

        # Check that the child table (genes) was synced correctly via MERGE
        cur.execute(
            f"SELECT gene_name FROM {pipeline.db_adapter.production_schema}.genes WHERE protein_accession = 'P12345' AND is_primary = TRUE"
        )
        assert (
            cur.fetchone()[0] == "TP1_UPDATED"
        ), "Primary gene name for P12345 should have been updated in child table."

        # Check that metadata was updated to V2
        cur.execute(
            f"SELECT version FROM {pipeline.db_adapter.production_schema}.py_load_uniprot_metadata"
        )
        assert cur.fetchone()[0] == "V2_TEST"


def test_delta_load_version_check(settings: Settings, sample_xml_file: Path, mocker):
    """
    Tests that the delta load version check correctly prevents re-runs or
    running against an older version.
    """
    # --- Arrange: Perform an initial full load ---
    settings.data_dir = sample_xml_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_file.rename(sprot_file)
    pipeline = PyLoadUniprotPipeline(settings)

    release_info_v1 = {
        "version": "V1_TEST",
        "release_date": datetime.date(2024, 1, 1),
        "swissprot_entry_count": 2,
        "trembl_entry_count": 0,
    }
    mocker.patch.object(
        extractor.Extractor, "get_release_info", return_value=release_info_v1
    )
    pipeline.run(dataset="swissprot", mode="full")

    # --- Act & Assert 1: Attempting to re-run the same version ---
    # The run method should return early without making changes.
    print("--- Attempting delta load with same version ---")
    pipeline.run(dataset="swissprot", mode="delta")

    # Verify no data was changed
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT uniprot_id FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        assert cur.fetchone()[0] == "TEST1_HUMAN"  # Should be unchanged from V1

    # --- Act & Assert 2: Attempting to run an older version ---
    print("--- Attempting delta load with older version ---")
    release_info_v0 = {
        "version": "V0_TEST_OLDER",
        "release_date": datetime.date(2023, 1, 1),
        "swissprot_entry_count": 0,
        "trembl_entry_count": 0,
    }
    mocker.patch.object(
        extractor.Extractor, "get_release_info", return_value=release_info_v0
    )
    with pytest.raises(ValueError, match="Source data is older"):
        pipeline.run(dataset="swissprot", mode="delta")


def test_status_command_reporting(settings: Settings, db_adapter: PostgresAdapter):
    """
    Tests that the get_current_release_version function reports the correct status.
    """
    # 1. Before anything is loaded, it should return None
    version = db_adapter.get_current_release_version()
    assert version is None, "Version should be None for an uninitialized database"

    # 2. After a load, it should return the correct version
    # Manually create the metadata to test the read path specifically
    release_info = {
        "version": "2025_STATUS_TEST",
        "release_date": datetime.date(2025, 2, 1),
        "swissprot_entry_count": 1,
        "trembl_entry_count": 1,
    }
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        db_adapter._create_production_schema_if_not_exists(cur)
        conn.commit()

    db_adapter.update_metadata(release_info)

    # Now, check the version again
    version = db_adapter.get_current_release_version()
    assert (
        version == "2025_STATUS_TEST"
    ), "get_current_release_version should return the loaded version"


def test_cli_full_load_with_env_vars(
    postgres_container: PostgresContainer, sample_xml_file: Path, tmp_path: Path, mocker
):
    """
    Tests the full end-to-end pipeline via the CLI, configured with environment variables.
    This is the most comprehensive integration test.
    """
    # --- Arrange ---
    # 1. Create a temporary data directory and place the sample XML file in it
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    test_sprot_path = data_dir / "uniprot_sprot.xml.gz"
    with (
        gzip.open(sample_xml_file, "rb") as f_in,
        gzip.open(test_sprot_path, "wb") as f_out,
    ):
        f_out.writelines(f_in)

    # 2. Set up environment variables for the test
    env = {
        "PY_LOAD_UNIPROT_DATA_DIR": str(data_dir),
        "PY_LOAD_UNIPROT_DB__HOST": postgres_container.get_container_host_ip(),
        "PY_LOAD_UNIPROT_DB__PORT": str(postgres_container.get_exposed_port(5432)),
        "PY_LOAD_UNIPROT_DB__USER": postgres_container.username,
        "PY_LOAD_UNIPROT_DB__PASSWORD": postgres_container.password,
        "PY_LOAD_UNIPROT_DB__DBNAME": postgres_container.dbname,
    }

    # 3. Mock the extractor to prevent network calls
    mock_release_info = {
        "version": "CLI_ENV_TEST",
        "release_date": datetime.date(2025, 4, 1),
        "swissprot_entry_count": 2,
        "trembl_entry_count": 0,
    }
    mocker.patch.object(
        extractor.Extractor, "get_release_info", return_value=mock_release_info
    )

    # --- Act ---
    # Run the 'run' command via the Typer test runner
    result = runner.invoke(
        app,
        ["run", "--dataset", "swissprot", "--mode", "full"],
        env=env,
        catch_exceptions=False,
    )

    # --- Assert ---
    # 1. Check CLI output
    assert result.exit_code == 0, result.stdout
    assert "ETL pipeline completed successfully!" in result.stdout

    # 2. Check database state
    prod_schema = "uniprot_public"  # Default production schema
    with (
        psycopg2.connect(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            user=postgres_container.username,
            password=postgres_container.password,
            dbname=postgres_container.dbname,
        ) as conn,
        conn.cursor() as cur,
    ):
        # Check that the production schema exists and staging is gone
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (prod_schema,))
        assert cur.fetchone() is not None, "Production schema should exist"
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = 'uniprot_staging'")
        assert cur.fetchone() is None, "Staging schema should be gone"

        # Check that data was loaded
        cur.execute(f"SELECT COUNT(*) FROM {prod_schema}.proteins")
        assert cur.fetchone()[0] == 2

        # Check that metadata was loaded
        cur.execute(f"SELECT version FROM {prod_schema}.py_load_uniprot_metadata")
        assert cur.fetchone()[0] == "CLI_ENV_TEST"

        # Check that load history was populated correctly
        cur.execute(
            f"SELECT status, mode, dataset FROM {prod_schema}.load_history ORDER BY start_time ASC"
        )
        history_rows = cur.fetchall()
        assert len(history_rows) == 1, "Should be one history record for the run"

        status, mode, dataset = history_rows[0]
        assert status == "COMPLETED"
        assert mode == "full"
        assert dataset == "swissprot"


def test_full_etl_pipeline_with_generated_data(
    settings: Settings, mocker, tmp_path: Path
):
    """
    Tests the full pipeline using a generated data file to ensure it is
    valid and loads correctly. This specifically tests the protein-to-taxonomy link.
    This test is self-contained and does not rely on external scripts.
    """
    # --- Arrange ---
    # 1. Define the test data and write it to a temporary file
    generated_xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://uniprot.org/uniprot http://www.uniprot.org/support/docs/uniprot.xsd">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <accession>Q9Y5Y5</accession>
  <name>TEST1_HUMAN</name>
  <organism>
    <name type="scientific">Oryctolagus cuniculus</name>
    <name type="common">Rabbit</name>
    <dbReference type="NCBI Taxonomy" id="9986"/>
    <lineage>
      <taxon>Eukaryota</taxon>
      <taxon>Metazoa</taxon>
      <taxon>Chordata</taxon>
      <taxon>Craniata</taxon>
      <taxon>Vertebrata</taxon>
      <taxon>Euteleostomi</taxon>
      <taxon>Mammalia</taxon>
      <taxon>Eutheria</taxon>
      <taxon>Euarchontoglires</taxon>
      <taxon>Glires</taxon>
      <taxon>Lagomorpha</taxon>
      <taxon>Leporidae</taxon>
      <taxon>Oryctolagus</taxon>
    </lineage>
  </organism>
  <sequence length="10" mass="1111" checksum="abcde" modified="2000-05-30" version="1">MTESTSEQ</sequence>
</entry>
<entry dataset="TrEMBL" created="2010-10-12" modified="2024-07-18" version="100">
  <accession>P67890</accession>
  <name>TEST2_MOUSE</name>
  <sequence length="20" mass="2222" checksum="fghij" modified="2010-10-12" version="1">MANOTHERTESTSEQ</sequence>
</entry>
</uniprot>
"""
    data_dir = tmp_path / "gen_data"
    data_dir.mkdir()
    xml_path = data_dir / "uniprot_sprot.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(generated_xml_content)

    # 2. Configure the pipeline to use this temporary file
    settings.data_dir = data_dir
    pipeline = PyLoadUniprotPipeline(settings)

    # 3. Mock the extractor
    mock_release_info = {
        "version": "2025_GENERATED_DATA_TEST",
        "release_date": datetime.date(2025, 1, 31),
        "swissprot_entry_count": 1,
        "trembl_entry_count": 1,
    }
    mocker.patch.object(
        extractor.Extractor, "get_release_info", return_value=mock_release_info
    )

    # --- Act ---
    pipeline.run(dataset="swissprot", mode="full")

    # --- Assert ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        # Assert that the protein P12345 was loaded
        cur.execute(f"SELECT COUNT(*) FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'")
        assert cur.fetchone()[0] == 1, "Protein P12345 should be loaded"

        # Assert that the taxonomy 9986 was loaded
        cur.execute(f"SELECT COUNT(*) FROM {pipeline.db_adapter.production_schema}.taxonomy WHERE ncbi_taxid = 9986")
        assert cur.fetchone()[0] == 1, "Taxonomy 9986 should be loaded"

        # Assert that the protein has the correct foreign key to the taxonomy
        cur.execute(f"SELECT ncbi_taxid FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'")
        result = cur.fetchone()
        assert result is not None, "Protein P12345 should have a result for ncbi_taxid"
        assert result[0] == 9986, "Protein P12345 should be linked to taxonomy 9986"
