import datetime
import gzip
import json
import logging
from pathlib import Path

import psycopg2
import pytest
from lxml import etree
from testcontainers.postgres import PostgresContainer
from typer.testing import CliRunner

from py_load_uniprot import PyLoadUniprotPipeline, extractor
from py_load_uniprot.cli import app
from py_load_uniprot.config import Settings, load_settings
from py_load_uniprot.db_manager import (
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
def settings(postgres_container: PostgresContainer, request) -> Settings:
    """
    Provides a Settings object configured to use the test container.
    This fixture can be parameterized to override default settings.
    Example:
    @pytest.mark.parametrize("settings", [{"num_workers": 1}], indirect=True)
    """
    # Use load_settings which initializes a new object each time
    settings = load_settings()
    settings.db.host = postgres_container.get_container_host_ip()
    settings.db.port = int(postgres_container.get_exposed_port(5432))
    settings.db.user = postgres_container.username
    settings.db.password = postgres_container.password
    settings.db.dbname = postgres_container.dbname

    # Apply any parameters passed via request
    if hasattr(request, "param"):
        for key, value in request.param.items():
            setattr(settings, key, value)

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
        settings: Settings, db_adapter: PostgresAdapter, sample_xml_file: Path, mocker
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
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema
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
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_with_evidence_file: Path, mocker
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
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema
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
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_file: Path, sample_xml_v2_file: Path, mocker
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
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

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


def test_delta_load_version_check(settings: Settings, db_adapter: PostgresAdapter, sample_xml_file: Path, mocker):
    """
    Tests that the delta load version check correctly prevents re-runs or
    running against an older version.
    """
    # --- Arrange: Perform an initial full load ---
    settings.data_dir = sample_xml_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_file.rename(sprot_file)
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

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


def test_status_command_reporting(settings: Settings, db_adapter: PostgresAdapter, mocker):
    """
    Tests that the get_current_release_version function reports the correct status.
    """
    # 1. Before anything is loaded, it should return None
    version = db_adapter.get_current_release_version()
    assert version is None, "Version should be None for an uninitialized database"

    # 2. After a load, it should return the correct version
    # Mock a successful pipeline run to create the metadata
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema
    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "2025_STATUS_TEST",
            "release_date": datetime.date(2025, 2, 1),
            "swissprot_entry_count": 1,
            "trembl_entry_count": 1,
        },
    )
    # Create a dummy file to avoid FileNotFoundError
    (settings.data_dir / "uniprot_sprot.xml.gz").touch()
    pipeline.run(dataset="swissprot", mode="full")

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
    settings: Settings, db_adapter: PostgresAdapter, mocker
):
    """
    Tests the full pipeline using the data file generated by the
    create_test_data.py script to ensure it is valid and loads correctly.
    This specifically tests the protein-to-taxonomy link.
    """
    # --- Arrange ---
    # 1. Run the script to generate the data file
    import subprocess
    subprocess.run(["python", "create_test_data.py"], check=True)

    # 2. Configure the pipeline to use this file
    settings.data_dir = Path("./data")
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

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


# V3: P12345's primary accession is changed to A1B2C3
SAMPLE_XML_V3_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2025-02-01" version="152">
  <accession>A1B2C3</accession>
  <accession>P12345</accession>
  <accession>Q9Y5Y5</accession>
  <name>TEST1_HUMAN</name>
  <protein>
    <recommendedName><fullName>Test protein 1 - Accession Change</fullName></recommendedName>
  </protein>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
</entry>
</uniprot>
"""


@pytest.fixture
def sample_xml_v3_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample V3 XML file for delta load testing (accession change)."""
    xml_path = tmp_path / "sample_v3.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_V3_CONTENT)
    return xml_path


SAMPLE_XML_MALFORMED_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <name>TEST1_HUMAN</name>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
</entry>
<entry> <!-- Missing closing tag -->
</uniprot>
"""


@pytest.fixture
def sample_xml_malformed_file(tmp_path: Path) -> Path:
    """Creates a gzipped, malformed XML file for testing."""
    xml_path = tmp_path / "sample_malformed.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_MALFORMED_CONTENT)
    return xml_path


def test_pipeline_fails_gracefully_on_malformed_xml(
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_malformed_file: Path, mocker
):
    """
    Tests that the pipeline raises a specific XMLSyntaxError if the input
    XML is malformed, ensuring robust error handling.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_malformed_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_malformed_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor, "get_release_info", return_value={"version": "MALFORMED"}
    )

    # --- Act & Assert ---
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema
    with pytest.raises(etree.XMLSyntaxError, match="Opening and ending tag mismatch"):
        pipeline.run(dataset="swissprot", mode="full")


SAMPLE_XML_MISSING_ELEMENTS_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<!-- Entry 1: Missing <gene> tag -->
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>M12345</accession>
  <name>MISSING_GENE</name>
  <protein>
    <recommendedName><fullName>Protein without a gene tag</fullName></recommendedName>
  </protein>
  <organism>
    <name type="scientific">Homo sapiens</name>
    <dbReference type="NCBI Taxonomy" id="9606"/>
  </organism>
  <sequence length="5" mass="555">MSEQ</sequence>
</entry>
<!-- Entry 2: Missing <sequence> tag -->
<entry dataset="Swiss-Prot" created="2001-01-01" modified="2024-01-01" version="10">
  <accession>M67890</accession>
  <name>MISSING_SEQ</name>
  <protein>
    <recommendedName><fullName>Protein without a sequence</fullName></recommendedName>
  </protein>
  <gene><name type="primary">MSG1</name></gene>
  <organism>
    <name type="scientific">Mus musculus</name>
    <dbReference type="NCBI Taxonomy" id="10090"/>
  </organism>
</entry>
<!-- Entry 3: Missing <fullName> inside <recommendedName> -->
<entry dataset="TrEMBL" created="2002-02-02" modified="2024-02-02" version="5">
  <accession>M11111</accession>
  <name>MISSING_NAME</name>
  <protein>
    <recommendedName></recommendedName>
  </protein>
  <gene><name type="primary">MSN1</name></gene>
  <organism>
    <name type="scientific">Rattus norvegicus</name>
    <dbReference type="NCBI Taxonomy" id="10116"/>
  </organism>
  <sequence length="3" mass="333">MSN</sequence>
</entry>
</uniprot>
"""


@pytest.fixture
def sample_xml_missing_elements_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample XML file with missing optional elements."""
    xml_path = tmp_path / "sample_missing_elements.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_MISSING_ELEMENTS_CONTENT)
    return xml_path


def test_pipeline_handles_missing_optional_elements(
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_missing_elements_file: Path, mocker
):
    """
    Tests that the pipeline correctly handles XML entries with missing
    optional elements (e.g., no gene, no sequence), loading them as NULLs.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_missing_elements_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_missing_elements_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "MISSING_ELEMENTS_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 3,
            "trembl_entry_count": 0,
        },
    )
    # Force single-threaded execution to ensure logs are captured by caplog
    settings.num_workers = 1
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

    # --- Act ---
    pipeline.run(dataset="swissprot", mode="full")

    # --- Assert ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        prod_schema = pipeline.db_adapter.production_schema
        # Check that all 3 proteins were loaded
        cur.execute(f"SELECT COUNT(*) FROM {prod_schema}.proteins")
        assert cur.fetchone()[0] == 3, "All three proteins should be loaded"

        # 1. Check protein with missing <gene>
        cur.execute(
            f"SELECT 1 FROM {prod_schema}.genes WHERE protein_accession = 'M12345'"
        )
        assert (
            cur.fetchone() is None
        ), "Protein M12345 should have no corresponding gene entry"

        # 2. Check protein with missing <sequence>
        cur.execute(
            f"SELECT sequence_length, molecular_weight FROM {prod_schema}.proteins WHERE primary_accession = 'M67890'"
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] is None, "Sequence length should be NULL"
        assert row[1] is None, "Molecular weight should be NULL"

        # 3. Check protein with missing <fullName>
        cur.execute(
            f"SELECT protein_name FROM {prod_schema}.proteins WHERE primary_accession = 'M11111'"
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] is None, "Protein name should be NULL for entry with empty recommendedName"

        # 4. Check a protein that has a name
        cur.execute(
            f"SELECT protein_name FROM {prod_schema}.proteins WHERE primary_accession = 'M12345'"
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "Protein without a gene tag"


SAMPLE_XML_NON_ASCII_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>N0N4SC11</accession>
  <name>NON_ASCII</name>
  <protein>
    <recommendedName><fullName>α-synuclein</fullName></recommendedName>
  </protein>
  <organism>
    <name type="scientific">Homo sapiens</name>
    <dbReference type="NCBI Taxonomy" id="9606"/>
  </organism>
  <comment type="function"><text>A protein involved in neurotransmitter release, with a name containing Greek letters: αβγ.</text></comment>
  <sequence length="5" mass="555">MSEQ</sequence>
</entry>
</uniprot>
"""


@pytest.fixture
def sample_xml_non_ascii_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample XML file with non-ASCII characters."""
    xml_path = tmp_path / "sample_non_ascii.xml.gz"
    # Ensure encoding is explicitly set to utf-8
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_NON_ASCII_CONTENT)
    return xml_path


def test_pipeline_handles_non_ascii_characters(
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_non_ascii_file: Path, mocker
):
    """
    Tests that non-ASCII characters in text fields (like protein names or
    comments) are correctly handled and persisted to the database.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_non_ascii_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_non_ascii_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "NON_ASCII_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 1,
            "trembl_entry_count": 0,
        },
    )
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

    # --- Act ---
    pipeline.run(dataset="swissprot", mode="full")

    # --- Assert ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        prod_schema = pipeline.db_adapter.production_schema

        # 1. Check the protein name
        cur.execute(
            f"SELECT protein_name FROM {prod_schema}.proteins WHERE primary_accession = 'N0N4SC11'"
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "α-synuclein"

        # 2. Check the comment data
        cur.execute(
            f"SELECT comments_data FROM {prod_schema}.proteins WHERE primary_accession = 'N0N4SC11'"
        )
        row = cur.fetchone()
        assert row is not None
        comments_data = row[0]
        assert "αβγ" in json.dumps(comments_data, ensure_ascii=False)


SAMPLE_XML_DUPLICATE_ACCESSION_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <name>TEST1_HUMAN_V1</name>
  <protein><recommendedName><fullName>Test protein 1, Version 1</fullName></recommendedName></protein>
  <organism><name type="scientific">Homo sapiens</name><dbReference type="NCBI Taxonomy" id="9606"/></organism>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
</entry>
<entry dataset="Swiss-Prot" created="2001-01-01" modified="2025-01-01" version="151">
  <accession>P12345</accession>
  <name>TEST1_HUMAN_V2</name>
  <protein><recommendedName><fullName>Test protein 1, Version 2</fullName></recommendedName></protein>
  <organism><name type="scientific">Homo sapiens</name><dbReference type="NCBI Taxonomy" id="9606"/></organism>
  <sequence length="11" mass="2222">MTESTSEQAAX</sequence>
</entry>
</uniprot>
"""


@pytest.fixture
def sample_xml_duplicate_accession_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample XML file with a duplicate primary accession."""
    xml_path = tmp_path / "sample_duplicate.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_DUPLICATE_ACCESSION_CONTENT)
    return xml_path


@pytest.mark.parametrize("settings", [{"num_workers": 1}], indirect=True)
def test_pipeline_fails_on_duplicate_accessions_in_source(
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_duplicate_accession_file: Path, mocker
):
    """
    Tests that if a source XML file contains duplicate primary accessions,
    the pipeline now fails with a ValueError instead of silently logging.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_duplicate_accession_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_duplicate_accession_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "DUPLICATE_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 2,
            "trembl_entry_count": 0,
        },
    )
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

    # --- Act & Assert ---
    # The pipeline should now raise a ValueError due to the duplicate accession.
    with pytest.raises(ValueError, match="Duplicate primary accession 'P12345'"):
        pipeline.run(dataset="swissprot", mode="full")

    # --- Assert that the database is clean ---
    # The staging schema should have been cleaned up, and no production schema created.
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (pipeline.db_adapter.staging_schema,),
        )
        assert cur.fetchone() is None, "Staging schema should be dropped on failure"
        cur.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (pipeline.db_adapter.production_schema,),
        )
        assert cur.fetchone() is None, "Production schema should not be created on failure"


SAMPLE_XML_COMPLEX_DUPLICATE_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry><accession>DUPE01</accession><name>D1</name><sequence length="1">A</sequence></entry>
<entry><accession>UNIQUE01</accession><name>U1</name><sequence length="1">B</sequence></entry>
<entry><accession>DUPE01</accession><name>D2</name><sequence length="1">C</sequence></entry>
<entry><accession>UNIQUE02</accession><name>U2</name><sequence length="1">D</sequence></entry>
<entry><accession>UNIQUE03</accession><name>U3</name><sequence length="1">E</sequence></entry>
<entry><accession>DUPE01</accession><name>D3</name><sequence length="1">F</sequence></entry>
</uniprot>
"""


@pytest.fixture
def sample_xml_complex_duplicate_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample XML file with multiple duplicate accessions interspersed with unique ones."""
    xml_path = tmp_path / "sample_complex_duplicate.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_COMPLEX_DUPLICATE_CONTENT)
    return xml_path


@pytest.mark.parametrize("settings", [{"num_workers": 4}], indirect=True)
def test_pipeline_fails_on_duplicates_in_multiprocessing(
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_complex_duplicate_file: Path, mocker
):
    """
    Tests that the duplicate accession check is effective even in a multiprocessing
    context where entry processing order is non-deterministic.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_complex_duplicate_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_complex_duplicate_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "MP_DUPLICATE_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 6,
            "trembl_entry_count": 0,
        },
    )
    pipeline = PyLoadUniprotPipeline(settings)
    # Use distinct schema names to ensure no test interference
    pipeline.db_adapter.production_schema = "test_mp_duplicates_prod"
    pipeline.db_adapter.staging_schema = "test_mp_duplicates_staging"


    # --- Act & Assert ---
    # The pipeline should raise a ValueError due to the duplicate accession 'DUPE01'.
    # This error originates from the writer process.
    with pytest.raises(ValueError, match="Duplicate primary accession found in the input file"):
        pipeline.run(dataset="swissprot", mode="full")

    # --- Assert that the database is clean ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (pipeline.db_adapter.staging_schema,),
        )
        assert cur.fetchone() is None, "Staging schema should be dropped on failure"
        cur.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (pipeline.db_adapter.production_schema,),
        )
        assert cur.fetchone() is None, "Production schema should not be created on failure"


def test_delta_load_primary_accession_change(
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_file: Path, sample_xml_v3_file: Path, mocker
):
    """
    Tests that a delta load correctly handles a change in a protein's
    primary accession number, which is a critical edge case for maintaining
    data integrity.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_file.rename(sprot_file)
    pipeline = PyLoadUniprotPipeline(settings)

    # --- Act 1: Initial Full Load (V1) ---
    print("--- Running Initial Full Load (V1) for Accession Change Test ---")
    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "V1_ACC_TEST",
            "release_date": datetime.date(2024, 1, 1),
            "swissprot_entry_count": 2,
            "trembl_entry_count": 0,
        },
    )
    pipeline.run(dataset="swissprot", mode="full")

    # --- Assert 1: Verify initial state ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        assert cur.fetchone() is not None, "Protein P12345 should exist after full load"
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'A1B2C3'"
        )
        assert cur.fetchone() is None, "Protein A1B2C3 should not exist yet"

    # --- Act 2: Delta Load (V3) ---
    print("--- Running Delta Load (V3) for Accession Change ---")
    sprot_file.rename(settings.data_dir / "uniprot_sprot_v1.xml.gz")
    sample_xml_v3_file.rename(sprot_file)
    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "V3_ACC_TEST",
            "release_date": datetime.date(2025, 2, 1),
            "swissprot_entry_count": 1,
            "trembl_entry_count": 0,
        },
    )
    pipeline.run(dataset="swissprot", mode="delta")

    # --- Assert 2: Verify state after delta load ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        # The old primary accession should be gone
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        assert (
            cur.fetchone() is None
        ), "Old primary accession P12345 should be deleted"

        # The new primary accession should exist
        cur.execute(
            f"SELECT uniprot_id FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'A1B2C3'"
        )
        result = cur.fetchone()
        assert (
            result is not None
        ), "New primary accession A1B2C3 should be inserted"
        assert (
            result[0] == "TEST1_HUMAN"
        ), "Uniprot ID should remain the same for the new accession"

        # Check that the old accession is now a secondary accession for the new primary one
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.accessions WHERE protein_accession = 'A1B2C3' AND secondary_accession = 'P12345'"
        )
        assert (
            cur.fetchone() is not None
        ), "P12345 should now be a secondary accession for A1B2C3"

        # Check that other proteins (like P67890) from the initial load were correctly deleted as they were not in the V3 file
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P67890'"
        )
        assert cur.fetchone() is None, "P67890 should have been deleted"

        # The total count should be 1 (only A1B2C3)
        cur.execute(f"SELECT COUNT(*) FROM {pipeline.db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 1, "Only the updated protein should exist"


def test_full_load_rolls_back_on_data_load_failure(
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_file: Path, mocker
):
    """
    Tests that a full load transaction is rolled back if an error occurs
    during the data loading (COPY) phase, ensuring the database is left clean.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={"version": "ROLLBACK_TEST"},
    )

    # Mock the load_data method on the PostgresAdapter to simulate a failure
    # during the COPY command.
    mocker.patch.object(
        PostgresAdapter,
        "bulk_load_intermediate",
        side_effect=psycopg2.Error("Simulated COPY failure"),
    )

    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

    # --- Act & Assert ---
    # The pipeline should raise the simulated exception
    with pytest.raises(psycopg2.Error, match="Simulated COPY failure"):
        pipeline.run(dataset="swissprot", mode="full")

    # --- Assert Database State ---
    # After the failed run, the staging schema should have been dropped,
    # and no production schema should exist.
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        # Check that the staging schema was cleaned up
        cur.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (pipeline.db_adapter.staging_schema,),
        )
        assert (
            cur.fetchone() is None
        ), "Staging schema should be dropped on failure"

        # Check that the production schema was not created
        cur.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s",
            (pipeline.db_adapter.production_schema,),
        )
        assert (
            cur.fetchone() is None
        ), "Production schema should not be created on failure"


@pytest.fixture
def sample_xml_empty_file(tmp_path: Path) -> Path:
    """Creates a gzipped, completely empty file."""
    xml_path = tmp_path / "sample_empty.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write("")
    return xml_path


@pytest.fixture
def sample_xml_no_entries_file(tmp_path: Path) -> Path:
    """Creates a gzipped XML file with a root element but no entries."""
    xml_path = tmp_path / "sample_no_entries.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?><uniprot xmlns="http://uniprot.org/uniprot"></uniprot>')
    return xml_path


@pytest.mark.parametrize(
    "xml_file_fixture", ["sample_xml_empty_file", "sample_xml_no_entries_file"]
)
def test_pipeline_handles_empty_or_no_entry_files(
    settings: Settings,
    db_adapter: PostgresAdapter,
    xml_file_fixture: str,
    request,
    mocker,
):
    """
    Tests that the pipeline runs successfully without errors when the input
    XML file is empty or contains no <entry> elements.
    """
    # --- Arrange ---
    xml_file = request.getfixturevalue(xml_file_fixture)
    settings.data_dir = xml_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    xml_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "EMPTY_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 0,
            "trembl_entry_count": 0,
        },
    )

    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = "test_empty_file"
    pipeline.db_adapter.staging_schema = "test_empty_file_staging"

    # --- Act ---
    # The pipeline should run to completion without raising an exception
    pipeline.run(dataset="swissprot", mode="full")

    # --- Assert ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        prod_schema = pipeline.db_adapter.production_schema
        # Check that the production schema and metadata table were created
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (prod_schema,))
        assert cur.fetchone() is not None, "Production schema should still be created"

        # Check that no proteins were loaded
        cur.execute(f"SELECT COUNT(*) FROM {prod_schema}.proteins")
        assert cur.fetchone()[0] == 0, "Should be zero proteins loaded"

        # Check that metadata was still written
        cur.execute(f"SELECT version FROM {prod_schema}.py_load_uniprot_metadata")
        assert cur.fetchone()[0] == "EMPTY_TEST", "Metadata should be written"


@pytest.fixture(scope="session")
def sample_xml_full_profile_content():
    """
    A comprehensive XML entry designed to test the differences between
    'standard' and 'full' ETL profiles.
    """
    return """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2022-01-01" modified="2022-01-01" version="1">
  <accession>F00001</accession>
  <name>FULL_PROFILE_TEST</name>
  <protein><recommendedName><fullName>Full Profile Test Protein</fullName></recommendedName></protein>
  <organism>
    <name type="scientific">Test organism</name>
    <dbReference type="NCBI Taxonomy" id="99999"/>
  </organism>
  <!-- Comments: one 'standard' type, one 'non-standard' type -->
  <comment type="function"><text>This is a function comment (standard).</text></comment>
  <comment type="miscellaneous"><text>This is a miscellaneous comment (full only).</text></comment>
  <!-- Feature: should only be loaded in 'full' profile -->
  <feature type="active site"><location><position position="10"/></location></feature>
  <!-- DB Reference: should only be loaded in 'full' profile -->
  <dbReference type="PDB" id="1XYZ"/>
  <!-- Evidence: should only be loaded in 'full' profile -->
  <evidence key="1" type="ECO:0000256"/>
  <sequence length="20" mass="2222">FULLPROFILESEQTESTAA</sequence>
</entry>
</uniprot>
"""


@pytest.fixture
def sample_xml_full_profile_file(tmp_path: Path, sample_xml_full_profile_content: str) -> Path:
    """Creates a gzipped sample XML file for testing ETL profiles."""
    xml_path = tmp_path / "sample_full_profile.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(sample_xml_full_profile_content)
    return xml_path


def test_etl_profiles_standard_vs_full(
    settings: Settings,
    db_adapter: PostgresAdapter,
    sample_xml_full_profile_file: Path,
    mocker,
):
    """
    Tests that the 'standard' and 'full' ETL profiles correctly include or
    exclude detailed JSONB data.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_full_profile_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_full_profile_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "PROFILE_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 1,
            "trembl_entry_count": 0,
        },
    )

    # --- Act 1: Run with 'standard' profile (default) ---
    print("--- Running pipeline with 'standard' profile ---")
    # --- Act 1: Run with 'standard' profile (default) ---
    print("--- Running pipeline with 'standard' profile ---")
    settings.profile = "standard"  # Explicitly set profile on settings
    pipeline_standard = PyLoadUniprotPipeline(settings)
    pipeline_standard.db_adapter.production_schema = "test_profiles_standard"
    pipeline_standard.db_adapter.staging_schema = "test_profiles_standard_staging"
    pipeline_standard.run(dataset="swissprot", mode="full")

    # --- Assert 1: Check 'standard' profile results ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        prod_schema = pipeline_standard.db_adapter.production_schema
        cur.execute(
            f"SELECT comments_data, features_data, db_references_data, evidence_data FROM {prod_schema}.proteins WHERE primary_accession = 'F00001'"
        )
        row = cur.fetchone()
        assert row is not None, "Protein should be loaded in standard profile"
        comments, features, db_refs, evidence = row

        # Comments should be filtered to standard types
        assert comments is not None and len(comments) == 1
        assert comments[0]["attributes"]["type"] == "function"
        # The other JSON fields should be NULL (or empty JSON)
        assert features is None, "Features should be NULL in standard profile"
        assert db_refs is None, "DB References should be NULL in standard profile"
        assert evidence is None, "Evidence should be NULL in standard profile"

    # --- Act 2: Run with 'full' profile ---
    print("--- Running pipeline with 'full' profile ---")
    settings.profile = "full"  # Switch profile on the same settings object
    pipeline_full = PyLoadUniprotPipeline(settings)
    # Use a new adapter and different schema names to avoid conflicts
    pipeline_full.db_adapter = PostgresAdapter(
        settings,
        staging_schema="test_profiles_full_staging",
        production_schema="test_profiles_full",
    )
    pipeline_full.run(dataset="swissprot", mode="full")

    # --- Assert 2: Check 'full' profile results ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        prod_schema = pipeline_full.db_adapter.production_schema
        cur.execute(
            f"SELECT comments_data, features_data, db_references_data, evidence_data FROM {prod_schema}.proteins WHERE primary_accession = 'F00001'"
        )
        row = cur.fetchone()
        assert row is not None, "Protein should be loaded in full profile"
        comments, features, db_refs, evidence = row

        # All data should be present
        assert comments is not None and len(comments) == 2, "Should have all comments"
        assert features is not None and len(features) > 0, "Features should be loaded"
        assert db_refs is not None and len(db_refs) > 0, "DB References should be loaded"
        assert evidence is not None and len(evidence) > 0, "Evidence should be loaded"


# V4: P12345 and P67890 are both updated to have the same new primary accession,
# which should cause a unique constraint violation during the delta load.
SAMPLE_XML_V4_CONFLICT_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<!-- Update P12345 to a new accession -->
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2025-03-01" version="153">
  <accession>CONFLICT01</accession>
  <accession>P12345</accession>
  <name>TEST1_HUMAN</name>
  <protein><recommendedName><fullName>Test protein 1 - Conflict</fullName></recommendedName></protein>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
</entry>
<!-- Update P67890 to the *same* new accession -->
<entry dataset="TrEMBL" created="2010-10-12" modified="2025-03-01" version="101">
  <accession>CONFLICT01</accession>
  <accession>P67890</accession>
  <name>TEST2_MOUSE</name>
  <protein><recommendedName><fullName>Test protein 2 - Conflict</fullName></recommendedName></protein>
  <sequence length="12" mass="2222">MTESTSEQBBBB</sequence>
</entry>
</uniprot>
"""


@pytest.fixture
def sample_xml_v4_conflict_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample V4 XML file for testing a delta load conflict."""
    xml_path = tmp_path / "sample_v4_conflict.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_V4_CONFLICT_CONTENT)
    return xml_path


@pytest.mark.parametrize("settings", [{"num_workers": 1}], indirect=True)
def test_delta_load_handles_conflicting_accession_change(
    settings: Settings,
    db_adapter: PostgresAdapter,
    sample_xml_file: Path,
    sample_xml_v4_conflict_file: Path,
    mocker,
):
    """
    Tests that a delta load transaction is rolled back if it contains data
    that violates a unique constraint (e.g., two proteins updated to have the
    same new primary accession).
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_file.rename(sprot_file)
    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

    # --- Act 1: Initial Full Load ---
    print("--- Running Initial Full Load for Conflict Test ---")
    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "V1_CONFLICT_TEST",
            "release_date": datetime.date(2025, 3, 1),
            "swissprot_entry_count": 2,
            "trembl_entry_count": 0,
        },
    )
    pipeline.run(dataset="swissprot", mode="full")

    # --- Assert 1: Verify initial state ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2
        cur.execute(
            f"SELECT uniprot_id FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        assert cur.fetchone()[0] == "TEST1_HUMAN"

    # --- Act 2: Attempt Delta Load with conflicting data ---
    print("--- Running Delta Load with Conflicting Accession Changes ---")
    sprot_file.rename(settings.data_dir / "uniprot_sprot_v1.xml.gz")
    sample_xml_v4_conflict_file.rename(sprot_file)
    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "V4_CONFLICT_TEST",
            "release_date": datetime.date(2025, 3, 2),
            "swissprot_entry_count": 2,
            "trembl_entry_count": 0,
        },
    )

    # The pipeline should fail with a ValueError from the transformer
    # due to the duplicate primary accession in the source file.
    with pytest.raises(ValueError, match="Duplicate primary accession 'CONFLICT01'"):
        pipeline.run(dataset="swissprot", mode="delta")

    # --- Assert 2: Verify that the database state was rolled back ---
    print("--- Verifying database state after failed delta load ---")
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        # The total number of proteins should still be 2
        cur.execute(f"SELECT COUNT(*) FROM {db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2, "Protein count should be unchanged after failed delta."

        # The conflicting accession should not exist
        cur.execute(
            f"SELECT 1 FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'CONFLICT01'"
        )
        assert cur.fetchone() is None, "Conflicting accession should not have been inserted."

        # The original protein should be untouched
        cur.execute(
            f"SELECT uniprot_id, protein_name FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "TEST1_HUMAN"
        assert row[1] == "Test protein 1", "Protein name should not have been updated."

        # The other original protein should also be untouched
        cur.execute(
            f"SELECT 1 FROM {db_adapter.production_schema}.proteins WHERE primary_accession = 'P67890'"
        )
        assert cur.fetchone() is not None, "Original protein P67890 should still exist."

        # Metadata should not have been updated
        cur.execute(f"SELECT version FROM {db_adapter.production_schema}.py_load_uniprot_metadata")
        assert cur.fetchone()[0] == "V1_CONFLICT_TEST", "Metadata version should be unchanged."


SAMPLE_XML_V2_ONLY_NEW_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
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
def sample_xml_v2_only_new_file(tmp_path: Path) -> Path:
    """Creates a gzipped sample V2 XML file with only a new entry for deletion testing."""
    xml_path = tmp_path / "sample_v2_only_new.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_XML_V2_ONLY_NEW_CONTENT)
    return xml_path


def test_delta_load_pure_deletion(
    settings: Settings, db_adapter: PostgresAdapter, sample_xml_file: Path, sample_xml_v2_only_new_file: Path, mocker
):
    """
    Tests that a delta load correctly handles the deletion of all existing
    proteins when the new release file contains completely different entries.
    """
    # --- Arrange ---
    settings.data_dir = sample_xml_file.parent
    sprot_file = settings.data_dir / "uniprot_sprot.xml.gz"
    sample_xml_file.rename(sprot_file)

    pipeline = PyLoadUniprotPipeline(settings)
    pipeline.db_adapter.production_schema = db_adapter.production_schema
    pipeline.db_adapter.staging_schema = db_adapter.staging_schema

    # --- Act 1: Initial Full Load (V1) ---
    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "V1_DEL_TEST",
            "release_date": datetime.date(2024, 1, 1),
            "swissprot_entry_count": 2,
            "trembl_entry_count": 0,
        },
    )
    pipeline.run(dataset="swissprot", mode="full")

    # --- Assert 1: State after Full Load ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {pipeline.db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 2
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        assert cur.fetchone() is not None
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P67890'"
        )
        assert cur.fetchone() is not None

    # --- Act 2: Delta Load (V2 - only new protein) ---
    sprot_file.rename(settings.data_dir / "uniprot_sprot_v1.xml.gz")
    sample_xml_v2_only_new_file.rename(sprot_file)

    mocker.patch.object(
        extractor.Extractor,
        "get_release_info",
        return_value={
            "version": "V2_DEL_TEST",
            "release_date": datetime.date(2025, 1, 1),
            "swissprot_entry_count": 1,
            "trembl_entry_count": 0,
        },
    )
    pipeline.run(dataset="swissprot", mode="delta")

    # --- Assert 2: State after Delta Load ---
    with postgres_connection(settings) as conn, conn.cursor() as cur:
        # Check total count: should be 1 (only the new protein)
        cur.execute(f"SELECT COUNT(*) FROM {pipeline.db_adapter.production_schema}.proteins")
        assert cur.fetchone()[0] == 1, "Total protein count should be 1 after pure-delete delta."

        # Check that the old proteins were deleted
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P12345'"
        )
        assert cur.fetchone() is None, "Protein P12345 should have been deleted."
        cur.execute(
            f"SELECT 1 FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'P67890'"
        )
        assert cur.fetchone() is None, "Protein P67890 should have been deleted."

        # Check that the new protein was inserted
        cur.execute(
            f"SELECT uniprot_id FROM {pipeline.db_adapter.production_schema}.proteins WHERE primary_accession = 'A0A0A0'"
        )
        assert (
            cur.fetchone()[0] == "TEST3_NEW"
        ), "New protein A0A0A0 should have been inserted."

        # Check that metadata was updated
        cur.execute(
            f"SELECT version FROM {pipeline.db_adapter.production_schema}.py_load_uniprot_metadata"
        )
        assert cur.fetchone()[0] == "V2_DEL_TEST"
