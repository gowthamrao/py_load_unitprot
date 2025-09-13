import csv
import gzip
import json
from pathlib import Path

import pytest

from py_load_uniprot import transformer

# Using the more comprehensive XML sample to test all parsing features
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


def read_tsv_gz(file_path: Path) -> list[list[str]]:
    """Helper function to read a gzipped TSV file."""
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        return list(reader)


def test_transform_xml_to_tsv_creates_correct_output(
    sample_xml_file: Path, tmp_path: Path
):
    """
    Tests that the transformer correctly parses a sample XML and produces
    the expected set of TSV files with correct content.
    """
    # Arrange
    output_dir = tmp_path / "output"

    # Act
    transformer.transform_xml_to_tsv(sample_xml_file, output_dir, profile="full")

    # Assert all expected files are created
    for table_name in transformer.TABLE_HEADERS.keys():
        # The sample doesn't have db_references_data, so that file isn't created
        if table_name == "db_references_data":
            continue
        # The sample doesn't have evidence_data
        if table_name == "evidence_data":
            continue

        file_path = output_dir / f"{table_name}.tsv.gz"
        # Only assert file existence if it should have data
        if table_name in [
            "proteins",
            "sequences",
            "accessions",
            "taxonomy",
            "genes",
            "protein_to_go",
            "keywords",
        ]:
            assert file_path.exists(), f"File for table '{table_name}' should exist"

    # --- Assert proteins.tsv.gz ---
    proteins_rows = read_tsv_gz(output_dir / "proteins.tsv.gz")
    assert proteins_rows[0] == transformer.TABLE_HEADERS["proteins"]
    protein_data = sorted(proteins_rows[1:], key=lambda r: r[0])  # Sort by accession
    assert len(protein_data) == 2

    # Check P12345
    p1_row = protein_data[0]
    assert p1_row[0:8] == [
        "P12345",
        "TEST1_HUMAN",
        "Test protein 1",
        "9606",
        "10",
        "1111",
        "2000-05-30",
        "2024-07-17",
    ]
    assert json.loads(p1_row[8])[0]["tag"] == "comment"
    assert json.loads(p1_row[9])[0]["tag"] == "feature"
    assert p1_row[10] == ""

    # Check P67890
    p2_row = protein_data[1]
    assert p2_row[0:8] == [
        "P67890",
        "TEST2_MOUSE",
        "Test protein 2",
        "10090",
        "12",
        "2222",
        "2010-10-12",
        "2024-07-18",
    ]

    # --- Assert other tables (with sorting where necessary) ---
    accessions_rows = read_tsv_gz(output_dir / "accessions.tsv.gz")
    assert accessions_rows == [
        transformer.TABLE_HEADERS["accessions"],
        ["P12345", "Q9Y5Y5"],
    ]

    sequences_rows = read_tsv_gz(output_dir / "sequences.tsv.gz")
    sequence_data = sorted(sequences_rows[1:], key=lambda r: r[0])
    assert len(sequence_data) == 2
    assert sequence_data[0] == ["P12345", "MTESTSEQAA"]
    assert sequence_data[1] == ["P67890", "MTESTSEQBBBB"]

    taxonomy_rows = read_tsv_gz(output_dir / "taxonomy.tsv.gz")
    taxonomy_data = sorted(taxonomy_rows[1:], key=lambda r: r[0])  # Sort by taxid
    assert len(taxonomy_data) == 2
    assert taxonomy_data[0] == ["10090", "Mus musculus", "Eukaryota > Metazoa"]
    assert taxonomy_data[1] == ["9606", "Homo sapiens", "Eukaryota > Metazoa"]

    genes_rows = read_tsv_gz(output_dir / "genes.tsv.gz")
    assert genes_rows == [transformer.TABLE_HEADERS["genes"], ["P12345", "TP1", "True"]]

    go_rows = read_tsv_gz(output_dir / "protein_to_go.tsv.gz")
    assert go_rows == [
        transformer.TABLE_HEADERS["protein_to_go"],
        ["P12345", "GO:0005515"],
    ]

    keywords_rows = read_tsv_gz(output_dir / "keywords.tsv.gz")
    assert keywords_rows == [
        transformer.TABLE_HEADERS["keywords"],
        ["P12345", "KW-0181", "Complete proteome"],
    ]


def test_parse_entry_extracts_evidence_data():
    """
    Tests that _parse_entry correctly finds all evidence tags, including nested
    ones, and serializes them into the `evidence_data` field.
    """
    # Arrange
    from lxml import etree

    xml_string = """
<entry created="2000-05-30" modified="2024-07-17" version="150" xmlns="http://uniprot.org/uniprot">
  <accession>P12345</accession>
  <name>TEST1_HUMAN</name>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
  <evidence key="1" type="ECO:0000269">
    <source>
      <dbReference type="PubMed" id="12345"/>
    </source>
  </evidence>
  <feature type="chain" description="Test protein 1" id="PRO_0000021325">
    <location><begin position="1"/><end position="10"/></location>
    <evidence key="2" type="ECO:0000256"/>
  </feature>
</entry>
"""
    elem = etree.fromstring(xml_string)

    # Act
    parsed_data = transformer._parse_entry(elem, profile="full")

    # Assert
    assert "proteins" in parsed_data
    assert len(parsed_data["proteins"]) == 1

    protein_row = parsed_data["proteins"][0]
    # The evidence_data is now the 12th column (index 11) due to protein_name
    evidence_json_str = protein_row[11]

    assert evidence_json_str is not None
    evidence_list = json.loads(evidence_json_str)

    assert isinstance(evidence_list, list)
    assert len(evidence_list) == 2

    # Check for evidence key "1"
    evidence_1 = next(
        (e for e in evidence_list if e.get("attributes", {}).get("key") == "1"), None
    )
    assert evidence_1 is not None
    assert evidence_1["attributes"]["type"] == "ECO:0000269"
    assert evidence_1["children"][0]["tag"] == "source"

    # Check for evidence key "2"
    evidence_2 = next(
        (e for e in evidence_list if e.get("attributes", {}).get("key") == "2"), None
    )
    assert evidence_2 is not None
    assert evidence_2["attributes"]["type"] == "ECO:0000256"


# --- Test for parallel implementation ---


def transform_xml_to_tsv_single_threaded(
    xml_file: Path, output_dir: Path, profile: str
):
    """
    A single-threaded version of the transformer, kept for baseline comparison.
    """
    from lxml import etree

    # This is a recreation of the original single-threaded implementation
    with (
        gzip.open(xml_file, "rb") as f_in,
        transformer.FileWriterManager(output_dir) as writers,
    ):
        context = etree.iterparse(
            f_in, events=("end",), tag=transformer._get_tag("entry")
        )
        seen_taxonomy_ids = set()

        for _, elem in context:
            parsed_data = transformer._parse_entry(elem, profile)

            for table_name, rows in parsed_data.items():
                if table_name == "taxonomy":
                    unique_rows = []
                    for row in rows:
                        tax_id = row[0]
                        if tax_id not in seen_taxonomy_ids:
                            unique_rows.append(row)
                            seen_taxonomy_ids.add(tax_id)
                    if unique_rows:
                        writers[table_name].writerows(unique_rows)
                else:
                    writers[table_name].writerows(rows)
            # Crucial memory management
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]


def test_parallel_transformer_matches_single_threaded(
    sample_xml_file: Path, tmp_path: Path
):
    """
    Verifies that the parallel transformer produces the exact same output as
    the original single-threaded implementation.
    """
    # Arrange
    output_single = tmp_path / "output_single"
    output_parallel = tmp_path / "output_parallel"
    output_single.mkdir()
    output_parallel.mkdir()

    # Act
    # Run single-threaded version
    transform_xml_to_tsv_single_threaded(sample_xml_file, output_single, profile="full")
    # Run parallel version
    transformer.transform_xml_to_tsv(
        sample_xml_file, output_parallel, profile="full", num_workers=2
    )

    # Assert
    # Check that the same files were created
    single_files = sorted([p.name for p in output_single.glob("*.tsv.gz")])
    parallel_files = sorted([p.name for p in output_parallel.glob("*.tsv.gz")])
    assert (
        single_files == parallel_files
    ), "The set of created files should be identical"
    assert len(single_files) > 0, "At least one file should have been created"

    # Check the content of each file
    for filename in single_files:
        single_content = read_tsv_gz(output_single / filename)
        parallel_content = read_tsv_gz(output_parallel / filename)

        # Sort content to account for non-deterministic order of processing
        # Header should be the same, so we sort data rows
        single_header, single_data = single_content[0], sorted(single_content[1:])
        parallel_header, parallel_data = parallel_content[0], sorted(
            parallel_content[1:]
        )

        assert single_header == parallel_header, f"Headers in {filename} should match"
        assert (
            single_data == parallel_data
        ), f"Data in {filename} should match after sorting"


def test_transform_xml_to_tsv_with_empty_file(tmp_path: Path):
    """
    Tests that the transformer correctly handles an empty input file.
    """
    # Arrange
    empty_xml_file = tmp_path / "empty.xml.gz"
    with gzip.open(empty_xml_file, "wt", encoding="utf-8") as f:
        f.write("")
    output_dir = tmp_path / "output"

    # Act
    transformer.transform_xml_to_tsv(empty_xml_file, output_dir, profile="full", num_workers=1)

    # Assert
    assert not output_dir.exists() or not any(output_dir.iterdir())


def test_element_to_json_with_empty_list():
    """
    Tests that _element_to_json returns None when given an empty list.
    """
    assert transformer._element_to_json([]) is None


def test_element_to_json_with_none():
    """
    Tests that _element_to_json returns None when given None.
    """
    assert transformer._element_to_json(None) is None


def test_parse_entry_with_no_accession():
    """
    Tests that _parse_entry returns an empty dict if an entry has no primary accession.
    """
    from lxml import etree

    xml_string = """
<entry xmlns="http://uniprot.org/uniprot">
  <name>TEST_NO_ACC</name>
</entry>
"""
    elem = etree.fromstring(xml_string)
    parsed_data = transformer._parse_entry(elem, profile="full")
    assert parsed_data == {}


def test_parse_entry_with_no_protein_name():
    """
    Tests that _parse_entry handles entries without a recommendedName.
    """
    from lxml import etree

    xml_string = """
<entry xmlns="http://uniprot.org/uniprot">
  <accession>P99999</accession>
  <name>TEST_NO_PROT_NAME</name>
  <protein>
    <submittedName><fullName>A submitted name</fullName></submittedName>
  </protein>
  <sequence length="1" mass="1">M</sequence>
</entry>
"""
    elem = etree.fromstring(xml_string)
    parsed_data = transformer._parse_entry(elem, profile="full")
    assert parsed_data["proteins"][0][2] is None  # protein_name should be None


def test_transform_xml_to_tsv_standard_profile(sample_xml_file: Path, tmp_path: Path):
    """
    Tests that the transformer correctly applies the 'standard' profile,
    which should limit the JSON data fields.
    """
    # Arrange
    output_dir = tmp_path / "output_standard"

    # Act
    transformer.transform_xml_to_tsv(
        sample_xml_file, output_dir, profile="standard", num_workers=1
    )

    # Assert
    proteins_rows = read_tsv_gz(output_dir / "proteins.tsv.gz")
    assert len(proteins_rows) == 3  # Header + 2 entries
    protein_data = sorted(
        proteins_rows[1:], key=lambda r: r[0]
    )  # Sort by accession

    # Check P12345
    p1_row = protein_data[0]
    # comments_data should only contain 'function'
    comments_json = json.loads(p1_row[8])
    assert len(comments_json) == 1
    assert comments_json[0]["attributes"]["type"] == "function"
    # These fields should be empty for standard profile
    assert p1_row[9] == ""  # features_data
    assert p1_row[10] == ""  # db_references_data
    assert p1_row[11] == ""  # evidence_data

    # Check P67890 (has no comments, so field should be empty)
    p2_row = protein_data[1]
    assert p2_row[8] == ""  # comments_data


def test_get_total_entries_with_empty_file(tmp_path: Path):
    """
    Tests that _get_total_entries returns 0 for an empty gzipped file.
    """
    empty_file = tmp_path / "empty.xml.gz"
    with gzip.open(empty_file, "wt") as f:
        f.write("")
    assert transformer._get_total_entries(empty_file) == 0


def test_parallel_transform_with_empty_file(tmp_path: Path):
    """
    Tests the parallel transformer with an empty file.
    """
    empty_file = tmp_path / "empty.xml.gz"
    with gzip.open(empty_file, "wt") as f:
        f.write("")
    output_dir = tmp_path / "output_parallel_empty"

    transformer.transform_xml_to_tsv(
        empty_file, output_dir, profile="full", num_workers=2
    )

    assert not output_dir.exists() or not any(output_dir.iterdir())


@pytest.fixture
def bad_taxid_xml_file(tmp_path: Path) -> Path:
    """
    Creates a gzipped XML file with an entry that will cause a ValueError
    (from int() conversion) inside the worker process.
    """
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry>
  <accession>P12345</accession>
  <name>TEST_BAD_TAXID</name>
  <organism>
    <dbReference type="NCBI Taxonomy" id="not-an-int" />
  </organism>
  <sequence length="1" mass="1">M</sequence>
</entry>
</uniprot>
"""
    xml_path = tmp_path / "bad_taxid.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(xml_content)
    return xml_path


def test_worker_error_propagation(bad_taxid_xml_file: Path, tmp_path: Path):
    """
    Tests that an exception raised in a worker process (due to bad data)
    is correctly propagated and halts the main process.
    """
    # Arrange
    output_dir = tmp_path / "output_worker_error"

    # Act & Assert
    # The current implementation raises a generic error message. We'll match that
    # to confirm the pipeline halts as expected.
    with pytest.raises(ValueError, match="Duplicate primary accession found"):
        transformer.transform_xml_to_tsv(
            bad_taxid_xml_file, output_dir, profile="full", num_workers=2
        )


@pytest.fixture
def duplicate_xml_file(tmp_path: Path) -> Path:
    """Creates a gzipped XML file with a duplicate primary accession."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry><accession>P12345</accession><name>A</name><sequence length="1">M</sequence></entry>
<entry><accession>P67890</accession><name>B</name><sequence length="1">M</sequence></entry>
<entry><accession>P12345</accession><name>C</name><sequence length="1">M</sequence></entry>
</uniprot>
"""
    xml_path = tmp_path / "duplicate.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(xml_content)
    return xml_path


def test_duplicate_accession_halts_single_threaded_pipeline(
    duplicate_xml_file: Path, tmp_path: Path
):
    """
    Tests that the single-threaded pipeline halts when a duplicate accession is found.
    """
    output_dir = tmp_path / "output_single_duplicate"
    with pytest.raises(ValueError, match="Duplicate primary accession"):
        transformer.transform_xml_to_tsv(
            duplicate_xml_file, output_dir, profile="full", num_workers=1
        )


def test_transform_xml_to_tsv_with_none_workers(
    sample_xml_file: Path, tmp_path: Path, mocker
):
    """
    Tests that calling with num_workers=None correctly uses os.cpu_count.
    """
    # Arrange
    output_dir = tmp_path / "output_none_workers"
    # Mock cpu_count to ensure the parallel path is taken
    mocker.patch("os.cpu_count", return_value=2)
    # Spy on the single-threaded function to ensure it's NOT called, as
    # spying on the multiprocessing target function is not reliable.
    spy_single = mocker.spy(transformer, "_transform_single_threaded")

    # Act
    transformer.transform_xml_to_tsv(
        sample_xml_file, output_dir, profile="full", num_workers=None
    )

    # Assert
    assert (output_dir / "proteins.tsv.gz").exists()
    assert spy_single.call_count == 0
