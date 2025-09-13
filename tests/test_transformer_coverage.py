import gzip
import json
from pathlib import Path

import pytest
from lxml import etree

from py_load_uniprot import transformer

EMPTY_XML_CONTENT_NO_ENTRIES = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
</uniprot>
"""


@pytest.fixture
def empty_xml_file_no_entries(tmp_path: Path) -> Path:
    """Creates a gzipped sample XML file for testing."""
    xml_path = tmp_path / "sample.xml.gz"
    with gzip.open(xml_path, "wt", encoding="utf-8") as f:
        f.write(EMPTY_XML_CONTENT_NO_ENTRIES)
    return xml_path


def test_get_total_entries_with_no_entries(empty_xml_file_no_entries: Path):
    """
    Tests that _get_total_entries returns 0 for a file with no <entry> tags.
    """
    assert transformer._get_total_entries(empty_xml_file_no_entries) == 0


def test_transform_single_threaded_no_entries(empty_xml_file_no_entries: Path, tmp_path: Path):
    """
    Tests that _transform_single_threaded handles a file with no entries correctly.
    """
    output_dir = tmp_path / "output"
    transformer._transform_single_threaded(empty_xml_file_no_entries, output_dir, profile="full")
    assert not output_dir.exists() or not any(output_dir.iterdir())


XML_CONTENT_MULTIPLE_GENE_NAMES = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <name>TEST1_HUMAN</name>
  <protein>
    <recommendedName><fullName>Test protein 1</fullName></recommendedName>
  </protein>
  <gene>
    <name type="primary">TP1</name>
    <name type="synonym">TP1A</name>
    <name type="ordered locus">TP1B</name>
  </gene>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
</entry>
</uniprot>
"""


def get_entry_element(xml_content: str) -> etree._Element:
    """Helper to get the first <entry> element from an XML string."""
    root = etree.fromstring(xml_content.encode("utf-8"))
    return root.find(transformer._get_tag("entry"))


def test_parse_entry_with_multiple_gene_names():
    """
    Tests parsing an entry with multiple gene names, including synonyms.
    """
    elem = get_entry_element(XML_CONTENT_MULTIPLE_GENE_NAMES)
    parsed_data = transformer._parse_entry(elem, profile="full")
    assert "genes" in parsed_data
    assert len(parsed_data["genes"]) == 3
    assert parsed_data["genes"][0] == ["P12345", "TP1", True]
    assert parsed_data["genes"][1] == ["P12345", "TP1A", False]
    assert parsed_data["genes"][2] == ["P12345", "TP1B", False]


XML_CONTENT_STANDARD_PROFILE_NO_STANDARD_COMMENTS = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <name>TEST1_HUMAN</name>
  <protein>
    <recommendedName><fullName>Test protein 1</fullName></recommendedName>
  </protein>
  <comment type="random"><text>A random comment.</text></comment>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
</entry>
</uniprot>
"""


def test_parse_entry_standard_profile_no_comments():
    """
    Tests the 'standard' profile when there are no comments of the standard types.
    """
    elem = get_entry_element(XML_CONTENT_STANDARD_PROFILE_NO_STANDARD_COMMENTS)
    parsed_data = transformer._parse_entry(elem, profile="standard")
    protein_row = parsed_data["proteins"][0]
    comments_data = protein_row[8]
    assert comments_data is None


XML_CONTENT_FULL_PROFILE_NO_EXCLUDED_DB_REFS = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <name>TEST1_HUMAN</name>
  <protein>
    <recommendedName><fullName>Test protein 1</fullName></recommendedName>
  </protein>
  <dbReference type="EMBL" id="EM12345"/>
  <sequence length="10" mass="1111">MTESTSEQAA</sequence>
</entry>
</uniprot>
"""


def test_parse_entry_full_profile_no_db_references():
    """
    Tests the 'full' profile when there are no db references to exclude.
    """
    elem = get_entry_element(XML_CONTENT_FULL_PROFILE_NO_EXCLUDED_DB_REFS)
    parsed_data = transformer._parse_entry(elem, profile="full")
    protein_row = parsed_data["proteins"][0]
    db_references_data = protein_row[10]
    db_refs_list = json.loads(db_references_data)
    assert len(db_refs_list) == 1
    assert db_refs_list[0]["attributes"]["type"] == "EMBL"


XML_CONTENT_NO_SEQUENCE_TEXT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot">
<entry>
  <accession>P12345</accession>
  <name>TEST_NO_SEQ_TEXT</name>
  <sequence length="0" mass="0"></sequence>
</entry>
</uniprot>
"""


def test_parse_entry_no_sequence_text():
    """
    Tests that _parse_entry handles a sequence tag with no text content.
    """
    elem = get_entry_element(XML_CONTENT_NO_SEQUENCE_TEXT)
    parsed_data = transformer._parse_entry(elem, profile="full")
    assert "sequences" not in parsed_data


def test_element_to_json():
    """
    Tests the _element_to_json function with a simple element.
    """
    xml = """<root><child attribute="value">text</child></root>"""
    root = etree.fromstring(xml)
    json_str = transformer._element_to_json(list(root))
    data = json.loads(json_str)
    assert len(data) == 1
    assert data[0]["tag"] == "child"
    assert data[0]["attributes"]["attribute"] == "value"
    assert data[0]["text"] == "text"


def test_writer_process_writes_data(tmp_path: Path):
    """
    Tests that the _writer_process function correctly writes data from the
    results queue to the appropriate TSV files.
    """
    # Arrange
    output_dir = tmp_path / "output"
    # Can't use multiprocessing.Manager in a test, so use standard Queue
    results_queue = transformer.multiprocessing.Queue()
    error_event = transformer.multiprocessing.Event()
    total_entries = 2

    # Add some sample parsed data to the queue
    results_queue.put({
        "proteins": [["P12345", "ID1", "PROT1", 9606, 10, 100, "d1", "d2", "{}", "{}", "{}", "{}"]],
        "sequences": [["P12345", "SEQ1"]],
        "accessions": [["P12345", "S1"]],
        "genes": [["P12345", "GENE1", True]],
        "keywords": [["P12345", "KW1", "Keyword 1"]],
    })
    results_queue.put({
        "proteins": [["P67890", "ID2", "PROT2", 10090, 20, 200, "d3", "d4", "{}", "{}", "{}", "{}"]],
        "taxonomy": [[10090, "Mus musculus", "lineage"]],
        "protein_to_go": [["P67890", "GO:1234"]],
        "genes": [["P67890", "GENE2", False]],
        "keywords": [["P67890", "KW2", "Keyword 2"]],
    })

    # Act
    transformer._writer_process(results_queue, output_dir, total_entries, 1, error_event)

    # Assert
    # Check that the files are created and have the correct content
    proteins_file = output_dir / "proteins.tsv.gz"
    sequences_file = output_dir / "sequences.tsv.gz"
    accessions_file = output_dir / "accessions.tsv.gz"
    taxonomy_file = output_dir / "taxonomy.tsv.gz"
    go_file = output_dir / "protein_to_go.tsv.gz"

    assert proteins_file.exists()
    assert sequences_file.exists()
    assert accessions_file.exists()
    assert taxonomy_file.exists()
    assert go_file.exists()

    # A helper to read gzipped tsv files
    def read_gz_tsv(path):
        with gzip.open(path, "rt") as f:
            return [line.strip().split("	") for line in f]

    proteins_content = read_gz_tsv(proteins_file)
    assert len(proteins_content) == 3  # Header + 2 rows
    assert proteins_content[1] == ["P12345", "ID1", "PROT1", "9606", "10", "100", "d1", "d2", "{}", "{}", "{}", "{}"]

    sequences_content = read_gz_tsv(sequences_file)
    assert len(sequences_content) == 2  # Header + 1 row
    assert sequences_content[1] == ["P12345", "SEQ1"]


def test_writer_process_handles_exceptions(tmp_path: Path):
    """
    Tests that the _writer_process function correctly handles an exception
    passed through the results queue.
    """
    # Arrange
    output_dir = tmp_path / "output"
    results_queue = transformer.multiprocessing.Queue()
    error_event = transformer.multiprocessing.Event()
    total_entries = 1

    # Put an exception on the queue
    results_queue.put(ValueError("Test worker error"))

    # Act
    transformer._writer_process(results_queue, output_dir, total_entries, 1, error_event)

    # Assert
    assert error_event.is_set()
    assert output_dir.exists()

    # Check that files were created, but contain only the header
    proteins_file = output_dir / "proteins.tsv.gz"
    assert proteins_file.exists()

    with gzip.open(proteins_file, "rt") as f:
        lines = f.readlines()
        assert len(lines) == 1
        assert lines[0].strip() == "	".join(transformer.TABLE_HEADERS["proteins"])


def test_worker_parse_entry_exception(mocker):
    """
    Tests that _worker_parse_entry correctly catches an exception during parsing
    and puts it on the results queue.
    """
    # Arrange
    tasks_queue = transformer.multiprocessing.Queue()
    results_queue = transformer.multiprocessing.Queue()
    tasks_queue.put("<malformed_xml>")
    tasks_queue.put(None)  # Sentinel

    mocker.patch('lxml.etree.fromstring', side_effect=ValueError("Malformed XML"))

    # Act
    transformer._worker_parse_entry(tasks_queue, results_queue, "full")

    # Assert
    result = results_queue.get()
    assert isinstance(result, ValueError)
    assert "Malformed XML" in str(result)
