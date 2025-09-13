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
