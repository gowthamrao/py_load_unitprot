import pytest
import gzip
import csv
from pathlib import Path
from py_load_uniprot import transformer

# A small, representative UniProt XML sample
SAMPLE_XML_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<uniprot xmlns="http://uniprot.org/uniprot" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://uniprot.org/uniprot http://www.uniprot.org/support/docs/uniprot.xsd">
<entry dataset="Swiss-Prot" created="2000-05-30" modified="2024-07-17" version="150">
  <accession>P12345</accession>
  <accession>Q9Y5Y5</accession>
  <name>TEST1_HUMAN</name>
  <sequence length="10" mass="1111" checksum="abcde" modified="2000-05-30" version="1">MTESTSEQ</sequence>
</entry>
<entry dataset="TrEMBL" created="2010-10-12" modified="2024-07-18" version="100">
  <accession>P67890</accession>
  <name>TEST2_MOUSE</name>
  <sequence length="20" mass="2222" checksum="fghij" modified="2010-10-12" version="1">MANOTHERTESTSEQ</sequence>
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

def test_transform_xml_to_tsv_creates_correct_output(sample_xml_file: Path, tmp_path: Path):
    """
    Tests that the transformer correctly parses a sample XML and produces
    the expected TSV output.
    """
    # Arrange
    output_dir = tmp_path / "output"

    # Act
    transformer.transform_xml_to_tsv(sample_xml_file, output_dir)

    # Assert
    output_tsv = output_dir / "proteins.tsv.gz"
    assert output_tsv.exists()

    with gzip.open(output_tsv, "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        rows = list(reader)

    # Check header
    expected_header = [
        "primary_accession", "uniprot_id", "sequence_length", "molecular_weight",
        "created_date", "modified_date", "sequence"
    ]
    assert rows[0] == expected_header

    # Check data rows
    assert len(rows) == 3  # Header + 2 data rows

    # Check first data row
    assert rows[1] == [
        "P12345", "TEST1_HUMAN", "10", "1111", "2000-05-30", "2024-07-17", "MTESTSEQ"
    ]
    # Check second data row
    assert rows[2] == [
        "P67890", "TEST2_MOUSE", "20", "2222", "2010-10-12", "2024-07-18", "MANOTHERTESTSEQ"
    ]
