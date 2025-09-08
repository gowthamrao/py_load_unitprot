import pytest
import gzip
import csv
import json
from pathlib import Path
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

def test_transform_xml_to_tsv_creates_correct_output(sample_xml_file: Path, tmp_path: Path):
    """
    Tests that the transformer correctly parses a sample XML and produces
    the expected set of TSV files with correct content.
    """
    # Arrange
    output_dir = tmp_path / "output"

    # Act
    transformer.transform_xml_to_tsv(sample_xml_file, output_dir)

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
        if table_name in ["proteins", "sequences", "accessions", "taxonomy", "protein_to_taxonomy", "genes", "protein_to_go", "keywords"]:
             assert file_path.exists(), f"File for table '{table_name}' should exist"

    # --- Assert proteins.tsv.gz ---
    proteins_rows = read_tsv_gz(output_dir / "proteins.tsv.gz")
    assert proteins_rows[0] == transformer.TABLE_HEADERS["proteins"]
    assert len(proteins_rows) == 3 # Header + 2 entries

    # Check first protein row
    p1_row = proteins_rows[1]
    assert p1_row[0:6] == ["P12345", "TEST1_HUMAN", "10", "1111", "2000-05-30", "2024-07-17"]
    # Check JSONB columns - should not be empty
    assert json.loads(p1_row[6])[0]['tag'] == 'comment'
    assert json.loads(p1_row[7])[0]['tag'] == 'feature'
    assert p1_row[8] == '' # db_references_data is empty for this entry

    # --- Assert other tables ---
    accessions_rows = read_tsv_gz(output_dir / "accessions.tsv.gz")
    assert accessions_rows == [
        transformer.TABLE_HEADERS["accessions"],
        ["P12345", "Q9Y5Y5"]
    ]

    sequences_rows = read_tsv_gz(output_dir / "sequences.tsv.gz")
    assert len(sequences_rows) == 3
    assert sequences_rows[1] == ["P12345", "MTESTSEQAA"]

    taxonomy_rows = read_tsv_gz(output_dir / "taxonomy.tsv.gz")
    assert len(taxonomy_rows) == 3
    assert taxonomy_rows[1] == ["9606", "Homo sapiens", "Eukaryota > Metazoa"]

    genes_rows = read_tsv_gz(output_dir / "genes.tsv.gz")
    assert genes_rows == [
        transformer.TABLE_HEADERS["genes"],
        ["P12345", "TP1", "True"]
    ]

    go_rows = read_tsv_gz(output_dir / "protein_to_go.tsv.gz")
    assert go_rows == [
        transformer.TABLE_HEADERS["protein_to_go"],
        ["P12345", "GO:0005515"]
    ]

    keywords_rows = read_tsv_gz(output_dir / "keywords.tsv.gz")
    assert keywords_rows == [
        transformer.TABLE_HEADERS["keywords"],
        ["P12345", "KW-0181", "Complete proteome"]
    ]
