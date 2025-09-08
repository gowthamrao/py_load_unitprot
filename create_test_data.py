import gzip
from pathlib import Path

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

data_dir = Path("data")
data_dir.mkdir(exist_ok=True)
xml_path = data_dir / "uniprot_sprot.xml.gz"

with gzip.open(xml_path, "wt", encoding="utf-8") as f:
    f.write(SAMPLE_XML_CONTENT)

print(f"Created test file at {xml_path}")
