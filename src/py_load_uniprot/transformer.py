import csv
import gzip
from pathlib import Path
from lxml import etree
from rich.progress import Progress

from py_load_uniprot.config import settings

# UniProt XML namespace
UNIPROT_NAMESPACE = "{http://uniprot.org/uniprot}"

def _get_tag(tag_name: str) -> str:
    """Prepends the UniProt XML namespace to a tag name."""
    return f"{UNIPROT_NAMESPACE}{tag_name}"

def transform_xml_to_tsv(xml_file: Path, output_dir: Path):
    """
    Parses a UniProt XML file (e.g., uniprot_sprot.xml.gz) and transforms the
    core protein data into a gzipped TSV file.

    This function uses a streaming parser (`lxml.etree.iterparse`) to handle
    very large XML files without loading them entirely into memory.

    Args:
        xml_file: Path to the compressed UniProt XML file.
        output_dir: Directory where the output TSV file will be saved.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_tsv = output_dir / "proteins.tsv.gz"

    # These are the columns for our target 'proteins' table
    headers = [
        "primary_accession",
        "uniprot_id",
        "sequence_length",
        "molecular_weight",
        "created_date",
        "modified_date",
        "sequence",
    ]

    print(f"Starting transformation of {xml_file.name}...")
    print(f"Output will be written to: {output_tsv}")

    with gzip.open(xml_file, "rb") as f_in, gzip.open(output_tsv, "wt", encoding="utf-8") as f_out:
        writer = csv.writer(f_out, delimiter="\t", lineterminator='\n')
        writer.writerow(headers)

        context = etree.iterparse(f_in, events=("end",), tag=_get_tag("entry"))

        with Progress() as progress:
            task = progress.add_task(f"[cyan]Parsing {xml_file.name}[/]", total=None)
            count = 0
            for event, elem in context:
                # Extract data based on the XSD and FRD
                # The first accession is the primary one
                primary_accession = elem.findtext(_get_tag("accession"))
                # The first name is the UniProt ID
                uniprot_id = elem.findtext(_get_tag("name"))

                seq_elem = elem.find(_get_tag("sequence"))
                if seq_elem is not None:
                    sequence_length = seq_elem.get("length")
                    molecular_weight = seq_elem.get("mass")
                    sequence = seq_elem.text
                else:
                    sequence_length, molecular_weight, sequence = None, None, None

                created_date = elem.get("created")
                modified_date = elem.get("modified")

                writer.writerow([
                    primary_accession,
                    uniprot_id,
                    sequence_length,
                    molecular_weight,
                    created_date,
                    modified_date,
                    sequence,
                ])
                count += 1
                progress.update(task, advance=1)

                # Crucial memory management step for lxml
                # Clear the element and its descendants
                elem.clear()
                # Clear the preceding siblings of the element
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

            # Finalize progress bar
            progress.update(task, description=f"[green]Finished parsing {count} entries.[/]")

    print(f"[bold green]Transformation complete.[/bold green]")
