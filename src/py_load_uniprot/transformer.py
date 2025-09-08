import csv
import gzip
import json
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from lxml import etree
from rich.progress import Progress

# UniProt XML namespace
UNIPROT_NAMESPACE = "{http://uniprot.org/uniprot}"

# Dictionary mapping table names to their headers
TABLE_HEADERS = {
    "proteins": ["primary_accession", "uniprot_id", "sequence_length", "molecular_weight", "created_date", "modified_date", "comments_data", "features_data", "db_references_data"],
    "sequences": ["primary_accession", "sequence"],
    "accessions": ["protein_accession", "secondary_accession"],
    "taxonomy": ["ncbi_taxid", "scientific_name", "lineage"],
    "protein_to_taxonomy": ["protein_accession", "ncbi_taxid"],
    "genes": ["protein_accession", "gene_name", "is_primary"],
    "protein_to_go": ["protein_accession", "go_term_id"],
    "keywords": ["protein_accession", "keyword_id", "keyword_label"],
}

def _get_tag(tag_name: str) -> str:
    """Prepends the UniProt XML namespace to a tag name."""
    return f"{UNIPROT_NAMESPACE}{tag_name}"

def _element_to_json(element) -> str | None:
    """Converts an lxml element and its children into a JSON string."""
    if element is None:
        return None

    def element_to_dict(el):
        # Basic tag info
        d = {"tag": etree.QName(el).localname}
        # Add attributes, excluding namespace info
        if el.attrib:
            d["attributes"] = {k: v for k, v in el.attrib.items()}
        # Add text content if it exists
        if el.text and el.text.strip():
            d["text"] = el.text.strip()
        # Recursively add children
        children = [element_to_dict(child) for child in el]
        if children:
            d["children"] = children
        return d

    # UniProt often has a list of elements of the same type (e.g., multiple 'comment' tags)
    # We will handle this by creating a list of dictionaries
    data_list = [element_to_dict(el) for el in element]
    return json.dumps(data_list) if data_list else None

def _parse_entry(elem) -> dict[str, list]:
    """
    Parses a single <entry> element from the UniProt XML and extracts data
    for all target tables.
    """
    data = defaultdict(list)

    primary_accession = elem.findtext(_get_tag("accession"))
    if not primary_accession:
        return {} # Skip entry if it has no primary accession

    # --- Proteins and Sequences ---
    uniprot_id = elem.findtext(_get_tag("name"))
    seq_elem = elem.find(_get_tag("sequence"))
    sequence_length = seq_elem.get("length") if seq_elem is not None else None
    molecular_weight = seq_elem.get("mass") if seq_elem is not None else None
    sequence = seq_elem.text.replace("\n", "") if seq_elem is not None and seq_elem.text else None
    created_date = elem.get("created")
    modified_date = elem.get("modified")

    # --- JSONB Data ---
    comments_data = _element_to_json(elem.findall(_get_tag("comment")))
    features_data = _element_to_json(elem.findall(_get_tag("feature")))
    # Exclude GO, taxo, etc from general db references
    db_refs_to_exclude = {'GO', 'NCBI Taxonomy'}
    db_references_data = _element_to_json([
        db_ref for db_ref in elem.findall(_get_tag("dbReference"))
        if db_ref.get("type") not in db_refs_to_exclude
    ])

    data["proteins"].append([
        primary_accession, uniprot_id, sequence_length, molecular_weight,
        created_date, modified_date, comments_data, features_data, db_references_data
    ])
    if sequence:
        data["sequences"].append([primary_accession, sequence])

    # --- Accessions ---
    for acc_elem in elem.findall(_get_tag("accession"))[1:]: # Skip primary
        data["accessions"].append([primary_accession, acc_elem.text])

    # --- Taxonomy ---
    org_elem = elem.find(_get_tag("organism"))
    if org_elem is not None:
        taxid = org_elem.find(_get_tag("dbReference")).get("id")
        if taxid:
            taxid = int(taxid)
            scientific_name = org_elem.findtext(_get_tag("name"))
            lineage_list = [t.text for t in org_elem.findall(_get_tag("lineage") + "/" + _get_tag("taxon"))]
            lineage = " > ".join(lineage_list)
            data["taxonomy"].append([taxid, scientific_name, lineage])
            data["protein_to_taxonomy"].append([primary_accession, taxid])

    # --- Genes ---
    for gene_elem in elem.findall(_get_tag("gene")):
        is_primary = True
        for name_elem in gene_elem.findall(_get_tag("name")):
            gene_name = name_elem.text
            name_type = name_elem.get("type")
            if name_type == "primary":
                data["genes"].append([primary_accession, gene_name, is_primary])
                is_primary = False # Only first primary name is marked as such
            elif name_type in ("synonym", "ordered locus"):
                data["genes"].append([primary_accession, gene_name, False])

    # --- GO Terms ---
    for go_ref in elem.findall(f'.//{_get_tag("dbReference")}[@type="GO"]'):
        go_id = go_ref.get("id")
        if go_id:
            data["protein_to_go"].append([primary_accession, go_id])

    # --- Keywords ---
    for kw_elem in elem.findall(_get_tag("keyword")):
        kw_id = kw_elem.get("id")
        kw_label = kw_elem.text
        if kw_id:
            data["keywords"].append([primary_accession, kw_id, kw_label])

    return data

@contextmanager
def FileWriterManager(output_dir: Path):
    """Manages file handles and CSV writers for all output TSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_handles = {}
    csv_writers = {}
    try:
        for table, headers in TABLE_HEADERS.items():
            filepath = output_dir / f"{table}.tsv.gz"
            f = gzip.open(filepath, "wt", encoding="utf-8")
            file_handles[table] = f
            writer = csv.writer(f, delimiter="\t", lineterminator='\n')
            writer.writerow(headers)
            csv_writers[table] = writer
        yield csv_writers
    finally:
        for f in file_handles.values():
            f.close()

def transform_xml_to_tsv(xml_file: Path, output_dir: Path):
    """
    Parses a UniProt XML file and transforms the data into a set of gzipped TSV
    files corresponding to the relational schema.
    """
    print(f"Starting transformation of {xml_file.name}...")
    print(f"Output will be written to: {output_dir.resolve()}")

    with gzip.open(xml_file, "rb") as f_in, FileWriterManager(output_dir) as writers:
        context = etree.iterparse(f_in, events=("end",), tag=_get_tag("entry"))

        # Use a simple counter instead of rich progress for now to avoid overhead
        count = 0
        for event, elem in context:
            parsed_data = _parse_entry(elem)

            for table_name, rows in parsed_data.items():
                # The data for taxonomy is unique, so we should avoid duplicates
                # This simple check is not perfectly efficient but handles the case
                if table_name == "taxonomy":
                    # A basic way to handle duplicate taxonomy entries in a batch
                    # A more robust solution might use a set outside the loop
                    pass # For now, we accept duplicates, DB will handle on INSERT
                writers[table_name].writerows(rows)

            count += 1
            if count % 10000 == 0:
                print(f"  ...processed {count} entries")

            # Crucial memory management
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        print(f"[green]Finished parsing {count} entries.[/green]")

    print(f"[bold green]Transformation complete.[/bold green]")
