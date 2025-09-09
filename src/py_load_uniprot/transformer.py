import csv
import gzip
import json
import multiprocessing
import os
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

from lxml import etree
from rich.progress import BarColumn, Progress, TextColumn

# UniProt XML namespace
UNIPROT_NAMESPACE = "{http://uniprot.org/uniprot}"

# Dictionary mapping table names to their headers
TABLE_HEADERS: dict[str, list[str]] = {
    "proteins": [
        "primary_accession",
        "uniprot_id",
        "ncbi_taxid",
        "sequence_length",
        "molecular_weight",
        "created_date",
        "modified_date",
        "comments_data",
        "features_data",
        "db_references_data",
        "evidence_data",
    ],
    "sequences": ["primary_accession", "sequence"],
    "accessions": ["protein_accession", "secondary_accession"],
    "taxonomy": ["ncbi_taxid", "scientific_name", "lineage"],
    "genes": ["protein_accession", "gene_name", "is_primary"],
    "protein_to_go": ["protein_accession", "go_term_id"],
    "keywords": ["protein_accession", "keyword_id", "keyword_label"],
}


def _get_tag(tag_name: str) -> str:
    """Prepends the UniProt XML namespace to a tag name."""
    return f"{UNIPROT_NAMESPACE}{tag_name}"


def _element_to_json(element: Optional[list[etree._Element]]) -> str | None:
    """Converts an lxml element and its children into a JSON string."""
    if element is None:
        return None

    def element_to_dict(el: etree._Element) -> dict[str, Any]:
        # Basic tag info
        d: dict[str, Any] = {"tag": etree.QName(el).localname}
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


def _worker_parse_entry(
    tasks_queue: Any,
    results_queue: Any,
    profile: str,
) -> None:
    """
    Worker process function.
    Pulls a raw XML string from the tasks queue, parses it, and puts the
    structured data dictionary onto the results queue.
    """
    while True:
        xml_string = tasks_queue.get()
        if xml_string is None:  # Sentinel value to signal termination
            break
        try:
            # fromstring is faster than parsing a file-like object
            elem = etree.fromstring(xml_string)
            parsed_data = _parse_entry(elem, profile)
            results_queue.put(parsed_data)
        except etree.XMLSyntaxError:
            # Handle potential errors in XML snippets
            # In a real-world scenario, you might log this
            results_queue.put({})  # Put an empty dict to not stall the writer


def _parse_entry(elem: etree._Element, profile: str) -> dict[str, list[Any]]:
    """
    Parses a single <entry> element from the UniProt XML and extracts data
    for all target tables.
    """
    data: dict[str, list[Any]] = defaultdict(list)

    primary_accession = elem.findtext(_get_tag("accession"))
    if not primary_accession:
        return {}  # Skip entry if it has no primary accession

    # --- Proteins and Sequences ---
    uniprot_id = elem.findtext(_get_tag("name"))
    seq_elem = elem.find(_get_tag("sequence"))
    sequence_length = seq_elem.get("length") if seq_elem is not None else None
    molecular_weight = seq_elem.get("mass") if seq_elem is not None else None
    sequence = (
        seq_elem.text.replace("\n", "")
        if seq_elem is not None and seq_elem.text
        else None
    )
    created_date = elem.get("created")
    modified_date = elem.get("modified")

    # --- JSONB Data ---
    if profile == "full":
        comments_data = _element_to_json(elem.findall(_get_tag("comment")))
        features_data = _element_to_json(elem.findall(_get_tag("feature")))
        # Exclude GO, taxo, etc from general db references
        db_refs_to_exclude = {"GO", "NCBI Taxonomy"}
        db_references_data = _element_to_json(
            [
                db_ref
                for db_ref in elem.findall(_get_tag("dbReference"))
                if db_ref.get("type") not in db_refs_to_exclude
            ]
        )
        evidence_data = _element_to_json(elem.findall(f".//{_get_tag('evidence')}"))
    else:  # standard profile
        all_comments = elem.findall(_get_tag("comment"))
        standard_comment_types = {"function", "disease", "subcellular location"}
        standard_comments = [
            c for c in all_comments if c.get("type") in standard_comment_types
        ]
        comments_data = _element_to_json(standard_comments)
        features_data = None
        db_references_data = None
        evidence_data = None

    data["proteins"].append(
        [
            primary_accession,
            uniprot_id,
            sequence_length,
            molecular_weight,
            created_date,
            modified_date,
            comments_data,
            features_data,
            db_references_data,
            evidence_data,
        ]
    )
    if sequence:
        data["sequences"].append([primary_accession, sequence])

    # --- Accessions ---
    for acc_elem in elem.findall(_get_tag("accession"))[1:]:  # Skip primary
        if acc_elem.text:
            data["accessions"].append([primary_accession, acc_elem.text])

    # --- Taxonomy ---
    ncbi_taxid = None
    org_elem = elem.find(_get_tag("organism"))
    if org_elem is not None:
        # Robustly find the taxonomy ID
        db_ref_elem = org_elem.find(f'.//{_get_tag("dbReference")}[@type="NCBI Taxonomy"]')
        if db_ref_elem is not None and db_ref_elem.get("id"):
            ncbi_taxid = int(db_ref_elem.get("id"))
            scientific_name = org_elem.findtext(_get_tag("name"))
            lineage_list = [
                t.text
                for t in org_elem.findall(_get_tag("lineage") + "/" + _get_tag("taxon"))
                if t.text
            ]
            lineage = " > ".join(lineage_list)
            data["taxonomy"].append([ncbi_taxid, scientific_name, lineage])

    # Add the extracted ncbi_taxid to the protein data.
    # It will be None if not found, which is handled by the database schema (allows NULL).
    protein_row = data["proteins"][0]
    protein_row.insert(2, ncbi_taxid) # Insert taxid after uniprot_id

    # --- Genes ---
    for gene_elem in elem.findall(_get_tag("gene")):
        is_primary = True
        for name_elem in gene_elem.findall(_get_tag("name")):
            gene_name = name_elem.text
            name_type = name_elem.get("type")
            if name_type == "primary":
                data["genes"].append([primary_accession, gene_name, is_primary])
                is_primary = False  # Only first primary name is marked as such
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
def FileWriterManager(output_dir: Path) -> Iterator[dict[str, Any]]:
    """Manages file handles and CSV writers for all output TSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_handles: dict[str, Any] = {}
    csv_writers: dict[str, Any] = {}
    try:
        for table, headers in TABLE_HEADERS.items():
            filepath = output_dir / f"{table}.tsv.gz"
            f = gzip.open(filepath, "wt", encoding="utf-8")
            file_handles[table] = f
            writer = csv.writer(f, delimiter="\t", lineterminator="\n")
            writer.writerow(headers)
            csv_writers[table] = writer
        yield csv_writers
    finally:
        for f in file_handles.values():
            f.close()


def _writer_process(
    results_queue: Any,
    output_dir: Path,
    total_entries: int,
    num_workers: int,
) -> None:
    """
    Writer process function.
    Pulls parsed data from the results queue and writes it to TSV files.
    Also manages a progress bar.
    """
    processed_count = 0
    # Use a set to efficiently handle duplicate taxonomy entries, as they are common
    seen_taxonomy_ids: set[int] = set()

    with (
        FileWriterManager(output_dir) as writers,
        Progress(
            TextColumn("[bold blue]Parsing Entries...", justify="right"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.1f}%",
            TextColumn("({task.completed} of {task.total})"),
        ) as progress,
    ):
        task = progress.add_task("Parsing...", total=total_entries)

        while processed_count < total_entries:
            parsed_data = results_queue.get()
            if parsed_data:  # Ensure it's not an empty dict from a parse error
                for table_name, rows in parsed_data.items():
                    if table_name == "taxonomy":
                        # De-duplicate taxonomy entries before writing
                        unique_rows = []
                        for row in rows:
                            tax_id = row[0]  # ncbi_taxid is the first element
                            if tax_id not in seen_taxonomy_ids:
                                unique_rows.append(row)
                                seen_taxonomy_ids.add(tax_id)
                        if unique_rows:
                            writers[table_name].writerows(unique_rows)
                    else:
                        writers[table_name].writerows(rows)
            processed_count += 1
            progress.update(task, advance=1)


def _get_total_entries(xml_file: Path) -> int:
    """Quickly count the number of <entry> tags for the progress bar."""
    print("Counting total entries for progress tracking...")
    count = 0
    with gzip.open(xml_file, "rb") as f:
        # Use iterparse on a non-blocking fast event to count entries
        for _, elem in etree.iterparse(f, events=("end",), tag=_get_tag("entry")):
            count += 1
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    print(f"Found {count} total entries.")
    return count


def transform_xml_to_tsv(
    xml_file: Path,
    output_dir: Path,
    profile: str,
    num_workers: Optional[int] = os.cpu_count(),
) -> None:
    """
    Parses a UniProt XML file in parallel and transforms the data into gzipped TSV files.
    """
    # Fallback to 1 worker if cpu_count is None
    num_workers_actual = num_workers if num_workers is not None else 1
    print(
        f"Starting parallel transformation of {xml_file.name} with {num_workers_actual} worker processes..."
    )
    print(f"Using profile: [bold cyan]{profile}[/bold cyan]")
    print(f"Output will be written to: {output_dir.resolve()}")

    total_entries = _get_total_entries(xml_file)
    if total_entries == 0:
        print(
            "[yellow]Warning: No entries found in the XML file. Nothing to do.[/yellow]"
        )
        return

    # Use a manager for shared queues between processes
    with multiprocessing.Manager() as manager:
        tasks_queue = manager.Queue(
            maxsize=num_workers_actual * 4
        )  # Bounded queue to prevent memory bloat
        results_queue = manager.Queue()

        # Start the dedicated writer process
        writer = multiprocessing.Process(
            target=_writer_process,
            args=(results_queue, output_dir, total_entries, num_workers_actual),
        )
        writer.start()

        # Start a pool of worker processes, passing the profile to each worker
        pool = multiprocessing.Pool(
            num_workers_actual,
            _worker_parse_entry,
            (
                tasks_queue,
                results_queue,
                profile,
            ),
        )

        # --- Producer ---
        # The main process becomes the producer, reading the XML and queuing tasks
        with gzip.open(xml_file, "rb") as f_in:
            for event, elem in etree.iterparse(
                f_in, events=("end",), tag=_get_tag("entry")
            ):
                # Convert the element to a string to pass it to the queue
                xml_string = etree.tostring(elem, encoding="unicode")
                tasks_queue.put(xml_string)
                # Crucial memory management
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        # Signal workers to terminate by putting None on the queue
        for _ in range(num_workers_actual):
            tasks_queue.put(None)

        # Wait for all worker processes to finish
        pool.close()
        pool.join()

        # Wait for the writer process to finish
        writer.join()

    print("[bold green]Parallel transformation complete.[/bold green]")
