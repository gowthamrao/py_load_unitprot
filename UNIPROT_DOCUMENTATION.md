# Understanding the UniProt Data

This document provides an overview of the UniProt data source, its structure, and how it is mapped to the database schema by the `py-load-uniprot` ETL package.

## 1. Introduction to UniProt

UniProt is a comprehensive, high-quality, and freely accessible resource of protein sequence and functional information. It is a central hub for protein data, created by combining information from multiple sources to provide a complete picture of our knowledge about a particular protein.

The data in UniProt is essential for a wide range of biological research, from genomics and proteomics to systems biology and drug discovery.

## 2. The UniProt Knowledgebase (UniProtKB)

The core of UniProt is the UniProt Knowledgebase (UniProtKB), which is the most comprehensive and widely used protein information resource. UniProtKB consists of two sections:

*   **UniProtKB/Swiss-Prot:** This is the manually annotated and reviewed section of UniProtKB. It contains high-quality, non-redundant protein entries. The information is curated by expert biologists, ensuring a high level of accuracy and detail. When you need reliable, well-documented protein information, Swiss-Prot is the gold standard.

*   **UniProtKB/TrEMBL (Translated EMBL Nucleotide Sequence Data Library):** This is the computationally annotated and unreviewed section of UniProtKB. It contains translations of all coding sequences present in the public nucleotide sequence databases. TrEMBL provides a vast amount of protein sequence data, but the annotations are automatic and have not been manually checked. It is a valuable resource for its breadth of coverage.

The `py-load-uniprot` package can process data from both Swiss-Prot and TrEMBL.

## 3. Data Source and Format

The `py-load-uniprot` package consumes data directly from the official UniProt FTP site. The data is provided as large, compressed XML files.

*   **Source:** UniProt FTP Server (ftp.uniprot.org)
*   **Format:** UniProt XML format, compressed with gzip (e.g., `uniprot_sprot.xml.gz`).

The ETL process is designed to parse these large XML files in a memory-efficient way, making it possible to process the entire UniProtKB on standard hardware.

## 4. Database Schema Explained

The UniProt XML data is normalized and loaded into a relational database schema. This schema is designed to provide a structured and queryable representation of the most important data from UniProt.

Below is a description of each table created by the ETL process and the UniProt XML elements they are derived from.

### `proteins`

This is the central table, containing one row for each UniProt entry.

| Column               | Type    | Description                                                                                                                              | UniProt XML Source                                                                  |
| -------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `primary_accession`  | `TEXT`  | The primary accession number of the protein entry. This is the stable, unique identifier for a UniProt entry.                            | `<entry>/<accession>` (the first one)                                               |
| `uniprot_id`         | `TEXT`  | The unique, stable identifier for a UniProtKB entry (e.g., `BRCA2_HUMAN`).                                                                 | `<entry>/<name>`                                                                    |
| `ncbi_taxid`         | `INT`   | The NCBI taxonomy identifier for the organism.                                                                                           | `<entry>/<organism>/<dbReference type="NCBI Taxonomy">`                             |
| `sequence_length`    | `INT`   | The number of amino acids in the protein sequence.                                                                                       | `<entry>/<sequence>` attribute `length`                                             |
| `molecular_weight`   | `INT`   | The molecular weight of the protein in Daltons.                                                                                          | `<entry>/<sequence>` attribute `mass`                                               |
| `created_date`       | `DATE`  | The date the entry was created in UniProt.                                                                                               | `<entry>` attribute `created`                                                       |
| `modified_date`      | `DATE`  | The date the entry was last modified in UniProt.                                                                                         | `<entry>` attribute `modified`                                                      |
| `comments_data`      | `JSONB` | A JSON array containing various comments about the protein (function, subcellular location, etc.). See Section 5 for details.              | `<entry>/<comment>`                                                                 |
| `features_data`      | `JSONB` | A JSON array describing regions or sites of interest in the protein sequence (e.g., active sites, domains). See Section 5 for details.   | `<entry>/<feature>`                                                                 |
| `db_references_data` | `JSONB` | A JSON array of cross-references to other databases (e.g., PDB, Ensembl). See Section 5 for details.                                     | `<entry>/<dbReference>` (excluding GO and NCBI Taxonomy)                            |
| `evidence_data`      | `JSONB` | A JSON array containing evidence tags that support annotations. See Section 5 for details.                                               | `<entry>/<evidence>`                                                                |

### `sequences`

This table stores the amino acid sequence for each protein.

| Column              | Type   | Description                                                               | UniProt XML Source       |
| ------------------- | ------ | ------------------------------------------------------------------------- | ------------------------ |
| `primary_accession` | `TEXT` | The primary accession number, linking back to the `proteins` table.       | `<entry>/<accession>`    |
| `sequence`          | `TEXT` | The full amino acid sequence of the protein.                              | `<entry>/<sequence>`     |

### `accessions`

This table stores the secondary accession numbers for each protein entry.

| Column                | Type   | Description                                                              | UniProt XML Source    |
| --------------------- | ------ | ------------------------------------------------------------------------ | --------------------- |
| `protein_accession`   | `TEXT` | The primary accession number, linking back to the `proteins` table.      | `<entry>/<accession>` |
| `secondary_accession` | `TEXT` | A secondary accession number for the entry.                              | `<entry>/<accession>` (subsequent ones) |

### `taxonomy`

This table stores the NCBI taxonomy information for each organism.

| Column            | Type   | Description                                                               | UniProt XML Source                                |
| ----------------- | ------ | ------------------------------------------------------------------------- | ------------------------------------------------- |
| `ncbi_taxid`      | `INT`  | The NCBI taxonomy identifier.                                             | `<entry>/<organism>/<dbReference>`                |
| `scientific_name` | `TEXT` | The scientific name of the organism.                                      | `<entry>/<organism>/<name type="scientific">`      |
| `lineage`         | `TEXT` | The full taxonomic lineage of the organism.                               | `<entry>/<organism>/<lineage>/<taxon>`            |

### `genes`

This table stores the gene names associated with a protein.

| Column              | Type    | Description                                                              | UniProt XML Source                 |
| ------------------- | ------- | ------------------------------------------------------------------------ | ---------------------------------- |
| `protein_accession` | `TEXT`  | The primary accession number, linking back to the `proteins` table.      | `<entry>/<accession>`              |
| `gene_name`         | `TEXT`  | The name of the gene (e.g., `BRCA2`).                                    | `<entry>/<gene>/<name>`            |
| `is_primary`        | `BOOL`  | `TRUE` if this is the primary gene name for the protein.                 | `<entry>/<gene>/<name type="primary">` |

### `protein_to_go`

This table provides a mapping between proteins and Gene Ontology (GO) terms.

| Column              | Type   | Description                                                              | UniProt XML Source                                    |
| ------------------- | ------ | ------------------------------------------------------------------------ | ----------------------------------------------------- |
| `protein_accession` | `TEXT` | The primary accession number, linking back to the `proteins` table.      | `<entry>/<accession>`                                 |
| `go_term_id`        | `TEXT` | The Gene Ontology term identifier (e.g., `GO:0005515`).                   | `<entry>/<dbReference type="GO">`                     |

### `keywords`

This table stores the UniProt keywords associated with a protein.

| Column              | Type   | Description                                                              | UniProt XML Source       |
| ------------------- | ------ | ------------------------------------------------------------------------ | ------------------------ |
| `protein_accession` | `TEXT` | The primary accession number, linking back to the `proteins` table.      | `<entry>/<accession>`    |
| `keyword_id`        | `TEXT` | The unique identifier for the keyword (e.g., `KW-0002`).                  | `<entry>/<keyword>` attribute `id` |
| `keyword_label`     | `TEXT` | The keyword itself (e.g., `3D-structure`).                               | `<entry>/<keyword>`      |

## 5. JSON Data Fields and ETL Profiles

The `proteins` table contains several `JSONB` columns that store complex, semi-structured data. The content of these columns depends on the "profile" used when running the ETL (`--profile=standard` or `--profile=full`).

*   **`standard` profile (default):**
    *   This profile is designed for common use cases and includes a curated subset of the data to save space and improve query performance.
    *   `comments_data`: Only includes comments of type `function`, `disease`, and `subcellular location`.
    *   `features_data`, `db_references_data`, `evidence_data`: These columns will be `NULL`.

*   **`full` profile:**
    *   This profile loads the complete data from the UniProt XML, providing maximum detail.
    *   `comments_data`: Includes all types of comments.
    *   `features_data`: Includes all feature annotations.
    *   `db_references_data`: Includes all database cross-references (except for GO and NCBI Taxonomy, which are in their own tables).
    *   `evidence_data`: Includes all evidence tags.

The JSON data in these columns preserves the original structure of the XML, making it possible to perform detailed analysis that is not possible with the normalized tables alone.

## 6. Further Resources

For more detailed information about the UniProt data model and XML format, please refer to the official UniProt documentation:

*   **UniProt Home:** [https://www.uniprot.org/](https://www.uniprot.org/)
*   **UniProt FTP Site:** [https://ftp.uniprot.org/pub/databases/uniprot/](https://ftp.uniprot.org/pub/databases/uniprot/)
*   **UniProt Help/Documentation:** [https://www.uniprot.org/help/](https://www.uniprot.org/help/)
