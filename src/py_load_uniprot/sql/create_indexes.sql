-- B-Tree Indexes for foreign keys and common lookups
CREATE INDEX IF NOT EXISTS idx_proteins_uniprot_id ON __{SCHEMA_NAME}__.proteins (uniprot_id);
CREATE INDEX IF NOT EXISTS idx_accessions_secondary ON __{SCHEMA_NAME}__.accessions (secondary_accession);
CREATE INDEX IF NOT EXISTS idx_genes_name ON __{SCHEMA_NAME}__.genes (gene_name);
CREATE INDEX IF NOT EXISTS idx_keywords_label ON __{SCHEMA_NAME}__.keywords (keyword_label);
CREATE INDEX IF NOT EXISTS idx_prot_to_go_id ON __{SCHEMA_NAME}__.protein_to_go (go_term_id);
CREATE INDEX IF NOT EXISTS idx_prot_to_taxo_id ON __{SCHEMA_NAME}__.protein_to_taxonomy (ncbi_taxid);

-- GIN Indexes for JSONB columns
CREATE INDEX IF NOT EXISTS idx_proteins_comments_gin ON __{SCHEMA_NAME}__.proteins USING GIN (comments_data);
CREATE INDEX IF NOT EXISTS idx_proteins_features_gin ON __{SCHEMA_NAME}__.proteins USING GIN (features_data);
CREATE INDEX IF NOT EXISTS idx_proteins_db_refs_gin ON __{SCHEMA_NAME}__.proteins USING GIN (db_references_data);
