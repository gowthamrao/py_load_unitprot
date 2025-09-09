CREATE TABLE IF NOT EXISTS __{SCHEMA_NAME}__.py_load_uniprot_metadata (
    version VARCHAR(255) PRIMARY KEY,
    release_date DATE,
    load_timestamp TIMESTAMPTZ DEFAULT NOW(),
    swissprot_entry_count INTEGER,
    trembl_entry_count INTEGER
);

CREATE TABLE IF NOT EXISTS __{SCHEMA_NAME}__.load_history (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(36),
    status VARCHAR(50),
    mode VARCHAR(50),
    dataset VARCHAR(50),
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    error_message TEXT
);
