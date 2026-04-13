CREATE TABLE IF NOT EXISTS file_table (
    file_name VARCHAR(255) PRIMARY KEY,
    size BIGINT NOT NULL,
    block_count INT NOT NULL,
    start_index INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);