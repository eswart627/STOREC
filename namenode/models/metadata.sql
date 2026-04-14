CREATE TABLE IF NOT EXISTS metadata_table (
    id INTEGER AUTO_INCREMENT PRIMARY KEY ,
    file_id TEXT NOT NULL,
    block_id TEXT NOT NULL,
    size INT NOT NULL,
    node_id TEXT NOT NULL
);