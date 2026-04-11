CREATE TABLE IF NOT EXISTS dn_table (
    dn_index INT AUTO_INCREMENT PRIMARY KEY,
    dn_id VARCHAR(255) NOT NULL UNIQUE,
    dn_address VARCHAR(255) NOT NULL,
    dn_port INT NOT NULL,
    dn_status VARCHAR(50) NOT NULL,
    dn_last_heartbeat TIMESTAMP NOT NULL,
    dn_capacity BIGINT NOT NULL,
    dn_used BIGINT NOT NULL,
    dn_available BIGINT NOT NULL,
);
