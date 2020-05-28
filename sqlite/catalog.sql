PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE nodes (
    id INTEGER PRIMARY KEY,
    node_name TEXT NOT NULL,
    valid_thru INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    accept_new INTEGER NOT NULL DEFAULT 1,
    UNIQUE (node_name)
);
CREATE TABLE data (
    id INTEGER PRIMARY KEY,
    data_name TEXT NOT NULL,
    hash TEXT NOT NULL,
    desired_copies INTEGER NOT NULL DEFAULT 3,
    UNIQUE (data_name)
);
CREATE TABLE data_nodes (
    id INTEGER PRIMARY KEY,
    data_id INTEGER,
    node_id INTEGER,
    UNIQUE (data_id, node_id)
);
COMMIT;
