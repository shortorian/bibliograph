CREATE TABLE sources(
    source_id INT PRIMARY KEY, --pdDtype: Int32
    comment_id INT REFERENCES comments(comment_id), --pdDtype: Int32
    source VARCHAR(255) NOT NULL, --pdDtype: object
    date_inserted DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, --pdDtype: object
    date_modified DATETIME, --pdDtype: object
    FOREIGN KEY (comment_id) REFERENCES comments(comment_id)
);

CREATE TABLE assertions(
    assertion_id INT PRIMARY KEY, --pdDtype: Int32
    source_id INT NOT NULL REFERENCES sources(source_id), --pdDtype: Int32
    src_string_id INT NOT NULL REFERENCES strings(string_id), --pdDtype: Int32
    tgt_string_id INT NOT NULL REFERENCES strings(string_id), --pdDtype: Int32
    link_type_id INT NOT NULL REFERENCES link_types(link_type_id), --pdDtype: Int32
    comment_id INT REFERENCES comments(comment_id), --pdDtype: Int32
    date_inserted DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, --pdDtype: object
    date_modified DATETIME --pdDtype: object
);

CREATE TABLE strings(
    string_id INT PRIMARY KEY, --pdDtype: Int32
    node_id INT NOT NULL REFERENCES nodes(node_id), --pdDtype: Int32
    string TEXT NOT NULL, --pdDtype: object
    date_inserted DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, --pdDtype: object
    date_modified DATETIME --pdDtype: object
);

CREATE TABLE nodes(
    node_id INT PRIMARY KEY, --pdDtype: Int32
    node_type_id INT NOT NULL REFERENCES node_types(node_type_id), --pdDtype: Int32
    name_string_id INT REFERENCES strings(string_id), --pdDtype: Int32
    abbr_string_id INT REFERENCES strings(string_id), --pdDtype: Int32
    comment_id INT REFERENCES comments(comment_id), --pdDtype: Int32
    date_inserted DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, --pdDtype: object
    date_modified DATETIME, --pdDtype: object
    CONSTRAINT node_alt_pk UNIQUE (node_id, node_type_id)
);

CREATE TABLE node_types(
    node_type_id INT PRIMARY KEY, --pdDtype: Int32
    comment_id INT REFERENCES comments(comment_id), --pdDtype: Int32
    node_type VARCHAR(50) --pdDtype: object
);

CREATE TABLE comments(
    comment_id INT PRIMARY KEY, --pdDtype: Int32
    comment TEXT, --pdDtype: object
    date_inserted DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, --pdDtype: object
    date_modified DATETIME --pdDtype: object
);

CREATE TABLE edges(
    edge_id INT PRIMARY KEY, --pdDtype: Int32
    src_node_id INT NOT NULL REFERENCES nodes(node_id), --pdDtype: Int32
    tgt_node_id INT NOT NULL REFERENCES nodes(node_id), --pdDtype: Int32
    link_type_id INT NOT NULL REFERENCES link_types(link_type_id), --pdDtype: Int32
    comment_id INT REFERENCES comments(comment_id), --pdDtype: Int32
    date_inserted DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, --pdDtype: object
    date_modified DATETIME --pdDtype: object
);

CREATE TABLE link_types(
    link_type_id INT PRIMARY KEY, --pdDtype: Int32
    comment_id INT REFERENCES comments(comment_id), --pdDtype: Int32
    link_type VARCHAR(50) NOT NULL, --pdDtype: object
    is_directed BOOLEAN NOT NULL --pdDtype: bool
);

CREATE TABLE node_tags(
    node_id INT NOT NULL REFERENCES nodes(node_id), --pdDtype: Int32
    tag_id INT NOT NULL REFERENCES tags(tag_id) --pdDtype: Int32
);

CREATE TABLE tags(
    tag_id INT PRIMARY KEY, --pdDtype: Int32
    comment_id INT REFERENCES comments(comment_id), --pdDtype: Int32
    tag VARCHAR(50) NOT NULL --pdDtype: object
);