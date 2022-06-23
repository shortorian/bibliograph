'''
This file contains a python dictionary with all information necessary to
construct the node_types table and any associated tables for individual
node types in a bibliograph database. Each key in the dictionary is the
string-valued name of a node type. Each value is a dictionary with
information about that node type. The top-level dictionary has the form

{node_type:node_type_dictionary}

Keys in the top-level dictionary will become the values of the node_type
field in a row of the node_types table and they are also the names of
SQL tables containing metadata for nodes of that type if a metadata
table is created. Text in the "description" value of a node type
dictionary will be stored in the strings table at an index stored in the
dscrptn_string_id field of the node_types table.

All other keys in a node type dictionary will become column labels
in the SQL table for that node type. Values in the node_type
dictionaries must be list-like with two string values. The first value
is a SQL data type for the column and the second value is a pandas data
type for that column. Pandas converts "str" type to "object", but it
will accept either "str" or "object" for string-valued columns. If there
are any missing values in an integer column, pandas will convert that
column to float unless the type is "IntX", where X is 8, 16, 32, or 64
("Int" is case sensitive).

Bibliograph will always create three node types automatically even if
they are not defined in the node_types file. These are text_data,
entry_rules, and relation_rules.

For each element in the top-level dictionary of the form

"node_type": {
    "description":"description text",
    "first_column":[SQL_DTYPE_0, pandas_dtype_0],
    "second_column":[SQL_DTYPE_1, pandas_dtype_1],
    "third_column":[SQL_DTYPE_2, pandas_dtype_2]
}

bibliograph will create a row in the node_types table whose node_type
field will be the string value of the top-level key ("node_type" in the
example above). If a "description" key is present then bibliograph will
store its value in the strings table at an index stored in the
dscrptn_string_id field of the node_types table.

If there are any additional keys then bibliograph will create a SQL
table in the database with the following definition (in SQLite)

CREATE TABLE node_type(
    node_id INT PRIMARY KEY,
    node_type_id INT GENERATED ALWAYS AS (X) STORED,
    first_column SQL_DTYPE_0,
    second_column SQL_DTYPE_1,
    third_column SQL_DTYPE_2,
    FOREIGN KEY (node_id, node_type_id) REFERENCES nodes(node_id, node_type_id)
);

where X in the node_type_id line is the index of this node type in the
node_types table. In addition to the SQL table, bibliograph will create
a pandas DataFrame equivalent to the following expressions

df = pd.DataFrame(columns=['node_id',
                           'node_type_id',
                           'first_column',
                           'second_column',
                           'third_column'])
df = df.astype({'node_id':'Int32,
                'node_type_id':'Int32',
                'first_column':pandas_dtype_0,
                'second_column':pandas_dtype_1,
                'third_column':pandas_dtype_2})

NOTE: if this file contains anything other than a python dictionary,
bibliograph expects all that content to appear above the dictionary and
be separated from it by "\n###\n"
'''
###
{
    "works": {
        "description": "A work may be anything containing information "
                       "represented in the database. Intended to be "
                       "compatible with the FRBR class 'work'."
    },

    "actors": {
        "description": "An actor associated with something represented "
                       "in the database, for instance a person who "
                       "wrote an article or a laboratory where someone "
                       "worked. Intended to be compatible with the "
                       "CIDOC-CRM class 'actor'."
    },

    "dates": {
        "description": "A reference to some time span, for instance "
                       "'1969'. Intended to be compatible with the "
                       "CIDOC-CRM class 'Time Primitive'."
    },

    "identifiers": {
        "description": "An identifier is anything meant to uniquely "
                       "identify something else in a given context. "
                       "For instance a DOI or an accession number in "
                       "an archive. The type of identifier is defined "
                       "by the link type with another node in the "
                       "database. For instance if an identifier is "
                       "linked to another node and the link has node "
                       "type 'doi' then one can find the doi for that "
                       "node by getting the string representation of "
                       "the identifier."
    },

    "agreements": {
        "description": "An agreement is any contract or other agreement "
                       "between actors, for instance a grant supporting "
                       "a research project or a fellowship supporting a "
                       "person."
    },

    "quotes": {
        "description": "Text or other information copied from an "
                       "entity represented in the database."
    },

    "notes": {
        "description": "Annotations of any node in the database."
    },

    "text_data": {
        "description": "With two exceptions, all data read into a "
                       "bibliograph database is stored in the strings "
                       "table as csv-formatted text stored at the "
                       "full_string_id of a text_data node. The two "
                       "exceptions are for data representing "
                       "entry_rules nodes and relations_rules nodes, "
                       "as described below. Gauranteed columns:\n\n"
                       "comment_char: Optional. Single character to "
                       "separate comments from data.",
        "comment_char": ["CHARACTER(1)", "str"]
    },

    "entry_rules": {
        "description": "The string representation of an entry_rules "
                       "node is csv-formatted text defining the "
                       "labels and separators required to parse "
                       "individual entries in input data. Gauranteed "
                       "columns:\n\nitem_separator: Required. String "
                       "value of up to four characters which delimits "
                       "items within an entry.\n\ndefault prefix: "
                       "Optional. Prefix associated with any entries "
                       "that do not begin with a prefix defined in "
                       "the csv string associated with the "
                       "entry_rules. If null, entries without a "
                       "prefix will raise an error. Limit 12 "
                       "characters.\n\nspace_char: Optional. "
                       "Character that will be replaced by a space "
                       "unless escaped with a backslash.\n\n"
                       "na_values: Optional. Space-delimited list of "
                       "string values representing missing data. "
                       "Limit 19 characters including spaces.",
        "item_separator": ["VARCHAR(4)", "str"],
        "default_prefix": ["VARCHAR(12)", "str"],
        "space_char": ["CHAR(1)", "str"],
        "na_values": ["VARCHAR(19)", "str"]
    },

    "relations_rules": {
        "description": "The string representation of a "
                       "relations_rules node is csv-formatted text "
                       "defining relations between entries in "
                       "csv-formatted input data."
    }
}
