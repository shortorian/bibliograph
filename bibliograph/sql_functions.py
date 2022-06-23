from pathlib import Path
import pandas as pd
import sqlite3

# sql constraint keywords taken from documentation pages at
# https://sqlite.org/syntax/column-constraint.html
# https://www.postgresql.org/docs/9.4/ddl-constraints.html
# does not include keywords from other sql dialects
sql_constraint_keywords = ['check',
                           'collate',
                           'constraint',
                           'default',
                           'exclude',
                           'foreign',
                           'generated',
                           'not',
                           'primary',
                           'unique']

# sql data type keywords taken from sqlite affinity rules at
# https://sqlite.org/datatype3.html section 3.1
# may not handle keywords from other sql dialects
sql_int_keywords = ['int', 'unsigned']
sql_string_keywords = ['char']
sql_float_keywords = ['real', 'doub', 'floa', 'numeric', 'decimal']


def _fix_int_types(dtype_string):
    dtype_string = dtype_string.lower()

    if dtype_string.startswith('int'):
        if dtype_string.isalpha():
            return 'Int64'
        else:
            dtype_string = 'I' + dtype_string[1:]
            return dtype_string

    else:
        return dtype_string


def _read_table_declaration(text):
    if not text.strip().startswith('create table'):
        raise ValueError('not a sql CREATE TABLE expression')

    text = text.split('(', maxsplit=1)
    table_name = text[0].split()[-1]
    cols = map(str.strip, text[1].split('pddtype:'))
    cols = list(map(str.split, cols))

    if cols[0][0] not in sql_constraint_keywords:
        columns = [
            lbl[1] for lbl in cols[1:] if lbl[1] not in sql_constraint_keywords
        ]
        columns = [cols[0][0]] + columns

    else:
        columns = [
            lbl[1] for lbl in cols if lbl[1] not in sql_constraint_keywords
        ]

    types = [lbl[0] for lbl in cols[1:]]
    types = map(_fix_int_types, types)

    return (table_name, dict(zip(columns, types)))


def get_tables(db_path, table_names=None):

    conn_string = 'sqlite:///' + str(db_path.resolve(strict=True))

    if table_names is None:
        schema = pd.read_sql_table('sqlite_schema', conn_string)
        table_names = schema.loc[schema['type'] == 'table', 'name']

    elif isinstance(table_names, str):
        table_names = [table_names]

    return {
        name: pd.read_sql_table(name, conn_string)
        for name in table_names
    }


def get_sqlite_db_connection(db_path, overwrite=False):

    if db_path != ':memory:':

        db_path = Path(db_path)

        if overwrite:
            db_path.unlink(missing_ok=True)

        elif db_path.is_file():
            raise FileExistsError(
                'File {} exists. Use overwrite=True to delete an '
                'existing database.'.format(db_path)
            )

        db_path = str(db_path.resolve())

    db = sqlite3.connect(db_path)

    return db


def _create_sqlite_db(script_path, db_path, overwrite=False, encoding='utf8'):

    db = get_sqlite_db_connection(db_path, overwrite=overwrite)
    cursor = db.cursor()

    with script_path.open(encoding=encoding) as f:
        sql_script = f.read()

    cursor.executescript(sql_script)

    db.commit()

    if db_path == ':memory:':
        return db

    db.close()


def get_or_create_database_tables(
    db_fname,
    table_names=None,
    script=None,
    set_dtypes=True,
    overwrite=False,
    encoding='utf8'
):
    '''
    Create or access a database and return the contents of tables as
    pandas DataFrames.
    '''

    db_path = Path(db_fname)

    if script is not None:
        script_path = Path(script)
        _create_sqlite_db(script_path, db_path, overwrite, encoding=encoding)

    else:
        db_fname = str(db_path.resolve(strict=True))

    tables = get_tables(db_path, table_names)

    if set_dtypes is not None:

        if set_dtypes:
            types_dict = get_sql_cols_and_dtypes(script, encoding=encoding)
        else:
            types_dict = get_sql_cols_and_dtypes(set_dtypes, encoding=encoding)

        tables = {
            name: table.astype(types_dict[name])
            for name, table in tables.items()
        }

    return tables


def get_sql_cols_and_dtypes(script_fname, encoding='utf8'):

    with open(script_fname, encoding=encoding) as f:
        script = f.read().lower()

    tables = [
        t for t in script.split(';') if t.strip().startswith('create table')
    ]

    return dict(list(map(lambda x: _read_table_declaration(x), tables)))


def pandas_tables_from_sql_script(
    script_fname,
    table_names=None,
    set_dtypes=True,
    encoding='utf8'
):

    script_path = Path(script_fname)
    db = _create_sqlite_db(script_path, ':memory:', encoding=encoding)

    if table_names is None:
        schema = pd.read_sql('SELECT * FROM sqlite_schema', db)
        table_names = schema.loc[schema['type'] == 'table', 'name']
    elif isinstance(table_names, str):
        table_names = [table_names]

    tables = {
        name: pd.read_sql('SELECT * FROM {}'.format(name), db)
        for name in table_names
    }

    if set_dtypes:
        types_dict = get_sql_cols_and_dtypes(script_fname, encoding=encoding)
        tables = {
            name: table.astype(types_dict[name])
            for name, table in tables.items()
        }

    elif isinstance(set_dtypes, str):
        types_dict = get_sql_cols_and_dtypes(set_dtypes, encoding=encoding)
        tables = {
            name: table.astype(types_dict[name])
            for name, table in tables.items()
        }

    elif not set_dtypes:
        pass

    else:
        raise ValueError('set_dtypes must be boolean or string')

    db.close()

    return tables


def make_sql_node_table_definition(name, sql_dtypes, node_type_ids):
    '''
    sql_dtypes is a dictionary of the form
    "node_table_name": {
        "first_column":"SQL_DTYPE_0",
        "second_column":"SQL_DTYPE_1",
        "third_column":"SQL_DTYPE_2"
    }
    '''
    dtypes = sql_dtypes[name]
    node_type_id = node_type_ids[name]

    preamble = (
        'CREATE TABLE {}(\n\tnode_id INT PRIMARY KEY,\n\tnode_type_id '
        'INT GENERATED ALWAYS AS ({}) STORED,\n'
        .format(name, node_type_id)
    )

    body = '\n'.join(['\t{} {},'.format(k, v) for k, v in dtypes])

    conclusion = (
        '\tFOREIGN KEY (node_id, node_type_id) REFERENCES '
        'nodes(node_id, node_type_id)\n);'
    )

    return preamble + body + conclusion


#####################################################
# EXAMPLE CODE TO CALL make_sql_node_table_definition
#####################################################
'''
if db_fname is not None:

    sql_dtypes = {
        name: {k:v[0] for k,v in cols.items()} for name, cols in table_data
    }
    node_type_ids = {table_names[id]: id for id in node_type_ids}

    make_sql_def = lambda x: bg.make_sql_node_table_definition(
        x, sql_dtypes, node_type_ids
    )

    sql_table_definitions = [make_sql_def(name) for name in table_names]

    sql_script = '\n'.join(sql_table_definitions)

    if write_script is not False:
        script_path = str(Path(write_script).resolve())
        with open(script_path, 'w', encoding='utf8') as f:
            f.write(sql_script)

    db = bg.get_sqlite_db_connection(db_fname)
    cursor = db.cursor()

    cursor.executescript(sql_script)

    db.commit()
    db.close()
'''
