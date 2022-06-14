from pkg_resources import resource_filename as _rsrc_fname
from pandas import read_csv as _read_csv
from pathlib import Path as _Path

_default_node_types_fname = _Path(_rsrc_fname(__name__,
                                              'resources/node_types.py'))
if _default_node_types_fname.is_file():
    _default_node_types_fname = str(_default_node_types_fname.resolve())
else:
    raise FileNotFoundError('File resources/node_types.py not found. This '
                            'file is required to define the default node '
                            'type names and their tables in the database.')

_default_link_types_fname = _Path(_rsrc_fname(__name__,
                                              'resources/link_types.py'))
if _default_link_types_fname.is_file():
    _default_link_types_fname = str(_default_link_types_fname.resolve())
else:
    raise FileNotFoundError('File resources/link_types.py not found. This '
                            'file is required to define the default edge '
                            'type names.')

_default_tags_fname = _Path(_rsrc_fname(__name__, 'resources/node_tags.py'))
if _default_tags_fname.is_file():
    _default_tags_fname = str(_default_tags_fname.resolve())
else:
    _default_tags_fname = False

_default_label_map_fname = _Path(_rsrc_fname(__name__, 'resources/label_map.csv'))
if _default_label_map_fname.is_file():
    _default_label_map_fname = str(_default_label_map_fname.resolve())
else:
    raise FileNotFoundError('File resources/label_map.csv not found. This '
                            'file is required to define the default mapping '
                            'between data labels, node types, and edge types.')

_default_label_map = _read_csv(_default_label_map_fname,
                               header=0,
                               skipinitialspace=True)

from bibliograph.sql_functions import *
from bibliograph.util import *

_default_sql_schema_fname = _Path(_rsrc_fname(__name__,
                                              'resources/schema_script.sql'))
if _default_sql_schema_fname.is_file():
    _default_sql_schema_fname = str(_default_sql_schema_fname.resolve())
else:
    raise FileNotFoundError('File resources/schema_script.sql not found. '
                            'This file is required to define the default '
                            'database schema.')

_default_schema = pandas_tables_from_sql_script(_default_sql_schema_fname)

from bibliograph.core import *
from bibliograph.data_file_input import *
from bibliograph.textnet import *