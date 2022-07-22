import bibliograph as bg
import pandas as pd
import shorthand as shnd
import inspect
from bibtexparser import dumps as _dump_bibtex_string
from bibtexparser.bibdatabase import BibDatabase as _bibtex_db
from bibtexparser.bparser import BibTexParser as _bibtexparser
from datetime import datetime
from io import StringIO
from pathlib import Path


def _insert_alias_assertions(
    tn,
    parsed,
    aliases_dict,
    aliases_case_sensitive
):

    time_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # convert aliases_dict into dictionary of dataframes
    aliases = {
        k: _read_file_from_path_or_read_str_as_buffer(
            v, pd.read_csv, encoding='utf8', skipinitialspace=True
        )
        for k, v in aliases_dict.items()
        if k in tn.node_types['node_type'].array
    }

    # Get relevant aliases out of each set of aliases
    aliases = {
        k: _select_aliases(v, parsed, k, aliases_case_sensitive)
        for k, v in aliases.items()
    }

    # get integer IDs for the relevant node types
    node_type_ids = {
        k: tn.id_lookup('node_types', k, column_label='node_type')
        for k in aliases.keys()
    }

    # add any new strings we found to the textnet strings frame
    new_strings = [
        pd.DataFrame({
            'string': v.stack().array,
            'node_type_id': node_type_ids[k],
            'date_inserted': time_string,
            'date_modified': pd.NA
        })
        for k, v in aliases.items()
    ]
    new_strings = pd.concat(new_strings).drop_duplicates()

    # Add a node type for alias reference strings and add the alias
    # reference strings to the new strings
    tn.insert_node_type('alias_reference')
    alias_ref_node_id = tn.id_lookup('node_types', 'alias_reference')
    alias_ref_strings = pd.DataFrame({
        'string': aliases_dict.values(),
        'node_type_id': alias_ref_node_id,
        'date_inserted': time_string,
        'date_modified': pd.NA
    })
    new_strings = pd.concat([new_strings, alias_ref_strings])

    # drop any strings already present in tn.strings
    new_strings = new_strings.loc[
        ~new_strings['string'].isin(tn.strings['string'])
    ]

    # Insert the new strings in tn.strings
    new_strings = shnd.util.normalize_types(new_strings, tn.strings)
    tn.strings = pd.concat([tn.strings, new_strings])

    # make maps between string values and integer IDs relevant to each
    # set of aliases
    string_maps = {
        k: bg.util.get_string_values(tn, node_type_subset=v)
        for k, v in node_type_ids.items()
    }
    string_maps = {
        k: pd.Series(v.index, index=v.array)
        for k, v in string_maps.items()
    }

    # convert alias string values to integer string IDs
    aliases = {
        k: v.apply(lambda x: x.map(string_maps[k]))
        for k, v in aliases.items()
    }

    # Start building an assertions frame out of string values
    new_assertions = {
        k: pd.DataFrame({
            'src_string_id': aliases[k]['key'],
            'tgt_string_id': aliases[k]['value'],
            'ref_string_id': tn.id_lookup('strings', v)
        })
        for k, v in aliases_dict.items()
    }

    new_assertions = pd.concat(new_assertions.values(), ignore_index=True)

    # make a string value representing the current python function
    frame = inspect.currentframe()
    current_module = inspect.getframeinfo(frame).filename
    current_module = Path(current_module).stem.split('.')[0]
    current_function = inspect.getframeinfo(frame).function
    inp_string = '.'.join(['bibliograph', current_module, current_function])

    # Insert a strings row for the input string
    new_string = {
        'string': inp_string,
        'node_type_id': tn.id_lookup('node_types', 'python_function'),
        'date_inserted': time_string,
        'date_modified': pd.NA
    }
    new_string = shnd.util.normalize_types(new_string, tn.strings)
    tn.strings = pd.concat([tn.strings, new_string])

    # Add a link type for aliases if it doesn't already exist
    tn.insert_link_type('alias')
    alias_link_type_id = tn.id_lookup('link_types', 'alias')

    # make a dataframe with the remaining data for the assertions
    # frame and then concat it with the partial assertions frame
    assertion_metadata = pd.DataFrame(
        {
            'inp_string_id': tn.strings.index[-1],
            'link_type_id': alias_link_type_id,
            'date_inserted': time_string,
            'date_modified': pd.NA
        },
        index=new_assertions.index
    )
    new_assertions = pd.concat(
        [new_assertions, assertion_metadata],
        axis='columns'
    )

    # put the new assertions columns in the right order and then
    # add them to the textnet assertions
    new_assertions = new_assertions[tn.assertions.columns]
    new_assertions = shnd.util.normalize_types(
        new_assertions,
        tn.assertions
    )
    tn.assertions = pd.concat([tn.assertions, new_assertions])


def _make_syntax_metadata_table(textnet, parsed_shnd, entry_or_link):

    if entry_or_link not in ['entry', 'link']:
        raise ValueError('entry_or_link must be either "entry" or "link')

    try:
        syntax_case_sensitive = parsed_shnd.__getattribute__(
            entry_or_link + '_syntax_case_sensitive'
        )

    except AttributeError:
        syntax_case_sensitive = parsed_shnd.syntax_case_sensitive

    syntax_node_type = 'shorthand_{}_syntax'.format(entry_or_link)

    metadata = {
        'case_sensitive': syntax_case_sensitive,
        'allow_redundant_items': parsed_shnd.allow_redundant_items
    }

    textnet.insert_metadata_table(syntax_node_type, metadata)


def _read_file_from_path_or_read_str_as_buffer(
    filepath_or_string_data,
    reader=None,
    **kwargs
):

    try:
        return reader(filepath_or_string_data, **kwargs)

    except FileNotFoundError:
        return reader(StringIO(filepath_or_string_data), **kwargs)


def _select_aliases(
    aliases,
    parsed,
    node_type,
    case_sensitive=True,
    **kwargs
):

    if parsed.strings.empty or aliases.empty:
        return

    aliases.columns = ['key', 'value']
    aliases.loc[:, 'key'] = aliases['key'].ffill()

    aliases = aliases.query('key != value')
    aliases = aliases.drop_duplicates()

    if aliases['key'].isin(aliases['value']).any():

        keys_and_values = pd.concat([
            aliases['key'].drop_duplicates(),
            aliases['value'].drop_duplicates()
        ])
        keys_and_values = keys_and_values.loc[keys_and_values.duplicated()]

        raise ValueError(
            'The following aliases appear as both keys and values: {}'
            .format(set(keys_and_values))
        )

    if not case_sensitive:

        _aliases = aliases.copy()
        aliases.loc[:, 'key'] = aliases['key'].str.casefold().array

        aliases = aliases.query('key != value')
        aliases = aliases.drop_duplicates()

    collisions = aliases['value'].duplicated()
    if collisions.any():
        raise ValueError(
            'The following aliases are mapped to multiple values when '
            'case_sensitive is {}: {}'
            .format(case_sensitive, set(aliases.loc[collisions, 'value']))
        )

    strings = bg.util.get_string_values(
        parsed,
        casefold=(not case_sensitive),
        node_type_subset=node_type
    )

    aliased_strings = aliases.stack()
    alias_in_strings = aliased_strings.isin(strings)

    if not alias_in_strings.any():
        return pd.DataFrame(columns=aliases.columns)

    else:
        keys = aliased_strings.loc[alias_in_strings]
        keys = aliases.loc[
            keys.index.get_level_values(0).drop_duplicates(),
            'key'
        ]

        if not case_sensitive:
            return_index = aliases.loc[aliases['key'].isin(keys)].index
            return _aliases.loc[return_index]

        else:
            return aliases.loc[aliases['key'].isin(keys)]


def build_textnet_assertions(
    parsed,
    input_source_string,
    input_source_node_type
):

    # get a node_type_id for input source
    try:
        input_source_node_type_id = parsed.id_lookup(
            'node_types',
            input_source_node_type
        )

    except KeyError:

        new_node_type = shnd.util.normalize_types(
            pd.Series([input_source_node_type]),
            parsed.node_types
        )
        parsed.node_types = pd.concat([parsed.node_types, new_node_type])

        input_source_node_type_id = parsed.node_types.index[-1]

    # Insert a strings row for the input filename
    new_string = {
        'string': input_source_string,
        'node_type_id': input_source_node_type_id
    }
    new_string = shnd.util.normalize_types(new_string, parsed.strings)
    parsed.strings = pd.concat([parsed.strings, new_string])

    # create a TextNet and cache a string for the insertion dates
    tn = bg.TextNet()

    time_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # create a column for the assertion inp_string_id
    inp_string_id = pd.DataFrame(
        new_string.index[0],
        columns=['inp_string_id'],
        index=parsed.links.index
    )

    # create the assertions table
    tn.assertions = pd.concat([inp_string_id, parsed.links], axis='columns')
    tn.assertions = tn.assertions.drop('list_position', axis='columns')
    tn.assertions.loc[:, 'date_inserted'] = time_string
    tn.assertions.loc[:, 'date_modified'] = pd.NA
    tn.reset_assertions_dtypes()

    # create the strings table
    tn.strings = parsed.strings.copy()
    tn.strings.loc[:, 'date_inserted'] = time_string
    tn.strings.loc[:, 'date_modified'] = pd.NA

    # create the node_types table
    tn.node_types = pd.DataFrame(
        {'node_type': parsed.node_types.array, 'description': pd.NA}
    )
    tn.reset_node_types_dtypes()

    # create the link_types table
    tn.link_types = pd.DataFrame(
        {'link_type': parsed.link_types.array, 'description': pd.NA}
    )
    tn.reset_link_types_dtypes()

    # create the assertion_tags table
    tn.assertion_tags = parsed.link_tags.rename(
        columns={'link_id': 'assertion_id'}
    )
    tn.reset_assertion_tags_dtypes()

    return tn


def complete_textnet_from_assertions(tn, parsed):

    time_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # If there are alias links in the assertions table, map them to the
    # same node IDs
    try:
        alias_link_type_ID = tn.id_lookup('link_types', 'alias')

        aliases = tn.assertions.query('link_type_id == @alias_link_type_ID')

        aliases = aliases[['src_string_id', 'tgt_string_id']]

        src_node_types = aliases['src_string_id'].map(
            tn.strings['node_type_id']
        )
        tgt_node_types = aliases['tgt_string_id'].map(
            tn.strings['node_type_id']
        )

        if (src_node_types != tgt_node_types).any():
            raise ValueError(
                'There are alias assertions between strings with '
                'different node types'
            )

        aliased_node_id_map = aliases['src_string_id'].unique()
        aliased_node_id_map = pd.Series(
            range(len(aliased_node_id_map)),
            index=aliased_node_id_map
        )

        aliases.index = pd.Index(
            aliases['src_string_id'].map(aliased_node_id_map)
        )

        aliased_node_id_map = aliases.stack().drop_duplicates()
        aliased_node_id_map.index = pd.Index(
            aliased_node_id_map.index.get_level_values(0)
        )

        string_subset = tn.strings.loc[aliased_node_id_map]

        name_strings = pd.DataFrame({
            'node_id': aliased_node_id_map.index,
            'string': string_subset['string'],
            'string_id': string_subset.index,
            'node_type_id': string_subset['node_type_id']
        })
        name_strings = name_strings.loc[
            name_strings['string'].str.len().sort_values().index
        ]
        name_strings = name_strings.drop_duplicates(
            keep='last', subset='node_id'
        )
        name_strings = name_strings.sort_values(by='node_id')

        abbr_strings = pd.DataFrame({
            'node_id': aliased_node_id_map.index,
            'string': string_subset['string'],
            'string_id': string_subset.index
        })
        abbr_strings = abbr_strings.loc[
            abbr_strings['string'].str.len().sort_values().index
        ]
        abbr_strings = abbr_strings.drop_duplicates(subset='node_id')
        abbr_strings = abbr_strings.sort_values(by='node_id')

        tn.nodes = pd.DataFrame({
            'node_type_id': name_strings['node_type_id'].array,
            'name_string_id': name_strings['string_id'].array,
            'abbr_string_id': abbr_strings['string_id'].array,
            'date_inserted': time_string,
            'date_modified': pd.NA
        })

        tn.reset_nodes_dtypes()

        idx_of_strings_with_no_aliases = tn.strings.index.difference(
            aliased_node_id_map
        )
        strings_with_no_aliases = tn.strings.loc[
            idx_of_strings_with_no_aliases
        ]

        new_nodes = pd.DataFrame({
            'node_type_id': strings_with_no_aliases['node_type_id'],
            'name_string_id': strings_with_no_aliases.index,
            'abbr_string_id': strings_with_no_aliases.index,
            'date_inserted': time_string,
            'date_modified': pd.NA
        })
        new_nodes = shnd.util.normalize_types(new_nodes, tn.nodes)
        tn.nodes = pd.concat([tn.nodes, new_nodes])

        tn.strings.loc[aliased_node_id_map, 'node_id'] = (
            aliased_node_id_map.index
        )
        tn.strings.loc[idx_of_strings_with_no_aliases, 'node_id'] = (
            new_nodes.index
        )

    except KeyError:

        tn.nodes = pd.DataFrame(
            {
                'node_type_id': tn.strings['node_type_id'],
                'name_string_id': tn.strings.index,
                'abbr_string_id': tn.strings.index,
                'date_inserted': time_string,
                'date_modified': pd.NA
            }
        )

        tn.strings['node_id'] = tn.nodes.index

    tn.reset_strings_dtypes()

    def map_assert_str_to_nodes(label):
        return tn.assertions[label].map(tn.strings['node_id'])

    tn.edges = pd.DataFrame({
        'src_node_id': map_assert_str_to_nodes('src_string_id'),
        'tgt_node_id': map_assert_str_to_nodes('tgt_string_id'),
        'ref_node_id': map_assert_str_to_nodes('ref_string_id'),
        'link_type_id': tn.assertions['link_type_id'],
        'date_inserted': time_string,
        'date_modified': pd.NA
    })
    tn.edges = tn.edges.drop_duplicates().reset_index(drop=True)

    tn.reset_edges_dtypes()

    tn.edge_tags = pd.DataFrame(columns=['edge_id', 'tag_string_id'])

    if 'shorthand_text' in parsed.node_types.array:
        text_node_type = 'shorthand_text'
    elif 'items_text' in parsed.node_types.array:
        text_node_type = 'items_text'
    else:
        raise ValueError(
            'Unrecognized input full text type.\nNone of '
            '["shorthand_text", "items_text"] in parsed shorthand node '
            'types.\n'
        )

    text_metadata = {
        'space_char': parsed.space_char,
        'na_string_values': parsed.na_string_values,
        'na_node_type': parsed.na_node_type,
        'item_separator': parsed.item_separator
    }

    tn.insert_metadata_table(text_node_type, text_metadata)

    _make_syntax_metadata_table(tn, parsed, 'entry')

    if 'shorthand_link_syntax' in parsed.node_types.array:
        _make_syntax_metadata_table(tn, parsed, 'link')

    return tn


def textnet_from_parsed_shorthand(
    parsed,
    input_source_string,
    input_source_node_type,
    aliases_dict=None,
    aliases_case_sensitive=True
):

    tn = build_textnet_assertions(
        parsed,
        input_source_string,
        input_source_node_type
    )

    if aliases_dict is not None:
        _insert_alias_assertions(
            tn,
            parsed,
            aliases_dict,
            aliases_case_sensitive
        )

    return complete_textnet_from_assertions(tn, parsed)


def slurp_bibtex(
    bibtex_fname,
    entry_syntax_fname,
    syntax_case_sensitive=True,
    allow_redundant_items=False,
    aliases_dict=None,
    aliases_case_sensitive=True,
    **kwargs
):

    # initialize bibtex and shorthand parsers
    bibtex_parser = _bibtexparser(common_strings=True)
    s = shnd.Shorthand(
        entry_syntax=entry_syntax_fname,
        syntax_case_sensitive=syntax_case_sensitive,
        allow_redundant_items=allow_redundant_items
    )

    # if a function to write entry strings was not provided, use
    # functions from bibtexparser
    if 'entry_writer' in kwargs.keys():

        entry_writer = kwargs['entry_writer']

    else:

        def btwriter(entry_series):
            db = _bibtex_db()
            db.entries = [dict(entry_series.dropna().map(str))]
            return _dump_bibtex_string(db)

        entry_writer = btwriter

    # drop 'entry_writer' from the input args so we can pass it directly
    # to Shorthand.parse_items
    kwargs = {k: v for k, v in kwargs.items() if k != 'entry_writer'}

    # parse input
    with open(bibtex_fname, encoding='utf8') as f:
        parsed = s.parse_items(
            pd.DataFrame(bibtex_parser.parse_file(f).entries),
            entry_writer=entry_writer,
            **kwargs
        )

    tn = textnet_from_parsed_shorthand(
        parsed,
        bibtex_fname,
        'file_name',
        aliases_dict,
        aliases_case_sensitive
    )

    return tn


def slurp_shorthand(
    shorthand_fname,
    entry_syntax_fname,
    link_syntax_fname=None,
    syntax_case_sensitive=True,
    allow_redundant_items=False,
    aliases_dict=None,
    aliases_case_sensitive=True,
    **kwargs
):

    s = shnd.Shorthand(
        entry_syntax=entry_syntax_fname,
        link_syntax=link_syntax_fname,
        syntax_case_sensitive=syntax_case_sensitive,
        allow_redundant_items=allow_redundant_items
    )

    parsed = s.parse_text(shorthand_fname, **kwargs)

    tn = textnet_from_parsed_shorthand(
        parsed,
        shorthand_fname,
        'file_name',
        aliases_dict,
        aliases_case_sensitive
    )

    return tn
