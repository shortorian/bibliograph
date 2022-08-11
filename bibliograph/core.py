import bibliograph as bg
import pandas as pd
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
    aliases_case_sensitive,
    input_source_string_id
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
    aliases_dict = {k: v.to_csv(index=False) for k, v in aliases.items()}

    # Get relevant aliases out of each set of aliases
    aliases = {
        k: _select_aliases(v, parsed, k, aliases_case_sensitive)
        for k, v in aliases.items()
    }
    if not aliases_case_sensitive:
        low_keys = {
            k: v.apply(lambda x: x.str.casefold() if x.name == 'key' else x)
            for k, v in aliases.items()
        }
        low_values = {
            k: v.apply(lambda x: x.str.casefold() if x.name == 'value' else x)
            for k, v in aliases.items()
        }
        aliases = {
            k: pd.concat([v, low_keys[k], low_values[k]]).drop_duplicates()
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
    alias_ref_node_id = tn.insert_node_type('alias_reference')
    alias_ref_strings = pd.DataFrame({
        'string': aliases_dict.values(),
        'node_type_id': alias_ref_node_id,
        'date_inserted': time_string,
        'date_modified': pd.NA
    })
    new_strings = pd.concat([new_strings, alias_ref_strings])

    # drop any strings already present in tn.strings
    new_strings = new_strings.loc[
        ~new_strings['string'].isin(tn.strings['string']),
        :
    ]

    # Insert the new strings in tn.strings
    new_strings = bg.util.normalize_types(new_strings, tn.strings)
    tn.strings = pd.concat([tn.strings, new_strings])

    # make maps between string values and integer IDs relevant to each
    # set of aliases
    string_maps = {
        k: bg.util.get_string_values(tn, node_type_subset=v)
        for k, v in node_type_ids.items()
    }

    if not aliases_case_sensitive:

        low_maps = {k: v.str.casefold() for k, v in string_maps.items()}
        case_alias = {
            k: (v != low_maps[k]) for k, v in string_maps.items()
        }
        new_aliases = {
            k: pd.concat(
                [v[case_alias[k]], low_maps[k][case_alias[k]]],
                axis='columns',
                ignore_index=True
            )
            for k, v in string_maps.items()
        }
        new_aliases = {
            k: v.rename(columns={0: 'key', 1: 'value'})
            for k, v in new_aliases.items()
        }

        aliases = {
            k: pd.concat([v, new_aliases[k]]) for k, v in aliases.items()
        }

    string_maps = {
        k: pd.Series(v.index, index=v.array)
        for k, v in string_maps.items()
    }

    # convert alias keys to integer string IDs
    def merge_keys_and_values(aliases, string_map):
        k = aliases.merge(
            string_map.rename('k'),
            left_on='key',
            right_index=True
        )
        v = aliases.merge(
            string_map.rename('v'),
            left_on='value',
            right_index=True
        )
        output = k[['k', 'key']].merge(v[['v', 'key']])[['k', 'v']]
        output = output.rename(columns={'k': 'key', 'v': 'value'})
        return output.drop_duplicates()

    aliases = {
        k: merge_keys_and_values(v, string_maps[k]) for k, v in aliases.items()
    }

    # Start building an assertions frame out of string values
    new_assertions = {
        k: pd.DataFrame({
            'src_string_id': aliases[k]['key'].array,
            'tgt_string_id': aliases[k]['value'].array,
            'ref_string_id': tn.id_lookup('strings', v)
        })
        for k, v in aliases_dict.items()
    }

    new_assertions = pd.concat(new_assertions.values(), ignore_index=True)

    # Add a link type for aliases if it doesn't already exist
    alias_link_type_id = tn.insert_link_type('alias')

    # make a dataframe with the remaining data for the assertions
    # frame and then concat it with the partial assertions frame
    assertion_metadata = pd.DataFrame(
        {
            'inp_string_id': input_source_string_id,
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
    new_assertions = bg.util.normalize_types(
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
        return pd.DataFrame(columns=aliases.columns)

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

    value_collisions = aliases['value'].duplicated()
    key_collisions = aliases['key'].duplicated()
    if value_collisions.any():

        if not key_collisions.any():
            aliases.columns = ['value', 'key']

        else:
            raise ValueError(
                'The following aliases are mapped to multiple values when '
                'case_sensitive is {}: {}'
                .format(
                    case_sensitive,
                    set(aliases.loc[value_collisions, 'value'])
                )
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


def _map_node_ids_for_single_link_constraint(
    constraint_row,
    tn,
    entry_syntax,
    entry_syntax_string_id
):

    node_type = constraint_row[0]
    link_types = constraint_row[1]

    # cache an empty return value
    empty = pd.DataFrame(columns=['src_node_id', 'tgt_node_id'])

    node_type_syntax = entry_syntax[node_type]
    link_types = pd.Series(map(str.strip, link_types.split()))

    if not link_types.isin(node_type_syntax['item_link_type']).all():
        raise ValueError(
            'Entry syntax at string ID {} does not define an '
            'entry of node type "{}" with item links of type(s) {}.'
            .format(entry_syntax_string_id, node_type, list(link_types))
        )

    # Get integer ID values for the link types mentioned in this
    # constraint
    link_type_ids = pd.Series(
        [tn.id_lookup('link_types', lt) for lt in link_types]
    )

    # Candidates for assertions that meet this constraint must have
    # the link types listed in the constraint
    candidate_assertions = tn.assertions.loc[
        tn.assertions['link_type_id'].isin(link_type_ids),
        :
    ]

    # Get node IDs and link types of candidate assertions
    candidate_assertions = pd.concat(
        [
            candidate_assertions['src_string_id'].map(tn.strings['node_id']),
            candidate_assertions['tgt_string_id'].map(tn.strings['node_id']),
            candidate_assertions['link_type_id']
        ],
        axis='columns'
    )
    candidate_assertions = candidate_assertions.rename(columns={
        'src_string_id': 'src_node_id',
        'tgt_string_id': 'tgt_node_id'
    })

    # Candidates can't have source or target nodes of null type
    null_node_ids = tn.get_nodes_with_null_types().index
    src_is_not_null = ~candidate_assertions['src_node_id'].isin(null_node_ids)
    tgt_is_not_null = ~candidate_assertions['tgt_node_id'].isin(null_node_ids)
    candidate_assertions = candidate_assertions.loc[
        src_is_not_null & tgt_is_not_null,
        :
    ]

    # Nodes can only be targets relevant to the constraint if they
    # are targets of at least two different links in the pool of
    # candidate assertions
    possible_tgt_nodes = candidate_assertions['tgt_node_id'].value_counts()
    possible_tgt_nodes = possible_tgt_nodes.loc[possible_tgt_nodes > 1]
    candidate_assertions = candidate_assertions.query(
        'tgt_node_id.isin(@possible_tgt_nodes.index)'
    )

    if candidate_assertions.empty:
        return empty

    # Nodes can only be sources that meet the constraint if they
    # are sources for at least as many candidate assertions as there
    # are link types mentioned in the constraint. This step is done
    # to reduce the size of the series in the groupby operation
    # below because (I think?) counting values in a large series is
    # cheaper than doing Series.groupby().apply()
    possible_src_nodes = candidate_assertions['src_node_id'].value_counts()
    possible_src_nodes = possible_src_nodes.loc[
        possible_src_nodes >= len(link_type_ids)
    ]
    candidate_assertions = candidate_assertions.query(
        'src_node_id.isin(@possible_src_nodes.index)'
    )

    if candidate_assertions.empty:
        return empty

    # Nodes can only be sources that meet the constraint if they
    # they are sources for at least one candidate assertion of each
    # type listed in the constraint
    possible_src_nodes = candidate_assertions.groupby(by='src_node_id')
    possible_src_nodes = possible_src_nodes.apply(
        lambda x: x.name
        if link_type_ids.isin(x['link_type_id']).all()
        else pd.NA
    )
    possible_src_nodes = possible_src_nodes.dropna()
    candidate_assertions = candidate_assertions.query(
        'src_node_id.isin(@possible_src_nodes.index)'
    )

    if candidate_assertions.empty:
        return empty

    candidate_assertions = candidate_assertions.groupby(
        by=['link_type_id', 'tgt_node_id']
    )
    candidate_assertions = candidate_assertions.apply(
        lambda x: tuple(x['src_node_id']) if len(x) > 1 else ()
    )
    candidate_assertions = candidate_assertions.dropna()

    if candidate_assertions.empty:
        return empty

    candidate_assertions = candidate_assertions.loc[
        candidate_assertions != ()
    ]

    if candidate_assertions.empty:
        return empty

    candidate_assertions = candidate_assertions.explode()
    candidate_assertions = candidate_assertions.reset_index(
        name='src_node_id'
    )
    candidate_assertions = candidate_assertions.groupby(by='src_node_id')
    candidate_assertions = candidate_assertions.apply(
        lambda x:
        tuple(x['tgt_node_id'])
        if link_type_ids.isin(x['link_type_id']).all()
        else pd.NA
    )

    node_id_map = candidate_assertions.dropna().drop_duplicates()

    if candidate_assertions.empty:
        return empty

    node_id_map = pd.Series(node_id_map.index, index=node_id_map.array)

    return candidate_assertions.map(node_id_map).reset_index()


def _get_strings_of_extreme_length(strings_frame, extremum_type):

    if extremum_type == 'shortest':
        keep = 'first'
    elif extremum_type == 'longest':
        keep = 'last'
    else:
        raise ValueError('Cannot parse extremum type')

    node_id_subset = strings_frame['node_id'].value_counts()
    node_id_subset = node_id_subset.loc[node_id_subset > 1].index
    string_subset = strings_frame.query('node_id.isin(@node_id_subset)')

    extreme_strings = string_subset.loc[
        string_subset['string'].str.len().sort_values().index
    ]

    return extreme_strings.drop_duplicates(keep=keep, subset='node_id')


def get_node_id_map_from_link_constraints(
    tn,
    link_constraints_string_id,
    entry_syntax_string_id
):

    # The constraints are the full text of a CSV file stored in the
    # strings frame, so they need to be read into a dataframe
    constraints = pd.read_csv(
        StringIO(tn.strings.loc[link_constraints_string_id, 'string']),
        encoding='utf8'
    )

    # Read the entry syntax into a dataframe and make a dictionary for
    # the definition of each node type mentioned in the constraints file
    entry_syntax_node_id = tn.strings.loc[entry_syntax_string_id, 'node_id']
    syntax_metadata = tn.shorthand_entry_syntax.query(
        'node_id == @entry_syntax_node_id'
    )
    entry_syntax = bg.syntax_parsing.validate_entry_syntax(
        tn.strings.loc[entry_syntax_string_id, 'string'],
        case_sensitive=syntax_metadata['case_sensitive'].squeeze()
    )
    entry_syntax = {
        node_type: entry_syntax.query('entry_node_type == @node_type')
        for node_type in entry_syntax['entry_node_type'].unique()
    }

    node_id_map = constraints.apply(
        lambda x: _map_node_ids_for_single_link_constraint(
            x,
            tn,
            entry_syntax,
            entry_syntax_string_id
        ),
        axis='columns'
    )

    node_id_map = pd.concat(tuple(node_id_map))
    node_id_map = node_id_map.rename(columns={0: 'tgt_node_id'})
    node_id_map = node_id_map.drop_duplicates()
    node_id_map = node_id_map.query('src_node_id != tgt_node_id')

    return pd.Series(
        node_id_map['tgt_node_id'].array,
        index=node_id_map['src_node_id'].array
    )


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

        new_node_type = bg.util.normalize_types(
            pd.Series([input_source_node_type]),
            parsed.node_types
        )
        parsed.node_types = pd.concat([parsed.node_types, new_node_type])

        input_source_node_type_id = parsed.node_types.index[-1]

    # Insert a strings row for the input string
    new_string = {
        'string': input_source_string,
        'node_type_id': input_source_node_type_id
    }
    new_string = bg.util.normalize_types(new_string, parsed.strings)
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
        {
            'node_type': parsed.node_types.array,
            'description': pd.NA,
            'null_type': False,
            'has_metadata': False
        }
    )
    na_node_type_id = tn.id_lookup('node_types', parsed.na_node_type)
    tn.node_types.loc[na_node_type_id, 'null_type'] = True
    tn.reset_node_types_dtypes()

    # create the link_types table
    tn.link_types = pd.DataFrame(
        {
            'link_type': parsed.link_types.array,
            'description': pd.NA,
            'null_type': False
        }
    )
    tn.reset_link_types_dtypes()

    # create the assertion_tags table
    tn.assertion_tags = parsed.link_tags.rename(
        columns={'link_id': 'assertion_id'}
    )
    tn.reset_assertion_tags_dtypes()

    full_text_node_types = pd.Series(['shorthand_text', 'items_text'])
    full_text_types_present = full_text_node_types.loc[
        full_text_node_types.isin(parsed.node_types)
    ]

    if len(full_text_types_present) > 1:
        raise ValueError(
            'Found multiple full text node types. Can only process one '
            'input text at a time'
        )
    elif full_text_types_present.empty:
        raise ValueError(
            'Could not find valid full text type in '
            'ParsedShorthand.node_types. Must be one of {}'
            .format(full_text_node_types)
        )

    text_node_type = full_text_types_present.squeeze()

    text_metadata = {
        'space_char': parsed.space_char,
        'na_string_values': parsed.na_string_values,
        'na_node_type': parsed.na_node_type,
        'item_separator': parsed.item_separator,
        'default_entry_prefix': parsed.default_entry_prefix
    }

    tn.insert_metadata_table(text_node_type, text_metadata)

    _make_syntax_metadata_table(tn, parsed, 'entry')

    if 'shorthand_link_syntax' in parsed.node_types.array:
        _make_syntax_metadata_table(tn, parsed, 'link')

    return tn


def complete_textnet_from_assertions(
    tn,
    parsed,
    aliases_case_sensitive,
    link_constraints_string_id,
    links_excluded_from_edges
):

    time_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # If there are alias links in the assertions table, map them to the
    # same node IDs
    if (tn.link_types['link_type'] == 'alias').any():

        alias_link_type_ID = tn.id_lookup('link_types', 'alias')

        aliases = tn.assertions.query('link_type_id == @alias_link_type_ID')
        aliases = aliases[['src_string_id', 'tgt_string_id']]

        # convert aliases to a one-to-many map
        aliases = bg.util.make_bidirectional_map_one_to_many(aliases)

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

        if aliases_case_sensitive:

            node_ids = aliases['src_string_id'].unique()
            aliased_node_id_map = pd.Series(
                range(len(node_ids)),
                index=node_ids
            )

        else:

            aliased_node_id_map = aliases['src_string_id'].map(
                tn.strings['string']
            )
            aliased_node_id_map = aliased_node_id_map.str.casefold()

            node_ids = aliased_node_id_map.unique()
            node_ids = pd.Series(range(len(node_ids)), index=node_ids)

            aliased_node_id_map = aliased_node_id_map.map(node_ids)

            aliased_node_id_map = pd.concat(
                [
                    aliases['src_string_id'].rename('src_string_id'),
                    aliased_node_id_map.rename('node_id')
                ],
                axis='columns')
            aliased_node_id_map = aliased_node_id_map.drop_duplicates()
            aliased_node_id_map = pd.Series(
                aliased_node_id_map['node_id'].array,
                index=aliased_node_id_map['src_string_id'].array
            )

        aliases.index = pd.Index(
            aliases['src_string_id'].map(aliased_node_id_map)
        )

        aliased_node_id_map = aliases.stack().drop_duplicates()
        aliased_node_id_map.index = pd.Index(
            aliased_node_id_map.index.get_level_values(0)
        )

        idx_of_name_strings = aliased_node_id_map.loc[
            ~aliased_node_id_map.index.duplicated()
        ]
        name_strings = tn.strings.loc[idx_of_name_strings]

        tn.nodes = pd.DataFrame(
            {
                'node_type_id': name_strings['node_type_id'].array,
                'name_string_id': name_strings.index,
                'abbr_string_id': name_strings.index,
                'date_inserted': time_string,
                'date_modified': pd.NA
            },
            index=idx_of_name_strings.index
        )

        tn.reset_nodes_dtypes()

        idx_of_strings_with_no_aliases = tn.strings.index.difference(
            aliased_node_id_map.array
        )
        strings_with_no_aliases = tn.strings.loc[
            idx_of_strings_with_no_aliases
        ]

        new_nodes = pd.DataFrame({
            'node_type_id': strings_with_no_aliases['node_type_id'].array,
            'name_string_id': strings_with_no_aliases.index,
            'abbr_string_id': strings_with_no_aliases.index,
            'date_inserted': time_string,
            'date_modified': pd.NA
        })
        new_nodes = bg.util.normalize_types(new_nodes, tn.nodes)
        tn.nodes = pd.concat([tn.nodes, new_nodes])

        tn.strings.loc[aliased_node_id_map.array, 'node_id'] = (
            aliased_node_id_map.index.astype(tn.big_id_dtype)
        )
        tn.strings.loc[idx_of_strings_with_no_aliases, 'node_id'] = (
            new_nodes.index.astype(tn.big_id_dtype)
        )

    else:

        tn.nodes = pd.DataFrame(
            {
                'node_type_id': tn.strings['node_type_id'].array,
                'name_string_id': tn.strings.index,
                'abbr_string_id': tn.strings.index,
                'date_inserted': time_string,
                'date_modified': pd.NA
            }
        )

        tn.strings['node_id'] = tn.nodes.index

    tn.reset_strings_dtypes()

    for name, table in tn.node_metadata_tables.items():
        if 'string_id' in table.columns:
            node_ids = table['string_id'].map(tn.strings['node_id'])
            table = table.drop('string_id', axis='columns')
            table = pd.concat(
                [node_ids.rename('node_id'), table],
                axis='columns'
            )
            tn.node_metadata_tables[name] = table

    if link_constraints_string_id is not None:

        entry_syntax_node_id = tn.shorthand_entry_syntax['node_id'].iloc[0]
        entry_syntax_string_id = tn.nodes.loc[
            entry_syntax_node_id,
            'name_string_id'
        ]

        node_id_map = get_node_id_map_from_link_constraints(
            tn,
            link_constraints_string_id=link_constraints_string_id,
            entry_syntax_string_id=entry_syntax_string_id
        )

        to_map = tn.strings['node_id'].isin(node_id_map.index)
        mapped_values = tn.strings.loc[to_map, 'node_id'].map(node_id_map)
        tn.strings.loc[to_map, 'node_id'] = mapped_values.array

        tn.reset_strings_dtypes()

        tn.nodes = tn.nodes.loc[tn.nodes.index.isin(tn.strings['node_id']), :]

    names = _get_strings_of_extreme_length(tn.strings, 'longest')
    names = names.drop_duplicates(subset='node_id')

    abbrs = _get_strings_of_extreme_length(tn.strings, 'shortest')
    abbrs = abbrs.drop_duplicates(subset='node_id')

    tn.nodes.loc[names['node_id'], 'name_string_id'] = names.index
    tn.nodes.loc[abbrs['node_id'], 'abbr_string_id'] = abbrs.index

    tn.reset_nodes_dtypes()

    if links_excluded_from_edges is not None:

        excluded_link_type_ids = [
            tn.id_lookup('link_types', t) for t in links_excluded_from_edges
            if t in tn.link_types['link_type'].array
        ]
        assertion_selection = tn.assertions.loc[
            ~tn.assertions['link_type_id'].isin(excluded_link_type_ids)
        ]

    else:

        assertion_selection = tn.assertions

    def map_assert_str_to_nodes(label):
        return assertion_selection[label].map(tn.strings['node_id'])

    tn.edges = pd.DataFrame({
        'src_node_id': map_assert_str_to_nodes('src_string_id').array,
        'tgt_node_id': map_assert_str_to_nodes('tgt_string_id').array,
        'ref_node_id': map_assert_str_to_nodes('ref_string_id').array,
        'link_type_id': assertion_selection['link_type_id'].array,
        'date_inserted': time_string,
        'date_modified': pd.NA
    })
    tn.edges = tn.edges.drop_duplicates().reset_index(drop=True)

    tn.reset_edges_dtypes()

    tn.edge_tags = pd.DataFrame(columns=['edge_id', 'tag_string_id'])

    return tn


def textnet_from_parsed_shorthand(
    parsed,
    input_source_string,
    input_source_node_type,
    aliases_dict=None,
    aliases_case_sensitive=True,
    automatic_aliasing=False,
    link_constraints_fname=None,
    links_excluded_from_edges=None
):

    tn = build_textnet_assertions(
        parsed,
        input_source_string,
        input_source_node_type
    )

    input_source_string_id = tn.strings.loc[
        tn.strings['string'] == input_source_string
    ]
    input_source_string_id = input_source_string_id.index[0]

    if aliases_dict is not None:
        _insert_alias_assertions(
            tn,
            parsed,
            aliases_dict,
            aliases_case_sensitive,
            input_source_string_id=input_source_string_id
        )

    if automatic_aliasing:
        alias_generators = {
            'actor': bg.alias_generators.western_surname_alias_generator_vector,
            'identifier': bg.alias_generators.doi_alias_generator
        }

        strings = {
            k: tn.select_strings_by_node_type(k)
            for k in alias_generators.keys()
        }
        aliases = {
            k: alias_generators[k](v['string'])
            for k, v in dict(strings).items()
        }

        aliases = {
            k: pd.DataFrame({
                'string': v['string'].array,
                'alias': aliases[k].array
            })
            for k, v in strings.items()
        }

        aliases = {k: v.dropna() for k, v in aliases.items()}

        aliases_dict = {
            k: StringIO(v.to_csv(index=False)) for k, v in aliases.items()
        }

        _insert_alias_assertions(
            tn,
            tn,
            aliases_dict,
            aliases_case_sensitive=False,
            input_source_string_id=input_source_string_id
        )

    if link_constraints_fname is not None:

        link_constraints_node_type_id = tn.insert_node_type(
            'link_constraints_text'
        )

        with open(link_constraints_fname, 'r', encoding='utf8') as f:
            link_constraints_text = f.read()

        if link_constraints_text not in tn.strings['string'].array:
            '''new_string = pd.DataFrame({
                'string': link_constraints_text,
                'node_type_id': link_constraints_node_type_id,
                'date_inserted': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'date_modified': pd.NA
            })
            new_string = bg.util.normalize_types(new_string, tn.strings)'''
            time_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_string = bg.util.normalize_types(
                {
                    'string': link_constraints_text,
                    'node_type_id': link_constraints_node_type_id,
                    'date_inserted': time_string,
                    'date_modified': pd.NA
                },
                tn.strings
            )
            tn.strings = pd.concat([tn.strings, new_string])
            link_constraints_string_id = new_string.index[0]

    else:

        link_constraints_string_id = None

    entry_syntax_metadata = tn.shorthand_entry_syntax.squeeze()

    if entry_syntax_metadata['allow_redundant_items']:

        entry_syntax = tn.strings.loc[
            entry_syntax_metadata['string_id'],
            'string'
        ]
        entry_syntax = bg.syntax_parsing.validate_entry_syntax(
            entry_syntax,
            case_sensitive=entry_syntax_metadata['case_sensitive'],
            allow_redundant_items=True
        )

        duplicate_link_types = entry_syntax['item_link_type'].loc[
            entry_syntax[['item_node_type', 'item_link_type']].duplicated()
        ]
        duplicate_link_type_ids = [
            tn.id_lookup('link_types', lt) for lt in duplicate_link_types
        ]

        full_text_metadata = {
            k: v for k, v in tn.node_metadata_tables.items()
            if k.endswith('_text')
        }

        if len(full_text_metadata) > 1:
            raise ValueError(
                'Found multiple full text node types. Can only process'
                'one input text at a time'
            )
        else:
            full_text_metadata = full_text_metadata.popitem()[1]

        default_na_string_value = full_text_metadata['na_string_values']
        default_na_string_value = default_na_string_value.squeeze()[0]
        default_na_string_id = tn.id_lookup('strings', default_na_string_value)

        na_node_type = full_text_metadata['na_node_type'].squeeze()
        na_node_type_id = tn.id_lookup('node_types', na_node_type)

        assertion_subset = tn.assertions.query(
            'tgt_string_id == @default_na_string_id'
        )
        assertion_subset = assertion_subset.query(
            'link_type_id.isin(@duplicate_link_type_ids)'
        )
        print(assertion_subset)
        column_subset = ['src_string_id', 'tgt_string_id', 'link_type_id']
        assertions_to_keep = assertion_subset[column_subset].duplicated(
            keep='first'
        )

        drop_assertion_ids = assertion_subset.loc[~assertions_to_keep].index

        tn.assertions = tn.assertions.drop(drop_assertion_ids)

    return complete_textnet_from_assertions(
        tn,
        parsed,
        aliases_case_sensitive,
        link_constraints_string_id=link_constraints_string_id,
        links_excluded_from_edges=links_excluded_from_edges
    )


def slurp_bibtex(
    bibtex_fname,
    entry_syntax_fname,
    syntax_case_sensitive=True,
    allow_redundant_items=False,
    aliases_dict=None,
    aliases_case_sensitive=True,
    automatic_aliasing=False,
    link_constraints_fname=None,
    links_excluded_from_edges=None,
    **kwargs
):

    # make a string value representing the current function call
    frame = inspect.currentframe()
    current_module = inspect.getframeinfo(frame).filename
    current_module = Path(current_module).stem.split('.')[0]
    current_function = inspect.getframeinfo(frame).function
    current_function = '.'.join(
        ['bibliograph', current_module, current_function]
    )

    excluded_locals = [
        'frame',
        'current_module',
        'current_function',
        'excluded_locals',
        'kwargs'
    ]
    args = {k: v for k, v in locals().items() if k not in excluded_locals}
    args.update(kwargs)

    input_source_string = '{}(**{})'.format(current_function, args)

    # initialize bibtex and shorthand parsers
    bibtex_parser = _bibtexparser(common_strings=True)
    s = bg.Shorthand(
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
        input_source_string,
        'python_function',
        aliases_dict,
        aliases_case_sensitive,
        automatic_aliasing,
        link_constraints_fname,
        links_excluded_from_edges
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
    automatic_aliasing=False,
    link_constraints_fname=None,
    links_excluded_from_edges=None,
    **kwargs
):

    # make a string value representing the current function call
    frame = inspect.currentframe()
    current_module = inspect.getframeinfo(frame).filename
    current_module = Path(current_module).stem.split('.')[0]
    current_function = inspect.getframeinfo(frame).function
    current_function = '.'.join(
        ['bibliograph', current_module, current_function]
    )

    excluded_locals = [
        'frame',
        'current_module',
        'current_function',
        'excluded_locals',
        'kwargs'
    ]
    args = {k: v for k, v in locals().items() if k not in excluded_locals}
    args.update(kwargs)

    input_source_string = '{}(**{})'.format(current_function, args)

    s = bg.Shorthand(
        entry_syntax=entry_syntax_fname,
        link_syntax=link_syntax_fname,
        syntax_case_sensitive=syntax_case_sensitive,
        allow_redundant_items=allow_redundant_items
    )

    parsed = s.parse_text(shorthand_fname, **kwargs)

    tn = textnet_from_parsed_shorthand(
        parsed,
        input_source_string,
        'python_function',
        aliases_dict,
        aliases_case_sensitive,
        automatic_aliasing,
        link_constraints_fname,
        links_excluded_from_edges
    )

    return tn
