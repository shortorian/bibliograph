from ast import literal_eval
import bibliograph as bg
import pandas as pd
import inspect
from bibtexparser import dumps as _dump_bibtex_string
from bibtexparser.bibdatabase import BibDatabase as _bibtex_db
from bibtexparser.bparser import BibTexParser as _bibtexparser
from datetime import datetime
from io import StringIO
from pathlib import Path

from bibliograph.TextNet import IdLookupError


def time_string():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _insert_alias_assertions(
    tn,
    aliases_dict,
    aliases_case_sensitive,
    inp_string_id,
    generators=None
):

    # convert aliases_dict into dictionary of dataframes
    aliases = {
        k: _read_file_from_path_or_read_str_as_buffer(
            v, pd.read_csv, encoding='utf8', skipinitialspace=True
        )
        for k, v in aliases_dict.items()
        if k in tn.node_types['node_type'].array
    }

    aliases = {k: v for k, v in aliases.items() if not v.empty}

    if len(aliases) == 0:
        return

    aliases_dict = {k: v.to_csv(index=False) for k, v in aliases.items()}

    # Get relevant aliases out of each set of aliases
    aliases = {
        k: _select_aliases(v, tn, k, aliases_case_sensitive)
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
    # we're not using tn.insert_string here because we need to check the
    # strings after doing the dict comprehension below.
    new_strings = [
        pd.DataFrame({
            'string': v.stack().array,
            'node_type_id': node_type_ids[k]
        })
        for k, v in aliases.items()
    ]
    new_strings = pd.concat(new_strings).drop_duplicates()

    # Add a node type for alias reference strings and add the alias
    # reference strings to the new strings
    '''
    SWITCHING TO LITERAL NODE TYPE
    alias_txt_node_id = tn.insert_node_type('aliases_text')
    alias_txt_strings = pd.DataFrame({
        'string': aliases_dict.values(),
        'node_type_id': alias_txt_node_id
    })'''
    literal_csv_node_type_id = tn.insert_node_type('_literal_csv')
    alias_txt_strings = pd.DataFrame({
        'string': aliases_dict.values(),
        'node_type_id': literal_csv_node_type_id
    })
    new_strings = pd.concat([new_strings, alias_txt_strings])

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
        k: tn.get_strings_by_node_type(v)['string']
        for k, v in node_type_ids.items()
    }

    if not aliases_case_sensitive:

        # If aliases are not case sensitive then add new aliases
        # between any existing strings that are duplicated when
        # casefolded
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
    # Do it this way instead of using tn.insert_assertion because
    # insert_assertion can't currently handle multiple assertions
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
            'inp_string_id': inp_string_id,
            'link_type_id': alias_link_type_id
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
    tn.reset_assertions_dtypes()

    if generators is not None:

        new_assrtn_ids = new_assertions.index

        auto_ref_string_ids = tn.assertions.loc[
            new_assrtn_ids,
            'ref_string_id'
        ]
        auto_ref_string_ids = auto_ref_string_ids.unique()

        tn.strings = tn.strings.drop(auto_ref_string_ids)

        frame = inspect.currentframe()
        current_module = inspect.getframeinfo(frame).filename
        current_module = Path(current_module).stem.split('.')[0]
        current_function = inspect.getframeinfo(frame).function
        current_function = '.'.join(
            ['bibliograph', current_module, current_function]
        )

        ref_args = {
            'tn': tn,
            'aliases_dict': aliases_dict,
            'aliases_case_sensitive': aliases_case_sensitive,
            'inp_string_id': inp_string_id,
            'generators': generators
        }
        ref_string = ('{}(**{})'.format(current_function, ref_args))

        ref_string_id = tn.insert_string(
            ref_string,
            '_python_function_call',
            add_node_type=True
        )

        tn.assertions.loc[new_assrtn_ids, 'ref_string_id'] = ref_string_id


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
    tn,
    node_type,
    case_sensitive=True,
    **kwargs
):

    if tn.strings.empty or aliases.empty:
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

    strings = tn.get_strings_by_node_type(node_type)['string']
    if not case_sensitive:
        strings = strings.str.casefold()

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
    entry_syntax_string_id,
    inp_string_id
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

    # Candidate assertions have the input string provided above
    candidate_assertions = tn.get_assertions_by_string_id(
        inp_string_id,
        component='inp'
    )

    # Candidates for assertions that meet this constraint must have
    # the link types listed in the constraint
    candidate_assertions = candidate_assertions.loc[
        candidate_assertions['link_type_id'].isin(link_type_ids),
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

    if node_id_map.empty:
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
    link_constraint_string_id,
    entry_syntax_string_id,
    inp_string_id
):

    # The constraints are the full text of a CSV file stored in the
    # strings frame, so they need to be read into a dataframe
    constraints = pd.read_csv(
        StringIO(tn.strings.loc[link_constraint_string_id, 'string']),
        encoding='utf8'
    )

    # Read the entry syntax into a dataframe and make a dictionary for
    # the definition of each node type mentioned in the constraints file
    syntax_case_sensitive = tn.get_assertions_by_link_type(
        'syntax_case_sensitive'
    )
    syntax_case_sensitive = syntax_case_sensitive.query(
        'inp_string_id == @inp_string_id'
    )
    syntax_case_sensitive = tn.strings.loc[
        syntax_case_sensitive['tgt_string_id'].iloc[0],
        'string'
    ]
    syntax_case_sensitive = literal_eval(syntax_case_sensitive)

    entry_syntax = bg.syntax_parsing.validate_entry_syntax(
        tn.strings.loc[entry_syntax_string_id, 'string'],
        case_sensitive=syntax_case_sensitive
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
            entry_syntax_string_id,
            inp_string_id
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
    tn,
    inp_string,
    textnet_build_parameters
):

    # TextNet build parameters are values like case sensitivities and
    # shorthand default entry prefixes. They're stored as string values
    # with node type '_literal_python' so they can be identified and
    # retrieved easily
    '''if '_literal_python' in tn.node_types['node_type'].array:

        literal_node_type_id = tn.id_lookup('node_types', '_literal_python')
        q = 'node_type_id == @literal_node_type_id'
        hashed_textnet_strings = pd.util.hash_pandas_object(
            tn.strings.query(q)[['string', 'node_type_id']],
            index=False
        )

        # Before storing any new literal values we need to check that we
        # won't create duplicates. Hash pairs of values for faster search.
        hashed_parameter_strings = pd.util.hash_pandas_object(
            pd.DataFrame({
                'string': [str(v) for v in textnet_build_parameters.values()],
                'node_type_id': literal_node_type_id
            }),
            index=False
        )
        parameter_keys_to_keep = pd.Series(textnet_build_parameters.keys())
        parameter_keys_to_keep = parameter_keys_to_keep.loc[
            ~hashed_parameter_strings.isin(hashed_textnet_strings)
        ]

        textnet_build_parameters = {
            k: v
            for k, v in parameter_keys_to_keep.array
        }

    else:
        pass

    input_metadata_link_type_ids = [
        tn.insert_link_type(k) for k in textnet_build_parameters.keys()
    ]
    parameter_literals = [
        '"{}"'.format(v) if isinstance(v, str) else str(v)
        for v in textnet_build_parameters.values()
    ]
    parameter_literals = list(map(str, parameter_literals))

    for v in set(parameter_literals):
        tn.insert_string(v, '_literal_python', allow_duplicates='suppress')'''

    parameter_literals = [
        '"{}"'.format(v) if isinstance(v, str) else str(v)
        for v in textnet_build_parameters.values()
    ]

    tn.insert_string(
        parameter_literals,
        '_literal_python',
        allow_duplicates='suppress',
        add_node_type=True
    )

    inp_string_id = tn.id_lookup('strings', inp_string)
    input_metadata_string_ids = [
        tn.id_lookup('strings', v) for v in parameter_literals
    ]
    input_metadata_link_type_ids = [
        tn.insert_link_type(k) for k in textnet_build_parameters.keys()
    ]

    tn.insert_assertion(
        inp_string_id,
        inp_string_id,
        input_metadata_string_ids,
        inp_string_id,
        input_metadata_link_type_ids
    )

    return tn


def complete_textnet_from_assertions(
    tn,
    aliases_case_sensitive,
    current_inp_string_id,
    link_constraints_string_id,
    links_excluded_from_edges,
    apply_link_constraints=True
):

    # If there are alias links in the assertions table, map them to the
    # same node IDs
    try:
        alias_link_type_id = tn.id_lookup('link_types', 'alias')
        alias_assertions_exist = (
            tn.assertions['link_type_id'] == alias_link_type_id
        )
        alias_assertions_exist = alias_assertions_exist.any()

    except IdLookupError:
        alias_assertions_exist = False

    if alias_assertions_exist:

        aliases_sensitivity_link_type_id = tn.id_lookup(
            'link_types',
            'aliases_case_sensitive'
        )

        link_is_alias_sensitivity = (
            tn.assertions['link_type_id'] == aliases_sensitivity_link_type_id
        )

        case_snstve_alias_inputs = tn.get_assertions_by_literal_tgt(
            True,
            subset=link_is_alias_sensitivity
        )
        case_snstve_alias_inputs = case_snstve_alias_inputs['inp_string_id']
        case_snstve_aliases = tn.assertions.query(
            'inp_string_id.isin(@case_snstve_alias_inputs)'
        )

        case_snstve_aliases = case_snstve_aliases.query(
            'link_type_id == @alias_link_type_id'
        )

        if not case_snstve_aliases.empty:

            case_snstve_aliases = case_snstve_aliases[
                ['src_string_id', 'tgt_string_id']
            ]

            # convert aliases to a one-to-many map
            case_snstve_aliases = bg.util.make_bidirectional_map_one_to_many(
                case_snstve_aliases
            )

            src_node_types = case_snstve_aliases['src_string_id'].map(
                tn.strings['node_type_id']
            )
            tgt_node_types = case_snstve_aliases['tgt_string_id'].map(
                tn.strings['node_type_id']
            )

            if (src_node_types != tgt_node_types).any():
                raise ValueError(
                    'There are alias assertions between strings with '
                    'different node types'
                )

            src_string_ids = case_snstve_aliases['src_string_id'].unique()
            aliased_node_id_map = pd.Series(
                range(len(src_string_ids)),
                index=src_string_ids
            )

        case_insnstve_alias_inputs = tn.get_assertions_by_literal_tgt(
            False,
            subset=link_is_alias_sensitivity
        )
        case_insnstve_alias_inputs = case_insnstve_alias_inputs[
            'inp_string_id'
        ]

        case_insnstve_aliases = tn.assertions.query(
            'inp_string_id.isin(@case_insnstve_alias_inputs)'
        )
        case_insnstve_aliases = case_insnstve_aliases.query(
            'link_type_id == @alias_link_type_id'
        )

        if not case_insnstve_aliases.empty:

            case_insnstve_aliases = case_insnstve_aliases[
                ['src_string_id', 'tgt_string_id']
            ]

            # convert aliases to a one-to-many map
            case_insnstve_aliases = bg.util.make_bidirectional_map_one_to_many(
                case_insnstve_aliases
            )

            src_node_types = case_insnstve_aliases['src_string_id'].map(
                tn.strings['node_type_id']
            )
            tgt_node_types = case_insnstve_aliases['tgt_string_id'].map(
                tn.strings['node_type_id']
            )

            if (src_node_types != tgt_node_types).any():
                raise ValueError(
                    'There are alias assertions between strings with '
                    'different node types'
                )

            string_subset = case_insnstve_aliases['src_string_id'].map(
                tn.strings['string']
            )
            string_subset = string_subset.str.casefold()

            node_ids = string_subset.unique()
            node_ids = pd.Series(range(len(node_ids)), index=node_ids)

            insnstve_alias_map = string_subset.map(node_ids)

            insnstve_alias_map = pd.concat(
                [
                    case_insnstve_aliases['src_string_id'],
                    insnstve_alias_map.rename('node_id')
                ],
                axis='columns'
            )
            insnstve_alias_map = insnstve_alias_map.drop_duplicates()
            insnstve_alias_map = pd.Series(
                insnstve_alias_map['node_id'].array,
                index=insnstve_alias_map['src_string_id'].array
            )

            if not case_snstve_aliases.empty:

                aliased_node_id_map += insnstve_alias_map.max()

                snstve_alias_map = aliased_node_id_map.rename('snstve_nodes')
                snstve_alias_map = snstve_alias_map.reset_index().rename(
                    columns={'index': 'src_string_id'}
                )

                insnstve_to_snstve_map = insnstve_alias_map.rename(
                    'insnstve_nodes'
                )
                insnstve_to_snstve_map = insnstve_to_snstve_map.reset_index()
                insnstve_to_snstve_map = insnstve_to_snstve_map.rename(
                    columns={'index': 'src_string_id'}
                )
                insnstve_to_snstve_map = pd.Series(
                    insnstve_to_snstve_map['insnstve_nodes'].array,
                    index=insnstve_to_snstve_map['snstve_nodes']
                )

                aliased_node_id_map = aliased_node_id_map.map(
                    insnstve_to_snstve_map
                )

                aliased_node_id_map = pd.concat(
                    [aliased_node_id_map, insnstve_alias_map]
                )

                aliased_node_id_map = aliased_node_id_map.drop_duplicates()

            else:

                aliased_node_id_map = insnstve_alias_map

        aliases = pd.concat([case_insnstve_aliases, case_snstve_aliases])
        aliases = aliases.drop_duplicates()

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
                'abbr_string_id': name_strings.index
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
            'abbr_string_id': strings_with_no_aliases.index
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
                'abbr_string_id': tn.strings.index
            }
        )

        tn.strings['node_id'] = tn.nodes.index

    tn.reset_strings_dtypes()

    names = _get_strings_of_extreme_length(tn.strings, 'longest')
    names = names.drop_duplicates(subset='node_id')

    abbrs = _get_strings_of_extreme_length(tn.strings, 'shortest')
    abbrs = abbrs.drop_duplicates(subset='node_id')

    tn.nodes.loc[names['node_id'], 'name_string_id'] = names.index
    tn.nodes.loc[abbrs['node_id'], 'abbr_string_id'] = abbrs.index

    '''
    SWITCHING TO LITERAL NODE TYPE
    assert_input_op_requirements = tn.get_assertions_by_link_type('requires')
    assert_input_op_requirements = assert_input_op_requirements.query(
        '(inp_string_id == src_string_id) & (inp_string_id == ref_string_id)'
    )
    link_constraint_string_ids = tn.get_strings_by_node_type(
        'link_constraints_text',
        string_ids_only=True
    )
    assert_link_constraint_required = assert_input_op_requirements.query(
        'tgt_string_id.isin(@link_constraint_string_ids)'
    )

    if apply_link_constraints and not assert_link_constraint_required.empty:

        entry_syntax_string_ids = tn.get_strings_by_node_type(
            'shorthand_entry_syntax',
            string_ids_only=True
        )
        assert_entry_syntax_required = assert_input_op_requirements.query(
            'tgt_string_id.isin(@entry_syntax_string_ids)'
        )
        entry_syntax_string_ids = pd.Series(
            assert_entry_syntax_required['tgt_string_id'].array,
            index=assert_entry_syntax_required['inp_string_id']
        )

        link_constraint_string_ids = pd.Series(
            assert_link_constraint_required['tgt_string_id'].array,
            index=assert_link_constraint_required['inp_string_id']
        )'''
    link_constraint_assertions = tn.get_assertions_by_link_type(
        'link_constraints',
        allow_missing_type=True
    )

    has_link_constraints = (link_constraints_string_id is not None)
    if has_link_constraints and not link_constraint_assertions.empty:

        link_constraint_assertions = link_constraint_assertions.sort_values(
            by='inp_string_id'
        )
        link_constraint_string_ids = link_constraint_assertions[
            'tgt_string_id'
        ]

        constrnt_inp_string_ids = (
            link_constraint_assertions['inp_string_id'].unique()
        )
        entry_syntax_string_ids = tn.get_assertions_by_link_type(
            'shorthand_entry_syntax'
        )
        entry_syntax_string_ids = entry_syntax_string_ids.query(
            'inp_string_id.isin(@constrnt_inp_string_ids)'
        )
        entry_syntax_string_ids = entry_syntax_string_ids.sort_values(
            by='inp_string_id'
        )
        entry_syntax_string_ids = entry_syntax_string_ids['tgt_string_id']

        constraint_and_syntax_string_id_by_inp_string_id = pd.DataFrame(
            {
                'link_constraint_string_id': link_constraint_string_ids.array,
                'entry_syntax_string_id': entry_syntax_string_ids.array,
            },
            index=link_constraint_assertions['inp_string_id']
        )

        def apply_link_constraints(inp_string_id, id_pair):
            return get_node_id_map_from_link_constraints(
                tn,
                link_constraint_string_id=id_pair['link_constraint_string_id'],
                entry_syntax_string_id=id_pair['entry_syntax_string_id'],
                inp_string_id=inp_string_id
            )

        node_id_map = {
            inp_string_id: apply_link_constraints(inp_string_id, id_pair)
            for inp_string_id, id_pair in
            constraint_and_syntax_string_id_by_inp_string_id.iterrows()
        }
        node_id_map = pd.concat(node_id_map.values())

        alias_link_type_id = tn.insert_link_type('alias')

        src_string_ids = tn.nodes.loc[node_id_map.index, 'name_string_id']
        tgt_string_ids = tn.nodes.loc[node_id_map, 'name_string_id']

        new_assertions = pd.DataFrame({
            'inp_string_id': current_inp_string_id,
            'src_string_id': src_string_ids.array,
            'tgt_string_id': tgt_string_ids.array,
            'ref_string_id': link_constraints_string_id,
            'link_type_id': alias_link_type_id
        })
        new_assertions = bg.util.normalize_types(new_assertions, tn.assertions)
        tn.assertions = pd.concat([tn.assertions, new_assertions])

        tn.strings['node_type_id'] = tn.strings['node_id'].map(
            tn.nodes['node_type_id']
        )

        cols = ['string', 'node_type_id', 'date_inserted', 'date_modified']
        tn.strings = tn.strings[[c for c in cols if c in tn.strings.columns]]
        del tn.nodes

        return complete_textnet_from_assertions(
            tn,
            aliases_case_sensitive=aliases_case_sensitive,
            current_inp_string_id=current_inp_string_id,
            link_constraints_string_id=None,
            links_excluded_from_edges=links_excluded_from_edges,
            apply_link_constraints=False
        )

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

    def map_assert_str_id_to_node_id(label):
        return assertion_selection[label].map(tn.strings['node_id'])

    tn.edges = pd.DataFrame({
        'src_node_id': map_assert_str_id_to_node_id('src_string_id').array,
        'tgt_node_id': map_assert_str_id_to_node_id('tgt_string_id').array,
        'ref_node_id': map_assert_str_id_to_node_id('ref_string_id').array,
        'link_type_id': assertion_selection['link_type_id'].array
    })
    tn.edges = tn.edges.drop_duplicates().reset_index(drop=True)

    tn.reset_edges_dtypes()

    tn.edge_tags = pd.DataFrame(columns=['edge_id', 'tag_string_id'])

    return tn


def textnet_from_parsed_shorthand(
    parsed,
    inp_string,
    aliases_dict=None,
    aliases_case_sensitive=True,
    automatic_aliasing=False,
    link_constraints_fname=None,
    links_excluded_from_edges=None,
    textnet_build_parameters=None
):

    linking_parameters = {
        'aliases_dict': aliases_dict,
        'aliases_case_sensitive': aliases_case_sensitive,
        'automatic_aliasing': automatic_aliasing,
        'links_excluded_from_edges': links_excluded_from_edges
    }

    if textnet_build_parameters is not None:
        textnet_build_parameters.update(linking_parameters)
    else:
        textnet_build_parameters = linking_parameters

    tn = build_textnet_assertions(
        parsed,
        inp_string,
        textnet_build_parameters
    )

    inp_string_id = tn.id_lookup('strings', inp_string)
    #alias_link_type_id = tn.insert_link_type('alias')

    if aliases_dict is not None:

        _insert_alias_assertions(
            tn,
            aliases_dict,
            aliases_case_sensitive,
            inp_string_id=inp_string_id
        )

    actor_aliaser = bg.alias_generators.western_surname_alias_generator_vector
    id_aliaser = bg.alias_generators.doi_alias_generator

    if automatic_aliasing:

        alias_generators = {'actor': actor_aliaser, 'identifier': id_aliaser}

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
            aliases_dict,
            aliases_case_sensitive=False,
            inp_string_id=inp_string_id,
            generators=alias_generators
        )

    if link_constraints_fname is not None:

        with open(link_constraints_fname, 'r', encoding='utf8') as f:
            link_constraints_text = f.read()

        if link_constraints_text not in tn.strings['string'].array:

            '''
            SWITCHING TO LITERAL NODE TYPE
            link_constraints_node_type_id = tn.insert_node_type(
                'link_constraints_text'
            )

            link_constraints_string_id = tn.insert_string(
                link_constraints_text,
                link_constraints_node_type_id,
                time_string()
            )'''

            link_constraints_string_id = tn.insert_string(
                link_constraints_text,
                tn.id_lookup('node_types', '_literal_csv'),
                time_string()
            )

            tn.insert_link_type('link_constraints')

            tn.insert_assertion(
                inp_string,
                inp_string,
                link_constraints_text,
                inp_string,
                'link_constraints'
            )

    else:
        link_constraints_string_id = None

    input_metadata = tn.resolve_assertions(
        subset=tn.get_assertions_by_tgt_node_type('_literal_python').index
    )
    input_metadata = dict(zip(
        input_metadata['link_type'],
        [literal_eval(v) for v in input_metadata['tgt_string'].array]
    ))

    if input_metadata['allow_redundant_items']:

        '''
        SWITCHING TO LITERAL NODE TYPE
        entry_syntax_assertions = tn.get_assertions_by_tgt_node_type(
            'shorthand_entry_syntax'
        )'''
        entry_syntax_assertions = tn.get_assertions_by_link_type(
            'shorthand_entry_syntax'
        )
        entry_syntax = entry_syntax_assertions['tgt_string'].squeeze()
        entry_syntax = bg.syntax_parsing.validate_entry_syntax(
            entry_syntax,
            case_sensitive=input_metadata['syntax_case_sensitive'],
            allow_redundant_items=input_metadata['allow_redundant_items']
        )

        default_na_string_value = input_metadata['na_string_values'][0]
        default_na_string_id = tn.id_lookup('strings', default_na_string_value)
        assertion_subset = tn.assertions.query(
            'tgt_string_id == @default_na_string_id'
        )

        duplicate_link_types = entry_syntax['item_link_type'].loc[
            entry_syntax[['item_node_type', 'item_link_type']].duplicated()
        ]
        duplicate_link_type_ids = [
            tn.id_lookup('link_types', lt) for lt in duplicate_link_types
        ]
        assertion_subset = assertion_subset.query(
            'link_type_id.isin(@duplicate_link_type_ids)'
        )

        column_subset = ['src_string_id', 'tgt_string_id', 'link_type_id']
        assertions_to_keep = assertion_subset[column_subset].duplicated(
            keep='first'
        )

        drop_assertion_ids = assertion_subset.loc[~assertions_to_keep].index

        tn.assertions = tn.assertions.drop(drop_assertion_ids)

    tn = complete_textnet_from_assertions(
        tn,
        aliases_case_sensitive,
        inp_string_id,
        link_constraints_string_id=link_constraints_string_id,
        links_excluded_from_edges=links_excluded_from_edges
    )

    date_inserted = time_string()

    for attr in ['assertions', 'strings', 'nodes', 'edges']:
        table = tn.__getattr__(attr)
        table['date_inserted'] = date_inserted
        table['date_modified'] = pd.NA

    tn.reset_assertions_dtypes()
    tn.reset_strings_dtypes()
    tn.reset_nodes_dtypes()
    tn.reset_edges_dtypes()

    return tn


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

    inp_string = '{}(**{})'.format(current_function, args)

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
            input_string=inp_string,
            input_node_type='_python_function_call',
            **kwargs
        )

    textnet_build_parameters = kwargs
    textnet_build_parameters['syntax_case_sensitive'] = syntax_case_sensitive
    textnet_build_parameters['allow_redundant_items'] = allow_redundant_items

    tn = textnet_from_parsed_shorthand(
        parsed,
        inp_string,
        aliases_dict,
        aliases_case_sensitive,
        automatic_aliasing,
        link_constraints_fname,
        links_excluded_from_edges,
        textnet_build_parameters
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
    item_separator='__',
    space_char='|',
    skiprows=0,
    na_string_values='!',
    na_node_type='missing',
    default_entry_prefix='wrk',
    comment_char='#'
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

    inp_string = '{}(**{})'.format(current_function, args)

    s = bg.Shorthand(
        entry_syntax=entry_syntax_fname,
        link_syntax=link_syntax_fname,
        syntax_case_sensitive=syntax_case_sensitive,
        allow_redundant_items=allow_redundant_items
    )

    textnet_build_parameters = {
        'item_separator': item_separator,
        'space_char': space_char,
        'na_string_values': na_string_values,
        'na_node_type': na_node_type,
        'default_entry_prefix': default_entry_prefix,
        'comment_char': comment_char,
        'skiprows': skiprows
    }

    parsed = s.parse_text(
        shorthand_fname,
        input_string=inp_string,
        input_node_type='_python_function_call',
        **textnet_build_parameters
    )

    textnet_build_parameters['syntax_case_sensitive'] = syntax_case_sensitive
    textnet_build_parameters['allow_redundant_items'] = allow_redundant_items

    tn = textnet_from_parsed_shorthand(
        parsed,
        inp_string,
        aliases_dict,
        aliases_case_sensitive,
        automatic_aliasing,
        link_constraints_fname,
        links_excluded_from_edges,
        textnet_build_parameters
    )

    return tn


def slurp_single_column(
    shorthand_fname,
    entry_syntax_fname,
    syntax_case_sensitive=True,
    allow_redundant_items=False,
    aliases_dict=None,
    aliases_case_sensitive=True,
    automatic_aliasing=False,
    link_constraints_fname=None,
    links_excluded_from_edges=None,
    item_separator='__',
    space_char='|',
    skiprows=0,
    na_string_values='!',
    na_node_type='missing',
    default_entry_prefix='wrk',
    comment_char='#'
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

    inp_string = '{}(**{})'.format(current_function, args)

    s = bg.Shorthand(
        entry_syntax=entry_syntax_fname,
        syntax_case_sensitive=syntax_case_sensitive,
        allow_redundant_items=allow_redundant_items
    )

    textnet_build_parameters = {
        'item_separator': item_separator,
        'space_char': space_char,
        'na_string_values': na_string_values,
        'na_node_type': na_node_type,
        'default_entry_prefix': default_entry_prefix,
        'comment_char': comment_char,
        'skiprows': skiprows
    }

    parsed = s.parse_text(
        shorthand_fname,
        input_string=inp_string,
        input_node_type='_python_function_call',
        drop_na=[],
        **textnet_build_parameters
    )

    textnet_build_parameters['syntax_case_sensitive'] = syntax_case_sensitive
    textnet_build_parameters['allow_redundant_items'] = allow_redundant_items

    tn = textnet_from_parsed_shorthand(
        parsed,
        inp_string,
        aliases_dict,
        aliases_case_sensitive,
        automatic_aliasing,
        link_constraints_fname,
        links_excluded_from_edges,
        textnet_build_parameters
    )

    return tn
