import string

from numpy import string_
import bibliograph as bg
import pandas as pd

def _apply_alias_map(strings, aliases, inplace=False):
    '''
    creates a lookup table between node IDs of source strings and any
    string aliases that exist in the same dataframe. replaces node IDs
    of string aliases with node IDs of source strings. node IDs are
    overwritten either on a copy of the input strings frame or, if 
    inplace == True, the input strings frame is mutated.

    Parameters
    ----------
    strings : pandas.DataFrame
        Input string values. Column labels must include
        ['node_id', 'string']
    
    aliases : pandas.DataFrame
        A map between string values and aliases. Column labels must
        include ['key', 'value']

    inplace : bool
        Mutate the input and don't make a copy. Default False.

    Returns
    -------
    only has a return value if inplace is True
    output : pandas.DataFrame
        copy of input strings frame with node IDs of string aliases
        replaced with node IDs from the source strings

    '''
    alias_map = strings.merge(aliases, left_on='string', right_on='key')
    alias_map = alias_map.merge(strings, left_index=True, right_index=True)
    alias_map = alias_map[['node_id_y', 'node_id_x']].drop_duplicates()
    alias_map = pd.Series(alias_map['node_id_y'].array,
                          index=alias_map['node_id_x'].array)

    string_is_aliased = strings['node_id'].isin(alias_map.index)
    mapped_nodes = strings.loc[string_is_aliased, 'node_id']

    if inplace:
        strings.loc[string_is_aliased, 'node_id'] = alias_map[mapped_nodes].array 
    else:
        output = strings.copy()
        output.loc[string_is_aliased, 'node_id'] = alias_map[mapped_nodes].array
        return output


def map_aliases(strings, aliases, case_sensitive=False):
    '''
    overwrites node_ids of strings which are aliases for other strings.

    Parameters
    ----------
    strings : pandas.DataFrame
        Input string values. Column labels must include
        ['string_id', 'node_id', 'string'] and at least one additional
        column labeled 'node_type' or 'node_type_id'

    aliases : pandas.DataFrame
        A map between string values and aliases. Column labels must
        include ['key', 'value']

    case_sensitive : bool
        If false, make all strings and aliases lower case before mapping
        node IDs.

    Returns
    -------
    pandas.DataFrame
        Dataframe with all the input values in the 'string' column and
        whose node IDs are a (possibly improper) subset of the input
        values in the 'node_id' column. The output dataframe will
        include rows for any aliases whose keys were not in the input
        strings frame but whose values were.

    Examples
    --------
    >>> import pandas as pd
    >>> strings = pd.DataFrame({'string_id':[0, 1, 2, 10],
    ...                         'node_id':[5, 6, 7, 8],
    ...                         'string':['alice smith',
    ...                                   'Alice Smith',
    ...                                   'asmith',
    ...                                   'A. Smith']})
    >>> nodes = pd.DataFrame({'node_id':[5, 6, 7, 8, 9, 10],
    ...                       'node_type_id':[0, 0, 0, 0, 1, 4]})
    >>> aliases = pd.DataFrame({'key':['asmith', 'A. Smith'],
    ...                         'value':['Alice Smith', 'alice smith']})
    >>> (strings, nodes, aliases)
    (   string_id  node_id       string
    0          0        5  alice smith
    1          1        6  Alice Smith
    2          2        7       asmith
    3         10        8     A. Smith,
       node_id  node_type_id
    0        5             0
    1        6             0
    2        7             0
    3        8             0
    4        9             1
    5       10             4,
            key        value
    0    asmith  Alice Smith
    1  A. Smith  alice smith)

    >>> map_aliases(strings, nodes, aliases).sort_values(by='node_id')
       string_id  node_id       string
    0          0        5  alice smith
    1          1        5  Alice Smith
    2          2        5       asmith
    3         10        5     A. Smith

    >>> map_aliases(strings, nodes, aliases, case_sensitive=True)
       string_id  node_id       string
    0          0        5  alice smith
    1          1        6  Alice Smith
    2          2        5       asmith
    3         10        6     A. Smith

    >>> aliases = pd.DataFrame({'key':['asmith',
    ...                                'A. Smith',
    ...                                'A.B. Smith',
    ...                                'a.b. smith'],
    ...                         'value':['Alice Smith',
    ...                                  'alice smith',
    ...                                  'alice smith',
    ...                                  'alice smith']})
    >>> bg.map_aliases(strings, nodes, aliases)
       string_id  node_id       string
    0          0        5  alice smith
    1          1        5  Alice Smith
    2          2        5       asmith
    3         10        5     A. Smith
    4          3        5   A.B. Smith
    5          4        5   a.b. smith
    
    >>> bg.map_aliases(strings, nodes, aliases, case_sensitive=True)
       string_id  node_id       string
    0          0        5  alice smith
    1          1        6  Alice Smith
    2          2        5       asmith
    3         10        6     A. Smith
    4          3        5   A.B. Smith
    5          4        5   a.b. smith

    >>> nodes = pd.DataFrame({'node_id':[5, 6, 7, 8, 9, 10],
    ...                       'node_type_id':[0, 4, 0, 0, 1, 4]})
    >>> map_aliases(strings, nodes, aliases)

    <stack trace>
    ValueError: Input strings map to more than one node type.
                Aliases must be mapped for a single node type.
    '''

    if strings.empty:
        return strings

    if 'node_type' in strings.columns:
        if len(strings['node_type'].value_counts()) > 1:
            raise ValueError('Input strings frame contains references to more '
                             'than one node type. Aliases must be mapped for '
                             'a single node type.')
    elif 'node_type_id' in strings.columns:
        if len(strings['node_type_id'].value_counts()) > 1:
            raise ValueError('Input strings frame contains references to more '
                             'than one node type. Aliases must be mapped for '
                             'a single node type.')
    else:
        raise ValueError('input strings frame must contain a column labeled '
                         'node_type or node_type_id.')

    if aliases['key'].isin(aliases['value']).any():
        raise ValueError('Aliases has strings that appear as both keys'
                         ' and values.')


    original_index = strings.index
    # we're going to mutate the strings frame, so cache the reference and
    # make an explicit copy
    _strings = strings
    strings = strings.copy()
    strings.reset_index(drop=True, inplace=True)

    # get index values for aliases whose values exist in the strings
    # dataframe but whose keys do not
    key_is_new = ~aliases['key'].isin(strings['string']) \
                 & aliases['value'].isin(strings['string'])
    key_is_new = aliases.loc[key_is_new].index

    # if not case sensitive then store a copy of the string values and
    # lower them. also make the aliases index lower case but leave the
    # values in original case so that we don't have to make an extra
    # copy of the aliases
    if not case_sensitive:
        strings.loc[:, 'string'] = strings['string'].str.lower().array

        if strings['string'].duplicated().any():

            def set_minimum(group, column):
                group.loc[:, column] = group[column].min()
                return group

            strings = strings.groupby(by='string') \
                             .apply(lambda x: set_minimum(x,'node_id'))

        # we're going to mutate aliases, so cache the reference and make a copy
        _aliases = aliases
        aliases = aliases.copy()
        aliases.loc[:, 'key'] = aliases['key'].str.lower().array

        # drop rows if keys are now mapped to themselves
        identities = aliases['key'].isin(aliases['value'])
        aliases.drop(aliases.index[identities], inplace=True)
        # drop any duplicate aliases created when lowering keys
        aliases.drop_duplicates(inplace=True)

    if aliases['key'].duplicated().any():
        raise ValueError('Aliases contains key(s) that map to multiple values '
                         'when case_sensitive={}. Aliases cannot be a one-to-'
                         'many or many-to-many map.'.format(case_sensitive))        

    if not aliases['key'].isin(strings['string']).any():
        if not case_sensitive:
            strings.loc[:, 'string'] = _strings['string'].array
        return strings

    _apply_alias_map(strings, aliases, inplace=True)

    strings.index = original_index

    # if there are aliases whose values exist in the strings dataframe
    # but whose keys do not, then infer node IDs for those keys and
    # append them to the strings dataframe 
    if not key_is_new.empty:

        if not case_sensitive:
            aliases_w_new_keys = _aliases.loc[key_is_new]
        else:
            aliases_w_new_keys = aliases.loc[key_is_new]

        aliases_w_new_keys.reset_index(inplace=True)
        aliases_w_new_keys.rename(columns={'index':'alias_index'},
                                  inplace=True)
        string_map = aliases_w_new_keys.merge(strings,
                                              left_on='value',
                                              right_on='string')

        if not case_sensitive:
            new_strings = _aliases.loc[string_map['alias_index'], 'key'].array
        else:
            new_strings = string_map['key'].array

        new_strings_frame = pd.DataFrame({'node_id':string_map['node_id'].array,
                                          'string':new_strings})
        new_strings_frame.drop_duplicates(inplace=True)

        new_string_ids = bg.non_intersecting_sequence(new_strings_frame.index,
                                                      strings['string_id'])
        new_strings_frame['string_id'] = new_string_ids
        
        _apply_alias_map(new_strings_frame, aliases, inplace=True)

        if not case_sensitive:
            strings.loc[:, 'string'] = _strings['string'].array

        strings = strings.append(new_strings_frame, ignore_index=True)
    
    elif not case_sensitive:
        strings.loc[:, 'string'] = _strings['string'].array

    strings.drop_duplicates(inplace=True)

    return strings


def _make_new_nodes_table(tn, node_data):
    # the nodes table contains a relationship between a node ID and a 
    # node type ID, so take the (node ID, node type) pairs from the 
    # node data and replace the node types with node type IDs
    nodes = node_data.loc[:, ['node_id', 'node_type']]
    nodes.drop_duplicates(inplace=True)

    if nodes['node_id'].duplicated().any():
        raise ValueError('a node_id has been assigned more than one '
                         'node_type_id')
                         
    nodes = nodes.merge(tn.node_types[['node_type_id','node_type']])
    nodes.drop('node_type', axis=1, inplace=True)
    nodes.reset_index(drop=True, inplace=True)

    # the nodes table also has string IDs for default names and 
    # abbreviations. naively assume that the longest string pointing to
    # each node is the name and the shortest string is the abbreviation
    columns = ['node_id', 'name_string_id', 'abbr_string_id']
    def group_max_min_length(group):
        lengths = group['string'].apply(len)
        max_string_id = group.loc[lengths.idxmax(), 'string_id']
        min_string_id = group.loc[lengths.idxmin(), 'string_id']
        node_id = group.name
        return pd.Series([node_id, max_string_id, min_string_id],
                         index=columns)
    string_lengths = node_data.groupby(by='node_id')
    string_lengths = string_lengths.apply(group_max_min_length)
    string_lengths.reset_index(drop=True, inplace=True)
    nodes = nodes.merge(string_lengths)
    #nodes['name_string_id'] = string_lengths['max'].array
    #nodes['abbr_string_id'] = string_lengths['min'].array

    return nodes


def _make_new_assertions_table(tn,
                               node_data,
                               assrtn_tgt_label):

    # the assertions table represents a four-part relation: a source
    # contains a link between two strings. the relation is represented
    # by the tuple (source_id,
    #               src_string_id,
    #               tgt_string_id,
    #               link_type_id)

    # assertion targets are represented by automatically generated 
    # strings, so find those in the node data
    string_is_assrtn_tgt = (node_data['string'] == node_data[assrtn_tgt_label])

    # get sources and targets, merge them to form the assertions table
    targets = node_data.loc[string_is_assrtn_tgt, ['string', 'string_id']]
    sources = node_data.loc[~string_is_assrtn_tgt,
                            [assrtn_tgt_label, 'string_id', 'link_type']]
    assertions = sources.merge(targets,
                               left_on=assrtn_tgt_label,
                               right_on='string',
                               suffixes=['_src','_tgt']) 
    assertions = assertions[['string_id_src', 'string_id_tgt', 'link_type']]
    assertions.rename(columns={'string_id_src':'src_string_id',
                               'string_id_tgt':'tgt_string_id'},
                      inplace=True)

    # assign source_id, replace link types with link IDs, create the
    # assertion_id column
    assertions = assertions.merge(tn.link_types[['link_type', 'link_type_id']])
    assertions.drop('link_type', axis=1, inplace=True)
    assertions['assertion_id'] = assertions.index.array

    return assertions


def _make_edges_from_assertions(tn, assrtn_selection):
    
    # the edges table is a three-part relation: an edge is a link 
    # between two sources, so we have a src_node_id, a tgt_node_id, and
    # a link_type_id

    # we naively assume that all assertions and string->node assignments
    # are valid, so get relevant columns from the assertions table and
    # replace strings with nodes
    edges = tn.resolve_assertions(assrtn_selection)
    edges = edges[['src_node_id', 'tgt_node_id', 'link_type_id']]

    edges.drop_duplicates(inplace=True)

    edges['edge_id'] = edges.index.array

    return edges


def _normalize_metadata(tn,
                        metadata,
                        node_data,
                        assrtn_tgt_label):

    string_is_assrtn_tgt = (node_data['string'] == node_data[assrtn_tgt_label])
    
    def get_node_metadata(node_type):
        node_md_cols = tn.get_table_by_label(node_type).columns
        correct_type = (metadata['node_type'] == node_type)
        correct_node_cols = metadata['metadata_column'].isin(node_md_cols)

        output = metadata.loc[correct_type & correct_node_cols, :]
        output = output.pivot(columns='input_label',
                              values='string',
                              index=assrtn_tgt_label)
                              
        target_nodes = node_data.loc[string_is_assrtn_tgt]
        node_id_map = pd.Series(target_nodes['node_id'].array,
                                index=target_nodes[assrtn_tgt_label].array)
        output['node_id'] = node_id_map[output.index].array
        return output

    return {node_type:get_node_metadata(node_type)
            for node_type in tn.node_types['node_type']}


def _make_new_node_md_table(tn, node_type, md_table):
    tn_columns = tn.schema[node_type].columns
    node_type_id = tn.get_element_by_value('node_type',
                                           node_type,
                                           'node_type_id')
    common_cols = [c for c in md_table.columns if c in tn_columns]
    md_table = md_table.loc[:, common_cols]

    if not md_table.empty:
        md_table['node_type_id'] = node_type_id
    else:
        md_table = tn.nodes.loc[tn.nodes['node_type_id']==node_type_id,
                                ['node_id', 'node_type_id']]

    return md_table.reset_index(drop=True)


def _make_md_strings_and_assrtns(tn,
                                 node_md_table,
                                 node_data,
                                 assrtn_tgt_label):

    node_ids = node_md_table['node_id'].array

    md_columns = node_md_table.columns[2:]
    node_md_table = node_md_table.loc[:, md_columns]
    node_md_table.fillna(' ', inplace=True)

    md_strings = node_md_table.apply(lambda x: tn.md_sep.join(x), axis=1).array
    string_ids = bg.non_intersecting_sequence(len(md_strings),
                                              tn.strings['string_id'])
    

    new_strings = pd.DataFrame({'string_id':string_ids,
                                'node_id':node_ids,
                                'string':md_strings},
                                index=string_ids)
    
    assrtn_targets = node_data.loc[:, [assrtn_tgt_label, 'node_id']]
    assrtn_targets.drop_duplicates(inplace=True)
    merged = new_strings.merge(assrtn_targets)
    
    merged.rename(columns={'string_id':'src_string_id'}, inplace=True) 
    merged = merged.merge(tn.strings,
                          left_on=assrtn_tgt_label,
                          right_on='string')
    
    merged.rename(columns={'string_id':'tgt_string_id'}, inplace=True)

    assrtn_ids = bg.non_intersecting_sequence(len(md_strings),
                                              tn.assertions['assertion_id'])
    md_link_id = tn.get_element_by_value('link_type',
                                         'metadata',
                                         'link_type_id')

    new_assrtns = pd.DataFrame({'assertion_id':assrtn_ids,
                                'src_string_id':merged['src_string_id'].array,
                                'tgt_string_id':merged['tgt_string_id'].array,
                                'link_type_id':md_link_id},
                                index=assrtn_ids)

    return new_strings, new_assrtns

    
def create_textnet_from_node_data(node_data,
                                  assrtn_tgt_label,
                                  label_map_fname=None,
                                  metadata=None,
                                  md_sep=None):

    # create a text network
    tn = bg.TextNet(label_map_fname=label_map_fname, md_sep=md_sep)

    # the strings table contains a relation between a string ID and 
    # a node ID, so form the strings table by taking those pairs from
    # the node data
    strings = node_data.loc[:, ['node_id', 'string']]
    strings.drop_duplicates(inplace=True)
    strings.reset_index(drop=True, inplace=True)
    strings['string_id'] = strings.index.array

    # insert strings into the text network
    tn.append_to('strings', strings)
    # merge string IDs into the node data
    node_data = node_data.merge(tn.strings[['string', 'string_id']])

    # node_data columns are now:
    #       assrtn_tgt_label,
    #       input_label,
    #       string,
    #       link_type,
    #       node_type,
    #       node_id,
    #       string_id

    # insert nodes into the text network
    nodes = _make_new_nodes_table(tn, node_data)
    tn.append_to('nodes', nodes)

    # insert assertions into the text network
    assertions = _make_new_assertions_table(tn,
                                            node_data,
                                            assrtn_tgt_label)
    assertions['source_id'] = 0
    tn.append_to('assertions', assertions)

    # make edges from non-title assertions and insert them into the 
    # network
    title_link_type_id = tn.get_element_by_value('link_type',
                                                 'title',
                                                 'link_type_id')
    assrtn_not_title = (tn.assertions['link_type_id'] != title_link_type_id)
    edges = _make_edges_from_assertions(tn, assrtn_not_title)
    tn.append_to('edges', edges)

    if metadata is not None:

        contains_separator = metadata.astype(dict(zip(metadata.columns,
                                                      [str]*len(metadata.columns))))
        check_sep = lambda x: x.str.contains(tn.md_sep)
        contains_separator = contains_separator.apply(check_sep)
        if contains_separator.any().any():
            raise ValueError('Metadata values contain the string "{}", which is '
                             'the current separator for values in metadata '
                             'strings. Use the md_sep keyword or change the '
                             'source files to eliminate the conflict.'
                             .format(tn.md_sep))

        metadata = _normalize_metadata(tn,
                                       metadata,
                                       node_data,
                                       assrtn_tgt_label)

        for node_type,md_table in metadata.items():

            node_md_table = _make_new_node_md_table(tn,
                                                    node_type,
                                                    md_table)
            tn.append_to(node_type, node_md_table)

            node_md_table.dropna(how='all', inplace=True)
            columns_in_order = [c for c in tn.schema[node_type].columns
                                if c in node_md_table.columns]
            node_md_table = node_md_table.loc[:, columns_in_order]
            
            no_md = node_md_table[node_md_table.columns[2:]]
            no_md = no_md.isna().all().all()

            if node_md_table.empty or no_md:
                continue

            md_str_assrt = _make_md_strings_and_assrtns(tn,
                                                        node_md_table,
                                                        node_data,
                                                        assrtn_tgt_label)

            md_str_assrt[1]['source_id'] = 0

            tn.append_to('strings', md_str_assrt[0])
            tn.append_to('assertions', md_str_assrt[1])

    return tn