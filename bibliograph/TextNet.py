from ast import literal_eval
import bibliograph as bg
import pandas as pd


class AssertionsNotFoundError(AttributeError):
    pass


class NodesNotFoundError(AttributeError):
    pass


class IdLookupError(KeyError):
    pass


def concat_list_item_elements(list_elements, sort=False):

    sep = bg.util.get_single_value(list_elements, 'list_delimiter')
    item_label = bg.util.get_single_value(list_elements, 'item_label')

    if sort:
        item_string = sep.join(
            list_elements.sort_values(by='list_position')['tgt_string']
        )
    else:
        item_string = sep.join(list_elements['tgt_string'])

    item = pd.DataFrame(
        {
            'item_position': list_elements.name,
            'tgt_string': item_string,
            'item_label': item_label
        },
        index=[list_elements.index.min()]
    )

    return item


def prefix_targets(row):
    '''
    Takes a row from a dataframe representing one item in an entry. The
    string representation of the item is the target of a link between
    the entry string and the item. If the item has a prefix, add the
    prefix to the target string along with the prefix separator as
    defined in the syntax. Return the prefixed or unmodified string
    along with its position in the entry.

    Parameters
    ----------
    row : pandas Series
        Index includes the labels [
            'tgt_string',
            'item_label',
            'item_position',
            'item_prefix_separator'
        ]

    Returns
    -------
    pandas Series
        Has index ['item_position', 'tgt_string']
    '''

    if not str.isdigit(row['item_label']):
        components = row[['item_label', 'item_prefix_separator', 'tgt_string']]
        return pd.Series(
            [row['item_position'], components.sum(), row['entry_prefix']],
            index=['item_position', 'tgt_string', 'entry_prefix']
        )

    else:
        return row[['item_position', 'tgt_string', 'entry_prefix']]


def prefix_multiindex_column(column, prefix_separators):
    '''
    Takes a column from a multiindexed dataframe representing an entry.
    The multiindex (and therefore the column name) is the pair
    (item label, item position). If the item label for this column is a
    prefix then add the prefix with a separator to each string in the
    column. Otherwise return the column unchanged.

    Parameters
    ----------
    column : pandas.Series
        Should be one column out of a pandas.DataFrame whose column
        labels are a multiindex.

    prefix_separators : pandas.Series
        Map between item labels (the index) and prefix separators (the
        array)

    Returns
    -------
    pandas.Series
        Has same indexes as column
    '''

    if not str.isdigit(column.name[0]):
        sep = prefix_separators.loc[column.name[0]]
        return column.name[0] + sep + column

    else:
        return column


def collapse_columns(df, columns):

    if bg.util.iterable_not_string(columns):
        if len(columns) > 1:
            if not df[columns].count(axis=1).lt(2).all():
                raise ValueError(
                    'found multiple values in a single row for columns '
                    '{}'.format(columns)
                )
            return df[columns].ffill(axis=1).dropna(axis=1)

        elif len(columns) == 1:
            return pd.DataFrame(df[columns])

        else:
            return pd.DataFrame(dtype='object')

    elif (type(columns) == str):
        return df[columns]

    else:
        raise ValueError('unrecognized input value for columns')


def complete_entry_strings(
    entry_parts,
    input_metadata,
    fill_spaces,
    hide_default_entry_prefixes=False
):

    if 'syntax_string_id' in entry_parts.columns:

        input_metadata = dict(
            input_metadata.loc[bg.util.get_single_value(
                entry_parts,
                'syntax_string_id'
            )]
        )

        entry_parts = entry_parts.drop('syntax_string_id', axis='columns')

    if 'item_separator' not in input_metadata.keys():
        raise ValueError('input_metadata must include item_separator')

    sep = input_metadata['item_separator']

    entry_parts = entry_parts.apply(
        lambda x: x.str.replace(sep, '\\' + sep, regex=False)
    )

    if hide_default_entry_prefixes:
        default_entry_prefix = input_metadata['default_entry_prefix']
        not_dflt_entry = (entry_parts['entry_prefix'] != default_entry_prefix)
        entry_parts.loc[not_dflt_entry, '0'] = (
            entry_parts.loc[not_dflt_entry, 'entry_prefix'] +
            sep +
            entry_parts.loc[not_dflt_entry, '0']
        )

    else:
        has_prefix = entry_parts['entry_prefix'].notna()
        entry_parts.loc[has_prefix, '0'] = (
            entry_parts.loc[has_prefix, 'entry_prefix'] +
            sep +
            entry_parts.loc[has_prefix, '0']
        )

    entry_parts = entry_parts.drop('entry_prefix', axis='columns')

    if 'comment_char' in input_metadata.keys():
        entry_parts = entry_parts.apply(
            lambda x:
            x.str.replace(
                input_metadata['comment_char'],
                '\\' + input_metadata['comment_char'],
                regex=False
            )
        )

    entry_strings = entry_parts.apply(
        lambda x: sep.join(x.dropna()),
        axis='columns'
    )

    if fill_spaces and 'space_char' in input_metadata.keys():
        entry_strings = entry_strings.str.replace(
            ' ',
            input_metadata['space_char'],
            regex=False
        )

    return entry_strings


def synthesize_entries_from_links(
    group,
    tn,
    entry_syntax,
    name_type,
    prefix_label_map
):

    if name_type not in ['full', 'abbr']:
        raise ValueError(
            "name_type must be 'full' or 'abbr'. Got {}".format(name_type)
        )
    elif name_type == 'full':
        name_type = 'full_string_id'
    elif name_type == 'abbr':
        name_type = 'abbr_string_id'

    # Cache an empty return value
    return_cols = [
        'tgt_string',
        'item_label',
        'item_position',
        'item_prefix_separator'
    ]
    empty = pd.DataFrame(columns=return_cols)

    # The minimum number of links required for this source node to
    # represent an instance of the entry with the given syntax is
    # the number of unprefixed item positions in the entry syntax. They
    # are denoted with integer item labels. The entry_prefix.notna()
    # query excludes entry syntax rows that were added to account for
    # links to the null node.
    min_links = entry_syntax.query('entry_prefix.notna()')
    min_links = min_links.query('item_label.str.isdigit()')
    min_links = len(min_links)
    if len(group) < min_links:
        return empty

    # This source node might be an instance of an entry that should
    # have the requested prefix. To check, we need to get the node types
    # of the edge targets and compare them to the node types and link
    # types listed in the entry syntax.
    group = group.merge(
        tn.nodes['node_type_id'].rename('tgt_node_type_id'),
        left_on='tgt_node_id',
        right_index=True,
        copy=False
    )

    # Hash the (node type, link type) pairs in the syntax and the edge
    # group so we can treat the type pairs as single values.
    syntax_type_pairs = pd.util.hash_pandas_object(
        entry_syntax[['item_node_type', 'item_link_type']],
        index=False
    )
    group_type_pairs = pd.util.hash_pandas_object(
        group[['tgt_node_type_id', 'link_type_id']],
        index=False
    )

    # Replace the type columns in the input frames with the hashes
    entry_syntax['type_pair_hash'] = syntax_type_pairs.array
    group['type_pair_hash'] = group_type_pairs.array

    # The group must contain at least one type pair associated with each
    # item in the entry syntax to be an instance of an entry with the
    # requested prefix.
    type_pair_check = entry_syntax.groupby(by='item_position')
    # For each item position, check if any type pair from the syntax is
    # in the group.
    type_pair_check = type_pair_check.apply(
        lambda x: x['type_pair_hash'].isin(group['type_pair_hash']).any()
    )
    # If any of the items isn't represented in the group, return empty.
    if not type_pair_check.all():
        return empty

    # We now assume this source string is one that would have the
    # requested prefix.

    # Merge the edge group with the entry syntax so we have enough
    # information to get the edge targets in order
    relevant_syntax_columns = [
        'type_pair_hash',
        'list_delimiter',
        'item_position',
        'item_label',
        'item_prefix_separator'
    ]
    group = group.merge(entry_syntax[relevant_syntax_columns])

    # Item positions in this group should be duplicated in three cases:
    #     1. The item is prefixed and there are nodes available for
    #        multiple prefixes
    #     2. The entry has representations with missing and non-missing
    #        values at the same item position
    #     3. The entry has representations with different types of
    #        missing values at the same item position
    # Case 1 is handled elsewhere. Cases 2 and 3 are handled here.
    position_is_duplicated = group['item_position'].duplicated(keep=False)
    if position_is_duplicated.any():

        tgt_node_is_null_type = group['tgt_node_type_id'].isin(
            tn.get_null_node_type_ids()
        )
        duplct_positions_with_non_null_tgt = group.loc[
            position_is_duplicated & ~tgt_node_is_null_type,
            'item_position'
        ]
        at_dplct_position_with_non_null_tgt = group['item_position'].isin(
            duplct_positions_with_non_null_tgt
        )

        # Drop missing values in case 2 above.
        group = group.loc[
            ~(tgt_node_is_null_type & at_dplct_position_with_non_null_tgt)
        ]

    # For case 3 above, replace all null node types with default null
    # node type and keep one per item position if any are duplicated.
    # TODO allow configurable dafault_na_node_type
    position_is_duplicated = group['item_position'].duplicated(keep=False)
    if position_is_duplicated.any():

        null_node_type_ids = tn.get_null_node_type_ids()
        default_na_node_type_id = null_node_type_ids[0]

        tgt_node_is_not_default_na = (
            group['tgt_node_type_id'] != default_na_node_type_id
        )

        if tgt_node_is_not_default_na.any():

            tgt_node_is_null_type = group['tgt_node_type_id'].isin(
                null_node_type_ids
            )

            group.loc[tgt_node_is_null_type, 'tgt_node_type_id'] == (
                default_na_node_type_id
            )

            duplct_positions_with_non_null_tgt = group.loc[
                position_is_duplicated & ~tgt_node_is_null_type,
                'item_position'
            ]
            at_dplct_position_with_non_null_tgt = group['item_position'].isin(
                duplct_positions_with_non_null_tgt
            )
            at_dplct_position_with_only_null_tgts = (
                position_is_duplicated & ~at_dplct_position_with_non_null_tgt
            )
            is_first_duplicate = group['item_position'].duplicated()

            duplicate_is_null_to_keep = (
                at_dplct_position_with_only_null_tgts & is_first_duplicate
            )
            duplicate_is_null_to_drop = (
                position_is_duplicated &
                tgt_node_is_null_type &
                ~duplicate_is_null_to_keep
            )

            # Drop duplicate null items in case 3 above
            group = group.loc[~duplicate_is_null_to_drop]

    # Insert the names of edge targets into the group
    group['tgt_string'] = group['tgt_node_id'].map(tn.nodes[name_type])
    group.loc[:, 'tgt_string'] = group['tgt_string'].map(tn.strings['string'])

    # For any items that are lists of elements, concatenate the elements
    # using the list delimiter in the entry syntax. Start by looking for
    # items with defined list positions.
    list_items = group.query('list_position.notna()')

    if not list_items.empty:
        list_items = list_items.groupby(by='item_position', group_keys=False)
        list_items = list_items.apply(concat_list_item_elements, sort=True)

        group = pd.concat([group.query('list_position.isna()'), list_items])

    # For any item positions that are duplicated but have no defined
    # list positions, concatenate the item strings in the order they
    # appear in the group.
    position_is_duplicated = group['item_position'].duplicated(keep=False)
    position_is_not_prefixed = group['item_label'].str.isdigit()
    to_concat = position_is_duplicated & position_is_not_prefixed
    list_items = group.loc[to_concat]

    if not list_items.empty:
        list_items = list_items.groupby(by='item_position', group_keys=False)
        list_items = list_items.apply(concat_list_item_elements)

        group = pd.concat([group.loc[~to_concat], list_items])

    # The group of edges now has one row per item position except for
    # prefixed items. The group might contain links to multiple prefixed
    # items even though only one value per item can be represented in an
    # entry string. To print an entry, we select the first prefix in the
    # prefix list from the entry syntax that is present in the group of
    # links.

    # Insert the precedence of the prefixes in the group
    prefix_precedence = group['item_label'].map(
        pd.Series(
            range(len(prefix_label_map)),
            index=prefix_label_map.index,
            dtype=pd.Int32Dtype()
        )
    )
    group = pd.concat(
        [group, prefix_precedence.rename('prefix_precedence')],
        axis='columns'
    )

    # Select prefixed items out of the group
    selected_prefixed_items = group.query('~item_label.str.isdigit()')

    # If there are no prefixed items, return the target strings, their
    # item labels (which could be prefixes or numbers indicating entry
    # positions), their positions in the entry, and their prefix
    # separators
    if selected_prefixed_items.empty:
        return group[return_cols]

    # Group items by their position in the entry and select only the
    # item with the first prefix in the list of prefixes in the entry
    # syntax.
    selected_prefixed_items = selected_prefixed_items.groupby(
        by='item_position'
    )
    selected_prefixed_items = selected_prefixed_items.apply(
        lambda x:
        x.loc[x['prefix_precedence'].astype(pd.Int32Dtype()).idxmin()]
    )

    # Combine the unprefixed items with the selected prefixed items and
    # sort by their position in the entry
    group = pd.concat([
            group.query('item_label.str.isdigit()'),
            selected_prefixed_items
        ])
    group = group.sort_values(by='item_position')

    # Return the target strings, their item labels (which could be
    # prefixes or numbers indicating entry positions), their positions
    # in the entry, and their prefix separators
    return group[return_cols]


def synthesize_entries_by_prefix(
    tn,
    node_ids,
    entry_syntax,
    name_type
):

    '''********************
    PREP SYNTAX DEFINITIONS
    ********************'''

    # Items in an entry might be linked to the null node rather than
    # the target defined in the syntax. We only care about this when
    # checking what kind of links are present for an entry in the
    # parsed data, so we add rows to the entry syntax with every
    # link type paired with a null item node type
    null_node_type_ids = tn.get_null_node_type_ids()
    null_syntax = {
        null_type_id: entry_syntax.copy()[[
            'item_label',
            'item_node_type',
            'item_link_type',
            'list_delimiter'
        ]]
        for null_type_id in null_node_type_ids
    }
    for null_type_id, syntax in null_syntax.items():
        syntax['item_node_type'] = tn.node_types.loc[null_type_id, 'node_type']

    null_syntax = pd.concat(null_syntax.values())
    null_syntax = null_syntax.drop_duplicates()
    null_syntax = null_syntax.dropna(
        subset=['item_node_type', 'item_link_type']
    )
    entry_syntax = pd.concat([entry_syntax, null_syntax])

    # Make a map from positional index labels to any associated item
    # prefixes
    prefix_label_map = entry_syntax.query('item_prefixes.notna()')
    prefix_label_map = pd.Series(
        prefix_label_map['item_prefixes'].array,
        index=prefix_label_map['item_label'].array
    )
    prefix_label_map = prefix_label_map.str.split().explode()

    # Mutate it into a map from item prefixes to positional index
    # labels
    prefix_label_map = pd.Series(
        prefix_label_map.index,
        index=prefix_label_map
    )

    # Make a map from positional index labels to any associated item
    # prefix separators
    sep_label_map = entry_syntax.query('item_prefix_separator.notna()')
    sep_label_map = pd.Series(
        sep_label_map['item_prefix_separator'].array,
        index=sep_label_map['item_label'].array
    )
    sep_label_map = prefix_label_map.map(sep_label_map)

    # Use the separator map to propagate prefix separators from the
    # syntax rows for the positional items to the syntax rows for
    # the prefixed items.
    prefix_separators = entry_syntax['item_label'].map(sep_label_map)
    entry_syntax.loc[:, 'item_prefix_separator'] = prefix_separators.array

    # Make a new column to represent item positions within the entry
    # separately from the item labels
    entry_syntax = pd.concat(
        [
            entry_syntax,
            entry_syntax['item_label'].rename('item_position')
        ],
        axis='columns'
    )

    # Insert item positions for items whose labels are alphanumeric
    # prefixes rather than positions
    entry_syntax['item_position'].update(
        entry_syntax['item_label'].map(prefix_label_map)
    )

    # Convert string-valued node types to integer IDs
    node_type_id_map = pd.Series(
        tn.node_types.index,
        index=tn.node_types['node_type']
    )
    node_type_ids = entry_syntax['item_node_type'].map(node_type_id_map)
    entry_syntax.loc[:, 'item_node_type'] = node_type_ids.array

    # Convert string-valued link types to integer IDs
    link_type_id_map = pd.Series(
        tn.link_types.index,
        index=tn.link_types['link_type']
    )
    link_type_ids = entry_syntax['item_link_type'].map(link_type_id_map).array
    entry_syntax.loc[:, 'item_link_type'] = link_type_ids

    '''*********
    SYNTAX READY
    *********'''

    # Get the integer node type ID for this entry
    entry_node_type = bg.util.get_single_value(entry_syntax, 'entry_node_type')
    entry_node_type_id = tn.id_lookup('node_types', entry_node_type)

    # Get node IDs for nodes with the relevant type
    node_id_selection = tn.nodes.query('index.isin(@node_ids)')
    node_id_selection = tn.nodes.query('node_type_id == @entry_node_type_id')
    node_id_selection = node_id_selection.index

    # Select appropriate edges whose sources are the selected node IDs
    query1 = 'src_node_id.isin(@node_id_selection)'
    query2 = "link_type_id.isin(@entry_syntax['item_link_type'])"
    edge_selection = tn.edges.query(query1).query(query2)

    if edge_selection.empty:
        return pd.Series(dtype='object')

    # Reduce edge selection to relevant columns
    edge_selection = edge_selection[
        ['src_node_id', 'tgt_node_id', 'link_type_id']
    ]

    # Edge tags that are only digits should be interpreted as positions
    # in a list of target nodes with the same link type to the same
    # source. Get the list positions for the current edge set.
    tag_selection = tn.edge_tags.merge(
        tn.strings['string'],
        left_on='tag_string_id',
        right_index=True
    )
    tag_selection = tag_selection.query('string.str.isdigit()')

    if not tag_selection.empty:

        edge_selection = edge_selection.merge(
            tag_selection[['edge_id', 'string']],
            left_index=True,
            right_on='edge_id'
        )
        edge_selection = edge_selection.rename(
            columns={'string': 'list_position'}
        )
        edge_selection.loc[:, 'list_position'] = (
            edge_selection['list_position'].apply(int)
        )

    else:

        edge_selection['list_position'] = pd.NA

    # Group the links by source string. If a source has links like
    # the entry prefix in the syntax, generate strings representing
    # items for that entry.
    edge_selection = edge_selection.groupby(by='src_node_id')

    entry_parts = edge_selection.apply(
        synthesize_entries_from_links,
        tn,
        entry_syntax,
        name_type,
        prefix_label_map
    )

    entry_parts['entry_prefix'] = bg.util.get_single_value(
        entry_syntax,
        'entry_prefix',
        none_ok=True
    )

    return entry_parts


def synthesize_entries_by_syntax(
    tn,
    node_ids,
    syntax,
    name_type,
    entry_prefix
):

    node_types = tn.nodes.loc[node_ids, 'node_type_id']
    node_types = tn.node_types.loc[node_types, 'node_type']
    syntax = syntax.query(
        "entry_node_type.isin(@node_types)"
    )
    if entry_prefix is not None:
        if not bg.util.iterable_not_string(entry_prefix):
            entry_prefix = [entry_prefix]
        syntax = syntax.query('entry_prefix.isin(@entry_prefix)')

    prefix_syntaxes = [
        syntax.query('entry_prefix == @prefix')
        for prefix in syntax['entry_prefix'].unique()
    ]

    entry_parts = [
        synthesize_entries_by_prefix(
            tn,
            node_ids,
            entry_syntax,
            name_type
        )
        for entry_syntax in prefix_syntaxes
    ]

    return pd.concat(entry_parts)


class TextNet():

    '''def __init__(
        self,
        assertions=None,
        strings=None,
        nodes=None,
        edges=None,
        node_types=None,
        link_types=None,
        assertion_tags=None,
        edge_tags=None,
        node_metadata_tables=None,
        big_id_dtype=pd.Int32Dtype(),
        small_id_dtype=pd.Int8Dtype()
    ):
        self.assertions = assertions
        self.strings = strings
        self.nodes = nodes
        self.edges = edges
        self.node_types = node_types
        self.link_types = link_types
        self.assertion_tags = assertion_tags
        self.edge_tags = edge_tags
        node_metadata_tables = node_metadata_tables'''

    def __init__(
        self,
        big_id_dtype=pd.Int32Dtype(),
        small_id_dtype=pd.Int8Dtype()
    ):

        self.big_id_dtype = big_id_dtype
        self.small_id_dtype = small_id_dtype

        self._string_side_tables = [
            'strings', 'assertions', 'link_types', 'assertion_tags'
        ]
        self._node_side_tables = [
            'nodes', 'edges', 'node_types', 'edge_tags'
        ]

        self._assertions_dtypes = {
            'inp_string_id': self.big_id_dtype,
            'src_string_id': self.big_id_dtype,
            'tgt_string_id': self.big_id_dtype,
            'ref_string_id': self.big_id_dtype,
            'link_type_id': self.small_id_dtype,
            'date_inserted': 'object',
            'date_modified': 'object'
        }
        self._assertions_index_dtype = self.big_id_dtype

        self._strings_dtypes = {
            'node_id': self.big_id_dtype,
            'string': 'object',
            'date_inserted': 'object',
            'date_modified': 'object'
        }
        self._strings_index_dtype = self.big_id_dtype

        self._nodes_dtypes = {
            'node_type_id': self.small_id_dtype,
            'name_string_id': self.big_id_dtype,
            'abbr_string_id': self.big_id_dtype,
            'date_inserted': 'object',
            'date_modified': 'object'
        }
        self._nodes_index_dtype = self.big_id_dtype

        self._edges_dtypes = {
            'src_node_id': self.big_id_dtype,
            'tgt_node_id': self.big_id_dtype,
            'ref_node_id': self.big_id_dtype,
            'link_type_id': self.small_id_dtype,
            'date_inserted': 'object',
            'date_modified': 'object'
        }
        self._edges_index_dtype = self.big_id_dtype

        self._node_types_dtypes = {
            'node_type': 'object',
            'description': 'object',
            'null_type': bool
        }
        self._node_types_index_dtype = self.small_id_dtype

        self._link_types_dtypes = {
            'link_type': 'object',
            'description': 'object',
            'null_type': bool
        }
        self._link_types_index_dtype = self.small_id_dtype

        self._assertion_tags_dtypes = {
            'assertion_id': self.big_id_dtype,
            'tag_string_id': self.big_id_dtype
        }
        self._assertion_tags_index_dtype = self.big_id_dtype

        self._edge_tags_dtypes = {
            'edge_id': self.big_id_dtype,
            'tag_string_id': self.big_id_dtype
        }
        self._edge_tags_index_dtype = self.big_id_dtype

    def __getattr__(self, attr):

        try:

            return self.__getattribute__(attr)

        except AttributeError as error:

            if attr in self._string_side_tables:
                raise AssertionsNotFoundError(
                    'assertions and strings not initialized for '
                    'this TextNet'
                )

            elif attr in self._node_side_tables:
                raise NodesNotFoundError(
                    'nodes and edges not initialized for '
                    'this TextNet'
                )

            else:
                raise error

    def _insert_type(
        self,
        name,
        description,
        null_type,
        node_or_link,
        overwrite_description
    ):

        table_name = node_or_link + '_types'
        column_name = node_or_link + '_type'
        existing_table = self.__getattr__(table_name)

        if name in existing_table[column_name].array:

            if pd.notna(description) and overwrite_description:
                existing_row = (existing_table[column_name] == name)
                existing_table.loc[existing_row, 'description'] = description
                self.__setattr__(table_name, existing_table)

            return self.id_lookup(table_name, name)

        else:

            new_row = {
                column_name: name,
                'description': description,
                'null_type': bool(null_type)
            }
            new_row = bg.util.normalize_types(new_row, existing_table)

            self.__setattr__(table_name, pd.concat([existing_table, new_row]))
            self._reset_table_dtypes(table_name)

            return new_row.index[0]

    def _get_null_type_ids(self, table_name):
        table = self.__getattr__(table_name)
        return table.loc[table['null_type']].index

    def _reset_table_dtypes(self, table_name):

        table_dtypes = self.__getattr__('_{}_dtypes'.format(table_name))

        try:
            self.nodes
            pass

        except NodesNotFoundError:

            if table_name == 'strings':
                table_dtypes = {
                    k: v for k, v in table_dtypes.items() if k != 'node_id'
                }
                table_dtypes['node_type_id'] = self.small_id_dtype

        table = self.__getattr__(table_name)

        optional_cols = ['date_inserted', 'date_modified']
        drop_cols = [c for c in optional_cols if c not in table.columns]

        table_dtypes = {
            k: v for k, v in table_dtypes.items() if k not in drop_cols
        }

        index_dtype = self.__getattr__('_{}_index_dtype'.format(table_name))

        table = table.astype(table_dtypes)
        table.index = table.index.astype(index_dtype)
        table = table[table_dtypes.keys()]
        table = table.fillna(pd.NA)

        self.__setattr__(table_name, table)

    def get_assertions_by_link_type_id(self, link_type_id, subset=None):

        if not bg.util.iterable_not_string(link_type_id):
            link_type_id = [link_type_id]

        if subset is None:
            return self.assertions.query('link_type_id.isin(@link_type_id)')

        else:
            subset_index = self.assertions.loc[subset].index
            query1 = 'link_type_id.isin(@link_type_id)'
            query2 = 'index.isin(@subset_index)'
            return self.assertions.query(query1).query(query2)

    def get_assertions_by_link_type(
        self,
        link_type,
        subset=None,
        allow_missing_type=True
    ):

        try:
            return self.get_assertions_by_link_type_id(
                self.id_lookup('link_types', link_type),
                subset
            )

        except IdLookupError:
            if allow_missing_type:
                return pd.DataFrame(columns=self.assertions.columns)
            else:
                raise

    def get_assertions_by_string_id(
        self,
        string_ids,
        component=None,
        subset=None,
        output_mode=None
    ):

        if output_mode is not None:
            if output_mode not in ['boolean_mask', 'id_map']:
                raise ValueError(
                    'output_mode must be one of {}. Got {}.'
                    .format([None, 'boolean_mask', 'id_map'], output_mode)
                )

        if not bg.util.iterable_not_string(string_ids):
            string_ids = [string_ids]

        if component is None:
            component = ['inp', 'src', 'tgt', 'ref']

        if bg.util.iterable_not_string(component):

            component_masks = {
                c: self.assertions['{}_string_id'.format(c)].isin(string_ids)
                for c in component
            }

            if subset is not None:

                subset_mask = self.assertions.index.isin(
                    self.assertions.loc[subset].index
                )
                component_masks = {
                    c: mask & subset_mask
                    for c, mask in component_masks.items()
                }

            if output_mode is not None:

                if output_mode == 'boolean_mask':
                    return component_masks

                elif output_mode == 'id_map':
                    return {
                        lbl: self.assertions.loc[
                            mask,
                            '{}_string_id'.format(lbl)
                        ]
                        for lbl, mask in component_masks.items()
                    }

                else:
                    raise ValueError(
                        '{} is not a valid value for output_mode'
                        .format(output_mode)
                    )

            else:
                return {
                    lbl: self.assertions.loc[mask]
                    for lbl, mask in component_masks.items()
                }

        else:

            label = '{}_string_id'.format(component)
            component_mask = self.assertions[label].isin(string_ids)

            if subset is not None:

                subset_mask = self.assertions.index.isin(
                    self.assertions.loc[subset].index
                )
                component_mask = component_mask & subset_mask

            if output_mode is not None:

                if output_mode == 'boolean_mask':
                    return component_mask

                elif output_mode == 'id_map':

                    return self.assertions.loc[
                        component_mask,
                        '{}_string_id'.format(label)
                    ]

                else:
                    raise ValueError(
                        '{} is not a valid value for output_mode'
                        .format(output_mode)
                    )

            else:
                return self.assertions.loc[component_mask]

    def get_assertions_by_inp_string_id(self, string_ids, output_mode=None):
        return self.get_assertions_by_string_id(
            string_ids=string_ids,
            component='inp',
            output_mode=output_mode
        )

    def get_assertions_by_src_string_id(self, string_ids, output_mode=None):
        return self.get_assertions_by_string_id(
            string_ids=string_ids,
            component='src',
            output_mode=output_mode
        )

    def get_assertions_by_tgt_string_id(self, string_ids, output_mode=None):
        return self.get_assertions_by_string_id(
            string_ids=string_ids,
            component='tgt',
            output_mode=output_mode
        )

    def get_assertions_by_ref_string_id(self, string_ids, output_mode=None):
        return self.get_assertions_by_string_id(
            string_ids=string_ids,
            component='ref',
            output_mode=output_mode
        )

    def get_assertions_by_node_type(
        self,
        node_type,
        component,
        subset=None,
        allow_missing_type=True
    ):

        try:
            node_type_id = self.id_lookup('node_types', node_type)

        except IdLookupError:
            if allow_missing_type:
                return pd.DataFrame(columns=self.assertions.columns)
            else:
                raise

        if not component.endswith('_string_id'):
            component = component + '_string_id'

        try:

            node_subset = self.nodes.query('node_type_id == @node_type_id')
            string_subset = self.strings.query(
                'node_id.isin(@node_subset.index)'
            )

        except NodesNotFoundError:

            string_subset = self.strings.query('node_type_id == @node_type_id')

        if subset is None:

            return self.assertions.query(
                '{}.isin(@string_subset.index)'.format(component)
            )

        else:

            return self.assertions.loc[subset].query(
                '{}.isin(@string_subset.index)'.format(component)
            )

    def get_assertions_by_inp_node_type(
        self,
        node_type,
        subset=None,
        allow_missing_type=True
    ):
        return self.get_assertions_by_node_type(node_type, 'inp', subset)

    def get_assertions_by_src_node_type(
        self,
        node_type,
        subset=None,
        allow_missing_type=True
    ):
        return self.get_assertions_by_node_type(node_type, 'src', subset)

    def get_assertions_by_tgt_node_type(
        self,
        node_type,
        subset=None,
        allow_missing_type=True
    ):
        return self.get_assertions_by_node_type(node_type, 'tgt', subset)

    def get_assertions_by_ref_node_type(
        self,
        node_type,
        subset=None,
        allow_missing_type=True
    ):
        return self.get_assertions_by_node_type(node_type, 'ref', subset)

    def get_assertions_by_literal(self, literal, column, subset=None):

        literal_types = [
            t for t in self.node_types['node_type'] if t.startswith('_literal')
        ]
        literal_type_id = self.id_lookup(
            'node_types',
            literal_types,
            return_scalar=False
        )

        if subset is None:
            component = self.strings.loc[self.assertions[column]]

        else:
            component = self.strings.loc[self.assertions.loc[subset, column]]

        try:
            self.nodes
            component_is_literal = (
                component['node_id'].map(self.nodes['node_type_id']).isin(
                    literal_type_id
                )
            )
        except NodesNotFoundError:
            component_is_literal = (
                component['node_type_id'].isin(literal_type_id)
            )
        component = component.loc[component_is_literal]
        component.loc[:, 'string'] = component['string'].map(literal_eval)

        matches_input = component['string'] == literal

        component_literal_types = component['string'].map(type)
        matches_input_type = component_literal_types == type(literal)

        component = component.loc[matches_input & matches_input_type].index

        return self.assertions.query('{}.isin(@component)'.format(column))

    def get_assertions_by_literal_tgt(self, literal, subset=None):
        column = 'tgt_string_id'
        return self.get_assertions_by_literal(literal, column, subset)

    def get_assertion_ids_by_node_id(
        self,
        node_ids,
        component=None
    ):

        if not bg.util.iterable_not_string(node_ids):
            node_ids = [node_ids]

        strings_to_nodes = self.strings.query('node_id.isin(@node_ids)')
        strings_to_nodes = strings_to_nodes['node_id']

        strings_to_assertions = self.get_assertions_by_string_id(
            strings_to_nodes.index,
            component=component,
            output_mode='id_map'
        )
        strings_to_assertions = pd.concat(strings_to_assertions.values())
        strings_to_assertions = pd.Series(
            strings_to_assertions.index,
            index=strings_to_assertions.array
        )

        hashed = pd.util.hash_pandas_object(
            strings_to_assertions.reset_index(),
            index=False
        )
        strings_to_assertions = strings_to_assertions.loc[
            ~hashed.duplicated().array
        ]

        output = {
            'assertion_id': strings_to_assertions.array,
            'node_id': strings_to_assertions.index.map(strings_to_nodes).array
        }
        output = pd.DataFrame(output, index=strings_to_assertions.index)

        return pd.Series(
            output['assertion_id'].array,
            index=output['node_id'].array
        )

    def get_link_syntaxes_by_input_string_id(self, inp_string_ids):

        has_relevant_input_string = self.assertions['inp_string_id'].isin(
            inp_string_ids
        )

        assertions_to_link_syntax = self.get_assertions_by_tgt_node_type(
            'shorthand_link_syntax',
            subset=has_relevant_input_string
        )
        assertions_to_entry_syntax = self.get_assertions_by_tgt_node_type(
            'shorthand_entry_syntax',
            subset=has_relevant_input_string
        )

        missing_entry_sntx = ~assertions_to_link_syntax['inp_string_id'].isin(
            assertions_to_entry_syntax['inp_string_id']
        )
        if missing_entry_sntx.any():
            raise ValueError(
                'Cannot parse link syntax without associated entry syntax'
            )

        assertions_to_link_syntax = assertions_to_link_syntax.sort_values(
            by='inp_string_id'
        )
        assertions_to_entry_syntax = assertions_to_entry_syntax.sort_values(
            by='inp_string_id'
        )

        link_syntax_metadata = self.get_node_metadata_by_string_id(
            assertions_to_link_syntax['tgt_string_id']
        )
        entry_syntax_metadata = self.get_node_metadata_by_string_id(
            assertions_to_entry_syntax['tgt_string_id']
        )

        entry_syntaxes = [
            self.strings.loc[str_id, 'string']
            for str_id in assertions_to_entry_syntax['tgt_string_id']
        ]
        link_syntaxes = [
            self.strings.loc[str_id, 'string']
            for str_id in assertions_to_link_syntax['tgt_string_id']
        ]

        link_syntaxes = [
            bg.syntax_parsing.parse_link_syntax(
                ls,
                entry_syntaxes[i],
                case_sensitive=(
                    link_syntax_metadata.iloc[i]['case_sensitive'].squeeze()
                )
            )
            for i, ls in enumerate(link_syntaxes)
        ]

        entry_syntaxes = [
            bg.syntax_parsing.validate_entry_syntax(
                s,
                case_sensitive=(
                    entry_syntax_metadata.iloc[i]['case_sensitive'].squeeze()
                )
            )
            for i, s in enumerate(entry_syntaxes)
        ]

        syntaxes = pd.DataFrame({
            'link_syntax': [item[1] for item in link_syntaxes],
            'link_types': [item[0] for item in link_syntaxes],
            'entry_syntax': entry_syntaxes,
            'inp_string_id': assertions_to_link_syntax['inp_string_id'].array
        })

        return syntaxes

    def get_null_link_type_ids(self):
        return self._get_null_type_ids('link_types')

    def get_null_node_type_ids(self):
        return self._get_null_type_ids('node_types')

    def get_nodes_with_null_types(self):
        null_type_ids = self.get_null_node_type_ids()
        return self.nodes.loc[self.nodes['node_type_id'].isin(null_type_ids)]

    def get_links_with_null_types(self):
        null_type_ids = self.get_null_link_type_ids()
        return self.links.loc[self.links['link_type_id'].isin(null_type_ids)]

    def get_node_types_by_node_id(self, node_ids):
        return self.nodes.loc[node_ids, 'node_type_id'].map(
            self.node_types['node_type']
        )

    def get_node_types_by_string_id(self, string_ids):
        node_ids = self.strings.loc[string_ids, 'node_id']
        return self.nodes.loc[node_ids, 'node_type_id'].map(
            self.node_types['node_type']
        )

    def get_input_metadata_assertions_by_inp_string_id(self, inp_string_id):

        if not bg.util.iterable_not_string(inp_string_id):
            inp_string_id = [inp_string_id]

        query1 = 'inp_string_id == src_string_id'
        query2 = 'inp_string_id == ref_string_id'
        query3 = 'inp_string_id.isin(@inp_string_id)'

        metadata_assertions = self.assertions.query(query1)
        metadata_assertions = metadata_assertions.query(query2)
        metadata_assertions = metadata_assertions.query(query3)

        return metadata_assertions

    def get_input_metadata_assertions_by_string_id(
        self,
        string_id,
        component=['src', 'tgt', 'ref']
    ):

        if not bg.util.iterable_not_string(string_id):
            string_id = [string_id]

        if not bg.util.iterable_not_string(component):
            component = [component]

        if any([c not in ['src', 'tgt', 'ref'] for c in component]):
            raise ValueError(
                "component can only contain values 'src', 'tgt', or "
                "'ref'. Got {}".format(component)
            )

        component_query = '({})'.format(')|('.join([
            '{}_string_id.isin(@string_id)'.format(c) for c in component
        ]))
        inp_strings = self.assertions.query(component_query)['inp_string_id']
        inp_strings = inp_strings.unique()

        return self.get_input_metadata_assertions_by_inp_string_id(inp_strings)

    def get_input_metadata_assertions_by_node_id(
        self,
        node_id,
        component=['src', 'tgt', 'ref']
    ):

        if not bg.util.iterable_not_string(node_id):
            node_id = [node_id]

        string_id = self.get_strings_by_node_id(node_id, string_ids_only=True)

        return self.get_input_metadata_assertions_by_string_id(
            string_id,
            component
        )

    def get_strings_by_node_id(self, node_id, string_ids_only=False):

        if not bg.util.iterable_not_string(node_id):
            node_id = [node_id]

        if string_ids_only:
            return self.strings.index[
                self.strings['node_id'].isin(node_id)
            ]

        else:
            return self.strings.query('node_id.isin(@node_id)')

    def get_strings_by_node_type(self, node_type, string_ids_only=False):

        if isinstance(node_type, str):
            node_type = [node_type]
            node_type_id = None

        elif bg.util.iterable_not_string(node_type):
            if isinstance(node_type[0], str):
                node_type_id = None
            else:
                node_type_id = node_type

        else:
            node_type_id = [node_type]

        if node_type_id is None:
            node_type_id = self.node_types.query('node_type.isin(@node_type)')
            node_type_id = node_type_id.index

        try:
            node_ids = self.nodes.query('node_type_id.isin(@node_type_id)')
            node_ids = node_ids.index
            output = self.strings.query('node_id.isin(@node_ids)')
        except NodesNotFoundError:
            output = self.strings.query('node_type_id.isin(@node_type_id)')

        if string_ids_only:
            return output.index

        return output

    def insert_link_type(
        self,
        name,
        description=pd.NA,
        null_type=False,
        overwrite_description=False
    ):
        return self._insert_type(
            name,
            description,
            null_type,
            'link',
            overwrite_description
        )

    def insert_node_type(
        self,
        name,
        description=pd.NA,
        null_type=False,
        overwrite_description=False
    ):
        return self._insert_type(
            name,
            description,
            null_type,
            'node',
            overwrite_description
        )

    def insert_string(
        self,
        string,
        node_type,
        date_inserted=None,
        add_node_type=False,
        allow_duplicates=False,
        return_scalar=None,
        node_type_description=None,
        node_type_is_null=False,
        overwrite_type_description=False
    ):

        err_message = (
            "Cannot add node types from an array. If node_type is "
            "iterable, all values must exist in "
            "TextNet.node_types['node_type'] or "
            "TextNet.node_types.index"
        )

        if isinstance(node_type, str):

            try:
                node_type_id = self.id_lookup('node_types', node_type)

            except KeyError as e:
                if any([node_type in v for v in e.args]):
                    if add_node_type:
                        node_type_id = self.insert_node_type(
                            node_type,
                            description=node_type_description,
                            null_type=node_type_is_null,
                            overwrite_description=overwrite_type_description
                        )
                    else:
                        raise e
                else:
                    raise e

        elif bg.util.iterable_not_string(node_type):

            num_strings = len(string)
            num_types = len(node_type)
            if num_strings != num_types:
                raise IndexError(
                    'string and node_type must have same length if '
                    'iterable. len(string)=={}, len(node_type)={}'
                    .format(num_strings, num_types)
                )

            if pd.api.types.is_integer_dtype(node_type):
                node_type_id = pd.Series(node_type)

                uniques = pd.Series(node_type_id.unique())
                if not uniques.isin(self.node_types.index).all():
                    raise ValueError(err_message)

                node_type_id = node_type_id.array

            else:
                uniques = pd.Series(pd.Series(node_type).unique())
                if not uniques.isin(self.node_types['node_type']).all():
                    raise ValueError(err_message)

                node_type_id = pd.Series(node_type).map(
                    pd.Series(
                        self.node_types.index,
                        index=self.node_types['node_type']
                    )
                )
                node_type_id = node_type_id.array

        else:

            node_type_id = node_type

        if not bg.util.iterable_not_string(string):
            string = [string]
            if return_scalar is None:
                return_scalar = True

        elif return_scalar:
            raise ValueError(
                'Cannot return scalar index value for non-scalar input'
            )

        else:
            return_scalar = False

        string = pd.Series(string)
        string_exists = string.isin(self.strings['string'])
        handle_existing = string_exists.any()
        duplicate_input = string.duplicated().any()

        try:
            self.nodes

            raise NotImplementedError(
                'cannot yet insert strings in TextNets with nodes'
            )

        except NodesNotFoundError:

            err_message = (
                'Attempting to insert duplicate strings with '
                'allow_duplicates=False'
            )

            duplicates = handle_existing or duplicate_input

            if (allow_duplicates is False) and duplicates:
                raise ValueError(err_message)

            elif (allow_duplicates == 'suppress'):

                if duplicate_input:
                    string = pd.Series(string.unique())

                if handle_existing:

                    existing = string.loc[string_exists]
                    string = string.loc[~string_exists]

                    if bg.util.iterable_not_string(node_type):
                        node_type = pd.Series(node_type)
                        node_type = node_type.loc[~string_exists]

            idx = range(len(string))

            new_strings = pd.DataFrame(
                {
                    'string': string.array,
                    'node_type_id': node_type_id
                },
                index=idx
            )

            if date_inserted is not None:
                new_strings['date_inserted'] = date_inserted

        new_strings = bg.util.normalize_types(new_strings, self.strings)

        self.strings = pd.concat([self.strings, new_strings])
        self.reset_strings_dtypes()

        if return_scalar:
            return new_strings.index[0]

        elif handle_existing:
            idx = pd.Series(range(len(string) + len(existing)))
            idx.loc[~string_exists] = new_strings.index.array
            existing_ids = self.id_lookup(
                'strings',
                existing,
                return_scalar=False
            )
            idx.loc[string_exists] = existing_ids.array

            return idx

        else:
            return new_strings.index

    def insert_assertion(
        self,
        inp,
        src,
        tgt,
        ref,
        link_type,
        date_inserted=None
    ):

        components = [inp, src, tgt, ref, link_type]

        is_string = list(map(lambda x: isinstance(x, str), components))
        all_strings = all(is_string)
        any_strings = any(is_string)

        if any_strings:
            if not all_strings:
                raise NotImplementedError(
                    'cannot handle string values and integer IDs at '
                    'the same time. Parameters [inp, src, tgt, ref, '
                    'link_type] must be all strings or all non-strings.'
                )
            else:
                inp = self.id_lookup('strings', inp)
                src = self.id_lookup('strings', src)
                tgt = self.id_lookup('strings', tgt)
                ref = self.id_lookup('strings', ref)
                link_type = self.id_lookup('link_types', link_type)

        is_iterable = list(map(bg.util.iterable_not_string, components))
        if any(is_iterable):
            lengths = [
                len(v) for i, v in enumerate(components) if is_iterable[i]
            ]
            idx = range(max(lengths))
        else:
            idx = [0]

        new_assertions = pd.DataFrame(
            {
                'inp_string_id': inp,
                'src_string_id': src,
                'tgt_string_id': tgt,
                'ref_string_id': ref,
                'link_type_id': link_type,
            },
            index=idx
        )

        if date_inserted is not None:
            new_assertions['date_inserted'] = date_inserted

        new_assertions = bg.util.normalize_types(
            new_assertions,
            self.assertions
        )

        self.assertions = pd.concat([self.assertions, new_assertions])

        self.reset_assertions_dtypes()

    def id_lookup(self, attr, string, column_label=None, return_scalar=True):
        '''
        Take the name of an attribute of ParsedShorthand. If the
        attribute is a pandas Series or DataFrame, return the numerical
        index of the input string within the attribute.

        Parameters
        ----------
        attr : str
            Name of an attribute of TextNet

        string : str
            String value to retrieve index for

        column_label : str, default 'string'
            Column label to index if ParsedShorthand.attr is a
            DataFrame. Default to 'string' because at the moment
            ParsedShorthand.strings is the only DataFrame available.
        '''

        attribute = self.__getattr__(attr)

        default_columns = {
            'strings': 'string',
            'node_types': 'node_type',
            'link_types': 'link_type'
        }

        if not bg.util.iterable_not_string(string):
            string = [string]

        string = pd.Series(string)

        try:
            # If this assertion passes, assume attribute is a Series
            assert attribute.str

            missing = ~string.isin(attribute)
            if missing.any():
                raise KeyError(
                    'None of {} found in TextNet.{}'
                    .format(string.loc[missing], attr)
                )

            selection = attribute.isin(string)

        except AttributeError:
            # Otherwise assume attribute is a DataFrame
            if column_label is None:

                if attr not in default_columns.keys():
                    raise ValueError(
                        'Must use column_label keyword when indexing {}'
                        .format(attr)
                    )

                else:
                    column_label = default_columns[attr]

            missing = ~string.isin(attribute[column_label])
            if missing.any():
                raise IdLookupError(
                    "None of {} found in TextNet.{}['{}']"
                    .format(list(string.loc[missing]), attr, column_label)
                )

            selector = attribute[column_label].isin(string)
            selection = attribute.loc[selector, column_label]

        length = len(selection.array)

        if length == 1 and return_scalar:
            return selection.index[0]
        else:
            return selection.index

    def map_string_id_to_node_type(self, string_id):

        try:
            int(string_id)

            if 'node_type_id' in self.strings.columns:
                output = self.node_types.loc[
                    self.strings.loc[string_id, 'node_type_id'],
                    'node_type'
                ]
            else:
                node_type_id = self.nodes.loc[
                    self.strings.loc[string_id, 'node_id'],
                    'node_type_id'
                ]
                output = self.node_types.loc[node_type_id, 'node_type']

            return output.iloc[0]

        except TypeError:

            if 'node_type_id' in self.strings.columns:
                output = string_id.map(self.strings['node_type_id'])
                return output.map(self.node_types['node_type'])

            else:
                output = string_id.map(self.strings['node_id'])
                output = output.map(self.nodes['node_type_id'])
                return output.map(self.node_types['node_type'])

    def reset_assertions_dtypes(self):
        self._reset_table_dtypes('assertions')

    def reset_strings_dtypes(self):
        self._reset_table_dtypes('strings')

    def reset_nodes_dtypes(self):
        self._reset_table_dtypes('nodes')

    def reset_edges_dtypes(self):
        self._reset_table_dtypes('edges')

    def reset_node_types_dtypes(self):
        self._reset_table_dtypes('node_types')

    def reset_link_types_dtypes(self):
        self._reset_table_dtypes('link_types')

    def reset_assertion_tags_dtypes(self):
        self._reset_table_dtypes('assertion_tags')

    def reset_edge_tags_dtypes(self):
        self._reset_table_dtypes('edge_tags')

    def resolve_assertions(
        self,
        include_node_types=True,
        tags=True,
        subset=None,
        link_type=None
    ):
        '''
        Get a copy of the assertions frame with all integer ID elements
        replaced by the string values they represent

        Parameters
        ----------
        node_types : bool, default True
            If True, add columns for the node types of the source,
            target, and reference strings (three additional columns).

        tags : bool, default True
            If True, add a column for assertion tags. All tags for a
            link are joined into a single string separated by spaces.

        Returns
        -------
        pandas.DataFrame
        '''

        assertions = self.assertions
        string_map = self.strings['string']
        lt_map = self.link_types['link_type']

        if subset is not None:
            assertions = assertions.loc[subset]

        if link_type is not None:
            link_type_id = self.id_lookup(
                'link_types',
                link_type,
                return_scalar=False
            )
            assertions = assertions.query('link_type_id.isin(@link_type_id)')

        if len(assertions.shape) == 1:
            assertions = pd.DataFrame(
                dict(assertions),
                index=[assertions.name]
            )

        resolved = pd.DataFrame(
            {'inp_string': assertions['inp_string_id'].map(string_map),
             'src_string': assertions['src_string_id'].map(string_map),
             'tgt_string': assertions['tgt_string_id'].map(string_map),
             'ref_string': assertions['ref_string_id'].map(string_map),
             'link_type': assertions['link_type_id'].map(lt_map)},
            index=assertions.index
        )

        other_cols = [
            c for c in assertions.columns if c not in resolved.columns
        ]
        resolved[other_cols] = assertions[other_cols].to_numpy()

        if include_node_types:
            resolved['inp_node_type'] = self.map_string_id_to_node_type(
                assertions['inp_string_id']
            )
            resolved['src_node_type'] = self.map_string_id_to_node_type(
                assertions['src_string_id']
            )
            resolved['tgt_node_type'] = self.map_string_id_to_node_type(
                assertions['tgt_string_id']
            )
            resolved['ref_node_type'] = self.map_string_id_to_node_type(
                assertions['ref_string_id']
            )

        if tags is True:
            # Resolve link tags as space-delimited lists
            tags = self.assertion_tags.groupby('assertion_id')
            tags = tags.apply(
                lambda x:
                ' '.join(self.strings.loc[x['tag_string_id'], 'string'])
            )

            resolved = resolved.join(tags.rename('tags'))

        return resolved.fillna(pd.NA)

    def resolve_edges(
        self,
        string_type='name',
        include_node_types=True,
        tags=True,
        subset=None,
        link_type=None
    ):
        '''
        Get a copy of the edges frame with all integer ID elements
        replaced by the string values they represent

        Parameters
        ----------
        node_types : bool, default True
            If True, add columns for the node types of the source,
            target, and reference strings (three additional columns).

        tags : bool, default True
            If True, add a column for assertion tags. All tags for a
            link are joined into a single string separated by spaces.

        Returns
        -------
        pandas.DataFrame
        '''

        if subset is None:
            edges = self.edges
        else:
            subset = {
                k: v
                if bg.util.iterable_not_string(v)
                else [v]
                for k, v in subset.items()
            }
            query = '({})'.format(')|('.join([
                '{0}_node_id.isin(@subset["{0}"])'
                .format(k) for k in subset.keys()
            ]))
            edges = self.edges.query(query)

        if link_type is not None:
            link_type_id = self.id_lookup(
                'link_types',
                link_type,
                return_scalar=False
            )
            edges = edges.query('link_type_id.isin(@link_type_id)')

        lt_map = self.link_types['link_type']

        def map_to_strings(node_ids):
            string_ids = node_ids.map(self.nodes[string_type + '_string_id'])
            return string_ids.map(self.strings['string'])

        resolved = pd.DataFrame(
            {'src_string': map_to_strings(edges['src_node_id']),
             'tgt_string': map_to_strings(edges['tgt_node_id']),
             'ref_string': map_to_strings(edges['ref_node_id']),
             'link_type': edges['link_type_id'].map(lt_map),
             'date_inserted': edges['date_inserted'],
             'date_modified': edges['date_modified']},
            index=edges.index
        )

        if include_node_types:
            resolved['src_node_type'] = self.get_node_types_by_node_id(
                edges['src_node_id']
            ).array
            resolved['tgt_node_type'] = self.get_node_types_by_node_id(
                edges['tgt_node_id']
            ).array
            resolved['ref_node_type'] = self.get_node_types_by_node_id(
                edges['ref_node_id']
            ).array

        if tags is True and not self.edge_tags.empty:
            # Resolve link tags as space-delimited lists
            tags = self.edge_tags.groupby('edge_id')
            tags = tags.apply(
                lambda x:
                ' '.join(self.strings.loc[x['tag_string_id'], 'string'])
            )

            resolved = pd.concat(
                [resolved, tags.rename('tags')],
                axis='columns'
            )

        return resolved.fillna(pd.NA)

    def resolve_nodes(self, subset=None, node_type=None):
        '''
        Get a copy of the nodes frame with all integer ID elements
        replaced by the string values they represent

        Returns
        -------
        pandas.DataFrame
            Same shape and index as the strings frame.
        '''

        if subset is None:
            nodes = self.nodes
        else:
            nodes = self.nodes.loc[subset]
            if len(nodes.shape) == 1:
                nodes = pd.DataFrame(dict(nodes), index=[nodes.name])

        if node_type is not None:
            node_type_id = self.id_lookup(
                'node_types',
                node_type,
                return_scalar=False
            )
            nodes = nodes.loc[nodes['node_type_id'].isin(node_type_id)]

        node_types = nodes['node_type_id'].map(self.node_types['node_type'])
        name_strings = nodes['name_string_id'].map(self.strings['string'])
        abbr_strings = nodes['abbr_string_id'].map(self.strings['string'])

        resolved = pd.DataFrame(
            {'node_type': node_types,
             'name_string': name_strings,
             'abbr_string': abbr_strings,
             'date_inserted': nodes['date_inserted'],
             'date_modified': nodes['date_modified']},
            index=nodes.index
        )

        return resolved.fillna(pd.NA)

    def resolve_strings(self, subset=None, node_type=None):
        '''
        Get a copy of the strings frame with all integer ID elements
        replaced by the string values they represent

        Returns
        -------
        pandas.DataFrame
            Same shape and index as the strings frame.
        '''

        has_subset = subset is not None
        has_type = node_type is not None

        if has_subset:
            resolved = self.strings.loc[subset].copy()
            if has_type:
                typed_idx = self.get_strings_by_node_type(node_type).index
                resolved = resolved.query('index.isin(@typed_idx)')
        elif has_type:
            typed_idx = self.get_strings_by_node_type(node_type).index
            resolved = self.strings.query('index.isin(@typed_idx)').copy()
        else:
            resolved = self.strings.copy()

        try:

            resolved['node_type'] = resolved['node_id'].map(
                self.nodes['node_type_id']
            )
            resolved['node_type'] = resolved['node_type'].map(
                self.node_types['node_type']
            )
            return resolved[[
                'node_id',
                'string',
                'node_type',
                'date_inserted',
                'date_modified'
            ]]

        except NodesNotFoundError:

            resolved['node_type_id'] = self.strings['node_type_id'].map(
                self.node_types['node_type']
            )
            return resolved.rename(columns={'node_type_id': 'node_type'})

    def select_strings_by_node_type(self, node_type):

        # if node_type is list-like, get the set
        if bg.util.iterable_not_string(node_type):

            node_type_id = [
                self.id_lookup('node_types', nt) for nt in node_type
            ]

            try:
                selection = self.nodes.query(
                    'node_type_id.isin(@node_type_id)'
                )
            except NodesNotFoundError:
                selection = self.strings.query(
                    'node_type_id.isin(@node_type_id)'
                )

        # if not list-like, first assume node_type is a string
        try:

            assert node_type.casefold()

            node_type_id = self.id_lookup('node_types', node_type)

            try:
                selection = self.nodes.query(
                    'node_type_id == @node_type_id'
                )
            except NodesNotFoundError:
                selection = self.strings.query(
                    'node_type_id == @node_type_id'
                )

        # if node_type doesn't have casefold, assume it's a numeric
        # node type ID
        except AttributeError:

            try:
                selection = self.nodes.query(
                    'node_type_id == @node_type'
                )
            except NodesNotFoundError:
                selection = self.strings.query(
                    'node_type_id == @node_type'
                )

        try:
            self.nodes

            return selection

        except NodesNotFoundError:
            return self.strings.loc[selection.index]

    def synthesize_shorthand_entries(
        self,
        node_subset=None,
        node_type=None,
        entry_prefix=None,
        entry_syntax=None,
        syntax_case_sensitive=True,
        allow_redundant_items=False,
        item_separator=None,
        comment_char=None,
        space_char=None,
        sort_by=None,
        sort_prefixes=True,
        sort_case_sensitive=True,
        fill_spaces=False,
        name_type='abbr',
        hide_default_entry_prefixes=False
    ):

        if node_subset is not None:

            if node_type is None:
                node_ids = self.nodes.loc[node_subset].index

            else:
                node_type_ids = self.id_lookup(
                    'node_types',
                    node_type,
                    return_scalar=False
                )
                node_ids = self.nodes.loc[node_subset]
                type_selector = node_ids['node_type_id'].isin(node_type_ids)
                node_ids = node_ids.loc[type_selector].index

        elif node_type is not None:

            node_type_ids = self.id_lookup(
                'node_types',
                node_type,
                return_scalar=False
            )
            type_selector = self.nodes['node_type_id'].isin(node_type_ids)
            node_ids = self.nodes.loc[type_selector].index

        else:

            node_ids = self.nodes.index

        if entry_syntax is None:

            entry_syntax_assertions = self.get_assertions_by_link_type(
                'shorthand_entry_syntax'
            )
            inp_strings_to_entry_syntax = pd.Series(
                entry_syntax_assertions['tgt_string_id'].array,
                index=entry_syntax_assertions['inp_string_id']
            )

            node_assertions = self.assertions.loc[
                self.get_assertion_ids_by_node_id(node_ids)
            ]

            column_labels = ['src_string_id', 'tgt_string_id', 'ref_string_id']

            node_maps = [
                pd.DataFrame({
                    'inp_string_id': node_assertions['inp_string_id'].array,
                    'node_id': node_assertions[label].map(
                        self.strings['node_id']
                    )
                })
                for label in column_labels
            ]

            inp_strings_to_nodes = pd.concat(node_maps)
            '''keep = pd.util.hash_pandas_object(
                inp_strings_to_nodes.reset_index(),
                index=False
            )
            keep = keep.duplicated() ^ keep.duplicated(keep=False)

            inp_strings_to_nodes = inp_strings_to_nodes.loc[keep.array]'''
            hashed = pd.util.hash_pandas_object(
                inp_strings_to_nodes.reset_index(),
                index=False
            )
            inp_strings_to_nodes = inp_strings_to_nodes.loc[
                ~hashed.duplicated().array
            ]

            syntaxes_to_nodes = inp_strings_to_nodes['inp_string_id'].map(
                inp_strings_to_entry_syntax
            )

            syntaxes_to_nodes = pd.DataFrame({
                'entry_syntax_string_id': syntaxes_to_nodes.array,
                'node_id': inp_strings_to_nodes['node_id'].array
            })

            syntax_order = (
                syntaxes_to_nodes['entry_syntax_string_id'].value_counts()
            )
            syntax_order = (
                syntaxes_to_nodes['entry_syntax_string_id'].sort_values(
                    key=lambda x: syntax_order[x],
                    ascending=False
                )
            )
            syntaxes_to_nodes = syntaxes_to_nodes.loc[syntax_order.index]
            syntaxes_to_nodes = syntaxes_to_nodes.drop_duplicates(
                subset='node_id'
            )

            syntaxes_to_nodes = pd.Series(
                syntaxes_to_nodes['node_id'].array,
                index=syntaxes_to_nodes['entry_syntax_string_id']
            )

            input_metadata_assertions = (
                self.get_input_metadata_assertions_by_inp_string_id(
                    inp_strings_to_entry_syntax.index
                )
            )
            input_metadata_assertions = self.get_assertions_by_link_type(
                [
                    'syntax_case_sensitive',
                    'allow_redundant_items',
                    'comment_char',
                    'item_separator',
                    'space_char',
                    'default_entry_prefix'
                ],
                subset=input_metadata_assertions.index
            )
            input_metadata = self.resolve_assertions(
                subset=input_metadata_assertions.index
            )
            input_metadata = input_metadata[['tgt_string', 'link_type']]
            input_metadata['entry_syntax_string_id'] = (
                input_metadata_assertions['src_string_id'].map(
                    inp_strings_to_entry_syntax
                )
            )
            input_metadata = input_metadata.pivot(
                index='entry_syntax_string_id',
                columns='link_type',
                values='tgt_string'
            )
            input_metadata = input_metadata.applymap(literal_eval)

            syntaxes = {
                string_id: bg.syntax_parsing.validate_entry_syntax(
                    self.strings.loc[string_id, 'string'],
                    case_sensitive=input_metadata.loc[
                        string_id,
                        'syntax_case_sensitive'
                    ],
                    allow_redundant_items=input_metadata.loc[
                        string_id,
                        'allow_redundant_items'
                    ]
                )
                for string_id in inp_strings_to_entry_syntax.array
            }

            entry_parts = [
                synthesize_entries_by_syntax(
                    self,
                    # syntaxes_to_nodes.loc[string_id],
                    node_ids,
                    syntax,
                    name_type,
                    entry_prefix
                )
                for string_id, syntax in syntaxes.items()
            ]

            entry_parts = pd.concat(entry_parts)

        else:

            try:
                entry_syntax_string = self.strings.loc[entry_syntax]
                entry_syntax_string = bg.util.get_single_value(
                    entry_syntax_string['string']
                )

            except KeyError:

                entry_syntax_string = self.strings.query(
                    'string == @entry_syntax'
                )

                if entry_syntax_string.empty:
                    entry_syntax_string = entry_syntax
                else:
                    entry_syntax_string = bg.util.get_single_value(
                        entry_syntax_string['string']
                    )

            syntax_df = bg.syntax_parsing.validate_entry_syntax(
                entry_syntax_string,
                case_sensitive=syntax_case_sensitive,
                allow_redundant_items=allow_redundant_items
            )

            entry_parts = synthesize_entries_by_syntax(
                self,
                node_ids,
                syntax_df,
                name_type,
                entry_prefix
            )

        '''node_id_entry_prefix_map = pd.Series(
            entry_parts.index.get_level_values(0),
            index=entry_parts['entry_prefix'].array
        )
        node_id_entry_prefix_map = node_id_entry_prefix_map.drop_duplicates()
        node_id_entry_prefix_map = pd.Series(
            node_id_entry_prefix_map.index,
            index=node_id_entry_prefix_map.array
        )'''

        if sort_by is None:

            # If we aren't sorting the output, insert prefixes for
            # prefixed strings and pivot the DataFrame so that each
            # row represents a single entry
            entry_parts = entry_parts.apply(
                prefix_targets,
                axis='columns'
            )
            entry_parts = entry_parts.reset_index()
            entry_parts = entry_parts.pivot(
                index=['src_node_id', 'entry_prefix'],
                columns='item_position',
                values='tgt_string'
            )
            entry_parts = entry_parts.reset_index().set_index('src_node_id')

        else:

            if not bg.util.iterable_not_string(sort_by):
                sort_by = [sort_by]

            # If we are sorting the output, make a map from item
            # labels to item prefix separators
            prefix_separators = entry_parts.copy()[
                ['item_prefix_separator', 'item_label']
            ]
            prefix_separators = prefix_separators.drop_duplicates()
            prefix_separators = prefix_separators.set_index('item_label')
            prefix_separators = prefix_separators.squeeze()

            # Pivot the item strings into a DataFrame so that each
            # row represents a single entry. The column labels are a
            # multiindex containing both the item label, which could
            # be a prefix, and the item position, which is always a
            # string of digits
            entry_parts = entry_parts.reset_index()
            entry_parts = entry_parts.pivot(
                index=['src_node_id', 'entry_prefix'],
                columns=['item_label', 'item_position'],
                values='tgt_string'
            )
            entry_parts = entry_parts.reset_index().set_index('src_node_id')

            if len(entry_parts) == 1:
                order = entry_parts.index

            elif sort_prefixes:
                # If we're sorting the prefixes first, insert
                # prefixes
                entry_parts = entry_parts.apply(
                    prefix_multiindex_column,
                    args=(prefix_separators,)
                )

                # Get column(s) for the item position(s) we're
                # sorting on.
                order = [
                    entry_parts.loc[:, (slice(None), str(label))]
                    for label in sort_by
                ]

                # If the item to sort on is prefixed, then there is
                # one column for each (item prefix, item position)
                # pair, so we have to collapse those into a single
                # column before sorting
                order = [
                    collapse_columns(part, part.columns)
                    for part in order
                ]

                # Make each item position into a single column,
                # optionally casefolding if the sort is not
                # case sensitive
                if sort_case_sensitive:
                    order = [part.squeeze() for part in order]
                else:
                    order = [
                        part.squeeze().str.casefold() for part in order
                    ]

                # concat the item position(s) we're sorting on into
                # a single dataframe
                order = pd.concat(order, axis='columns')

            else:
                # If we're sorting on unprefixed string values, do
                # the same operations described above, but get the
                # strings and sort them before adding the prefixes
                order = [
                    entry_parts.loc[:, (slice(None), str(label))]
                    for label in sort_by
                ]
                order = [
                    collapse_columns(part, part.columns)
                    for part in order
                ]

                if sort_case_sensitive:
                    order = [part.squeeze() for part in order]
                else:
                    order = [
                        part.squeeze().str.casefold() for part in order
                    ]

                order = pd.concat(order, axis='columns')

                entry_parts = entry_parts.apply(
                    prefix_multiindex_column,
                    args=(prefix_separators,)
                )

            # Get an index of entry strings in the requested order
            order = order.sort_values(list(order.columns)).index

            # Group all columns by item position and collapse groups
            # into single columns.
            entry_parts = entry_parts.groupby(
                axis=1,
                level='item_position',
                group_keys=False
            )
            entry_parts = entry_parts.apply(
                lambda x: collapse_columns(x, x.columns)
            )

            # Put the entries in the sorted order we generated above

            entry_parts = entry_parts.loc[order]

        '''entry_parts['entry_prefix'] = entry_parts.index.map(
            node_id_entry_prefix_map
        )'''

        try:

            input_metadata

            entry_parts['syntax_string_id'] = entry_parts.index.map(
                pd.Series(syntaxes_to_nodes.index, index=syntaxes_to_nodes)
            )

        except NameError:

            input_metadata = {
                k: v for k, v in locals().items()
                if (
                    k in ['comment_char', 'item_separator', 'space_char']
                    and v is not None
                )
            }

        try:
            syntax_df

            if hide_default_entry_prefixes is True:
                default_prefix = bg.util.get_single_value(
                    syntax_df,
                    'default_entry_prefix'
                )
            elif hide_default_entry_prefixes is not False:
                default_prefix = hide_default_entry_prefixes

            if hide_default_entry_prefixes:
                pfix_to_drop = entry_parts['entry_prefix'] == default_prefix
                entry_parts.loc[pfix_to_drop, 'entry_prefix'] = pd.NA

            entry_strings = complete_entry_strings(
                    entry_parts,
                    input_metadata,
                    fill_spaces,
                    hide_default_entry_prefixes=False
            )

        except NameError:

            entry_parts = [
                complete_entry_strings(
                    entry_parts.query('syntax_string_id == @i'),
                    input_metadata,
                    fill_spaces,
                    hide_default_entry_prefixes=True
                )
                for i in entry_parts['syntax_string_id'].unique()
            ]
            entry_strings = pd.concat(entry_parts)


        if entry_strings.empty:
            return pd.Series(dtype='object')

        else:
            return pd.Series(
                entry_strings.array,
                index=entry_strings.index.array
            )

