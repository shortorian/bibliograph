import bibliograph as bg
import pandas as pd


def _collapse_columns(df, columns):

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


def _concat_list_item_elements(list_elements):

    sep = bg.util.get_single_value(list_elements, 'list_delimiter')
    item_label = bg.util.get_single_value(list_elements, 'item_label')

    item_string = sep.join(
        list_elements.sort_values(by='list_position')['tgt_string']
    )

    item = pd.DataFrame(
        {
            'item_position': list_elements.name,
            'tgt_string': item_string,
            'item_label': item_label
        },
        index=[list_elements.index.min()]
    )

    return item


def _synthesize_entries_from_links(
    group,
    entry_syntax,
    prefix_label_map,
    strings,
):
    '''
    Check if a set of links with a common source string ID have links
    like a shorthand entry with the given syntax. If so, return a pandas
    DataFrame with string values for each item in the entry.

    Parameters
    ----------
    group : pandas.DataFrame
        The set of all links whose source is a given src_string_id.
        Has columns
        [
            'src_string_id',
            'tgt_string_id',
            'link_type_id',
            'list_position',
            'ref_string_id'
        ]
        every row should have the same value in column 'src_string_id'

    entry_syntax : pandas.DataFrame
        syntax definition for a single entry prefix. has columns
        [
            'item_node_type',
            'item_link_type',
            'list_delimiter',
            'item_position',
            'item_label'
        ]
        where the node and link types are integer IDs, item_position is
        numeric but string-valued, and item_label is string-valued and
        alphanumeric.

    prefix_label_map : pandas.Series
        A map between positional item labels (the index) and
        alphanumeric item prefixes (the array). The array should be
        sorted by the index and then by the position of the prefix
        within the list of prefixes in the entry syntax file.

    strings : pandas.DataFrame
        has columns ['string', 'node_type_id']

    Returns
    -------
    pandas.DataFrame
    '''

    # Cache an empty return value
    return_cols = [
        'tgt_string',
        'item_label',
        'item_position',
        'item_prefix_separator'
    ]
    empty = pd.DataFrame(columns=return_cols)

    # The minimum number of links required for this source string to
    # represent an instance of the entry prefix with the given syntax is
    # the number of unprefixed item positions in the entry syntax. They
    # are denoted with integer item labels. The entry_prefix.notna()
    # query excludes entry syntax rows that were added to account for
    # links to the null node.
    min_links = entry_syntax.query('entry_prefix.notna()')
    min_links = min_links.query('item_label.str.isdigit()')
    min_links = len(min_links)
    if len(group) < min_links:
        return empty

    # Reduce group dataframe to relevant columns only, and make a copy
    # so we don't mutate an argument.
    group = group[['tgt_string_id', 'link_type_id', 'list_position']].copy()

    # This source string might be an instance of an entry that should
    # have the requested prefix. To check, we need to get the node types
    # of the link targets and compare them to the node types and link
    # types listed in the entry syntax.
    group = group.merge(
        strings['node_type_id'].rename('tgt_node_type_id'),
        left_on='tgt_string_id',
        right_index=True,
        copy=False
    )

    # Hash the (node type, link type) pairs in the syntax and the link
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
    # If any of the items isn't represented in the group, return null.
    if not type_pair_check.all():
        return empty

    # We now assume this source string is one that would have the
    # requested prefix.

    # Merge the link group with the entry syntax so we have enough
    # information to get the link targets in order
    relevant_syntax_columns = [
        'type_pair_hash',
        'list_delimiter',
        'item_position',
        'item_label',
        'item_prefix_separator'
    ]
    group = group.merge(entry_syntax[relevant_syntax_columns])

    # Insert the string values of the link targets into the group
    group['tgt_string'] = group['tgt_string_id'].map(strings['string'])

    # For any items that are lists of elements, concatenate the elements
    # using the list delimiter in the entry syntax
    list_items = group.query('list_position.notna()')

    if not list_items.empty:
        list_items = list_items.groupby(by='item_position', group_keys=False)
        list_items = list_items.apply(_concat_list_item_elements)

        group = pd.concat([group.query('list_position.isna()'), list_items])

    # The group of links now has one row per item position except for
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


def _prefix_targets(row):
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
            [row['item_position'], components.sum()],
            index=['item_position', 'tgt_string']
        )

    else:
        return row[['item_position', 'tgt_string']]


def _prefix_column(column, prefix_separators):
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


class ParsedShorthand:
    '''
    ParsedShorthand has attributes containing the results of a
    parsing operation and methods to synthesize them in various ways.


    Parameters
    ----------
    All dataframe columns with labels ending "_id", as well as the
    column links['list_position'] are integer-valued. Series and columns
    with other labels are string-valued. Indexes are always
    integer-valued.

    strings : pandas.DataFrame
        has columns ['string', 'node_type_id']

    links : pandas.DataFrame
        has columns [
            'src_string_id',
            'tgt_string_id',
            'link_type_id',
            'list_position',
            'ref_string_id'
        ]

    link_tags : pandas.DataFrame
        has columns ['link_id', 'tag_string_id']

    node_types : pandas.Series

    link_types : pandas.Series

    entry_prefixes : pandas.Series

    item_labels : pandas.Series

    item_separator : str

    default_entry_prefix : str

    space_char : str

    na_string_values : str or list-like of str

    syntax_case_sensitive : bool
    '''

    def __init__(
            self,
            strings,
            links,
            link_tags,
            node_types,
            link_types,
            entry_prefixes,
            item_labels,
            item_separator,
            default_entry_prefix,
            space_char,
            comment_char,
            na_string_values,
            na_node_type,
            syntax_case_sensitive,
            allow_redundant_items
    ):

        [self.__setattr__(k, v) for k, v in locals().items() if k != 'self']

    def _get_entries_by_prefix(
        self,
        entry_syntax,
        source_string_id_subset
    ):

        # If this entry prefix isn't listed in the entry syntax, return
        # an empty series
        if entry_syntax.empty:
            return pd.Series(dtype='object')

        # Items in an entry might be linked to the null node rather than
        # the target defined in the syntax. We only care about this when
        # checking what kind of links are present for an entry in the
        # parsed data, so we add rows to the entry syntax with every
        # link type paired with a null item node type
        null_syntax = entry_syntax.copy()[[
            'item_label',
            'item_node_type',
            'item_link_type',
            'list_delimiter'
        ]]
        null_syntax['item_node_type'] = self.na_node_type
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
            self.node_types.index,
            index=self.node_types
        )
        node_type_ids = entry_syntax['item_node_type'].map(node_type_id_map)
        entry_syntax.loc[:, 'item_node_type'] = node_type_ids.array

        # Convert string-valued link types to integer IDs
        link_type_id_map = pd.Series(
            self.link_types.index,
            index=self.link_types
        )
        link_type_ids = entry_syntax['item_link_type'].map(link_type_id_map)
        entry_syntax.loc[:, 'item_link_type'] = link_type_ids.array

        # Get the integer node type ID for this entry
        entry_node_type = bg.util.get_single_value(
            entry_syntax,
            'entry_node_type'
        )
        entry_node_type = self.id_lookup('node_types', entry_node_type)

        # Get string IDs for strings that have the node type ID for this
        # entry
        string_id_selection = self.strings.query(
            'node_type_id == @entry_node_type'
        )
        string_id_selection = string_id_selection.index

        # Select links whose sources are the selected string IDs
        source_selected = self.links['src_string_id'].isin(
            string_id_selection
        )

        # Restrict selection to set of allowed source strings if caller
        # provided one
        if source_string_id_subset is not None:
            source_string_id_subset = self.links['src_string_id'].isin(
                source_string_id_subset
            )
            source_selected = source_selected & source_string_id_subset

        # Restrict selection to the link types listed for items
        # in the syntax for this entry prefix
        link_type_selected = self.links['link_type_id'].isin(
            entry_syntax['item_link_type']
        )

        link_selected = source_selected & link_type_selected

        # If these criteria leave no links selected, return an empty
        # Series. Otherwise, get a subset of links that meet our
        # criteria.
        if not link_selected.any():
            return pd.Series(dtype='object')
        else:
            link_selection = self.links.loc[link_selected]

        # Group the links by source string. If a source has links like
        # the entry prefix in the syntax, generate strings representing
        # items for that entry.
        link_selection = link_selection.groupby(by='src_string_id')

        entry_strings = link_selection.apply(
            _synthesize_entries_from_links,
            entry_syntax,
            prefix_label_map,
            self.strings
        )

        return entry_strings

    def id_lookup(self, attr, string, column_label='string'):
        '''
        Take the name of an attribute of ParsedShorthand. If the
        attribute is a pandas Series or DataFrame, return the numerical
        index of the input string within the attribute.

        Parameters
        ----------
        attr : str
            Name of an attribute of ParsedShorthand

        string : str
            String value to retrieve index for

        column_label : str, default 'string'
            Column label to index if ParsedShorthand.attr is a
            DataFrame. Default to 'string' because at the moment
            ParsedShorthand.strings is the only DataFrame available.
        '''

        attribute = self.__getattribute__(attr)

        try:
            # If this assertion passes, assume attribute is a Series
            assert attribute.str

            element = attribute.loc[attribute == string]

        except AttributeError:
            # Otherwise assume attribute is a DataFrame
            if column_label == 'string' and attr != 'strings':
                raise ValueError(
                    'Must use column_label keyword when indexing a '
                    'DataFrame'
                )

            element = attribute.loc[attribute[column_label] == string]
        length = len(element)

        if length == 1:
            return element.index[0]
        elif length == 0:
            raise KeyError(string)
        elif length > 1:
            raise ValueError('{} index not unique!'.format(attr))

    def resolve_strings(self):
        '''
        Get a copy of the strings frame with all integer ID elements
        replaced by the string values they represent

        Returns
        -------
        pandas.DataFrame
            Same shape and index as the strings frame.
        '''

        resolved = self.strings.copy()

        resolved['node_type_id'] = self.strings['node_type_id'].map(
            self.node_types
        )
        resolved = resolved.rename(columns={'node_type_id': 'node_type'})

        return resolved

    def resolve_links(self, node_types=True, tags=True):
        '''
        Get a copy of the links frame with all integer ID elements
        replaced by the string values they represent

        Parameters
        ----------
        node_types : bool, default True
            If True, add columns for the node types of the source,
            target, and reference strings (three additional columns).

        tags : bool, default True
            If True, add a column for link tags. All tags for a link are
            joined into a single string separated by spaces.

        Returns
        -------
        pandas.DataFrame
        '''

        string_map = self.strings['string']

        resolved = pd.DataFrame(
            {'src_string': self.links['src_string_id'].map(string_map),
             'tgt_string': self.links['tgt_string_id'].map(string_map)},
            index=self.links.index
        )

        resolved['link_type'] = self.links['link_type_id'].map(
            self.link_types
        )

        resolved['ref_string'] = self.links['ref_string_id'].map(string_map)

        if node_types:
            resolved['src_node_type'] = self.links['src_string_id'].map(
                self.strings['node_type_id']
            )
            resolved['src_node_type'] = resolved['src_node_type'].map(
                self.node_types
            )

            resolved['tgt_node_type'] = self.links['tgt_string_id'].map(
                self.strings['node_type_id']
            )
            resolved['tgt_node_type'] = resolved['tgt_node_type'].map(
                self.node_types
            )

            resolved['ref_node_type'] = self.links['ref_string_id'].map(
                self.strings['node_type_id']
            )
            resolved['ref_node_type'] = resolved['ref_node_type'].map(
                self.node_types
            )

        if tags is True:
            # Resolve link tags as space-delimited lists
            tags = self.link_tags.groupby('link_id')
            tags = tags.apply(
                lambda x:
                ' '.join(self.strings.loc[x['tag_string_id'], 'string'])
            )

        return resolved

    def synthesize_shorthand_entries(
        self,
        entry_prefix=None,
        entry_node_type=None,
        sort_by=None,
        sort_prefixes=True,
        sort_case_sensitive=True,
        source_string_id_subset=None,
        fill_spaces=False,
        missing_node_warning=True
    ):
        '''
        Takes an entry prefix and generates a pandas Series of string
        representations of each entry of that type in the parsed data.

        Parameters
        ----------
        entry_prefix : str
            prefix in the entry syntax

        entry_node_type : str
            entry node type in the entry syntax

        sort_by : str or list of str
            item columns on which the output should be sorted

        sort_prefixes : bool, default True
            If True, add item prefixes prior to sorting columns. If
            False, sort values then add any prefixes afterwards.

        source_string_id_subset : pandas.Series, default None
            Must be boolean dtype. If present, only generate output for
            entries whose source strings are in
            ParsedShorthand.strings.loc[source_string_id_subset]

        Returns
        -------
        pandas Series
            string-valued representations of a set of entries
        '''

        # Get the text of the entry syntax and validate it
        entry_syntax_node_type_ID = self.id_lookup(
            'node_types',
            'shorthand_entry_syntax'
        )
        entry_syntax = self.strings.query(
            'node_type_id == @entry_syntax_node_type_ID'
        )
        entry_syntax = bg.util.get_single_value(entry_syntax, 'string')
        entry_syntax = bg.syntax_parsing.validate_entry_syntax(
            entry_syntax,
            case_sensitive=self.syntax_case_sensitive
        )

        if entry_prefix is not None:
            # Select a single entry prefix out of the entry syntax
            entry_syntax = entry_syntax.query('entry_prefix == @entry_prefix')

            # If there's no node type for this entry prefix then no
            # synthesis is required. Read available entry strings
            # directly and return them.
            if entry_syntax['entry_node_type'].empty:
                if missing_node_warning:
                    print(
                        'Warning: requested entry type has no node '
                        'type in the entry syntax.\nSelecting'
                        'strings that begin with the entry prefix.'
                        '\nUnprefixed entries are ignored.'
                    )

                entry_strings = self.strings.query('node_type_id.isna()')
                prefix = entry_prefix + self.item_separator
                entry_strings = entry_strings.loc[
                    entry_strings['string'].str.startswith(prefix)
                ]

                entry_strings = entry_strings['string']

                if sort_by is not None:
                    entry_strings = entry_strings.sort_values()

                if fill_spaces:
                    entry_strings = entry_strings.str.replace(
                        ' ',
                        self.space_char,
                        regex=False
                    )

                return pd.Series(entry_strings.array)

            else:
                entry_strings = self._get_entries_by_prefix(
                    entry_syntax,
                    source_string_id_subset
                )

        elif entry_node_type is not None:
            # Select a single node type out of the entry syntax
            entry_syntax = entry_syntax.query(
                'entry_node_type == @entry_node_type'
            )

            # Synthesize entries for each entry prefix in the selection
            prefixes = entry_syntax['entry_prefix'].drop_duplicates()
            syntaxes = [
                entry_syntax.query('entry_prefix == @entry_prefix')
                for entry_prefix in prefixes
            ]
            entry_strings = [
                self._get_entries_by_prefix(syntax, source_string_id_subset)
                for syntax in syntaxes
            ]

            # Concatenate entries for the different prefixes
            entry_strings = pd.concat(entry_strings)

        if sort_by is None:
            # If we aren't sorting the output, insert prefixes for
            # prefixed strings and pivot the DataFrame so that each
            # row represents a single entry
            entry_strings = entry_strings.apply(
                _prefix_targets,
                axis='columns'
            )
            entry_strings = entry_strings.reset_index()
            entry_strings = entry_strings.pivot(
                index='src_string_id',
                columns='item_position',
                values='tgt_string'
            )

        else:

            # make sort_by list-like if it isn't already
            if not bg.util.iterable_not_string(sort_by):
                sort_by = [sort_by]

            # If we are sorting the output, make a map from item
            # labels to item prefix separators
            prefix_separators = entry_strings.copy()[
                ['item_prefix_separator', 'item_label']
            ]
            prefix_separators = prefix_separators.drop_duplicates()
            prefix_separators = prefix_separators.set_index('item_label')
            prefix_separators = prefix_separators.squeeze()

            # Pivot the entry strings into a DataFrame so that each
            # row represents a single entry. The column labels are a
            # multiindex containing both the item label, which could
            # be a prefix, and the item position, which is always a
            # string of digits
            entry_strings = entry_strings.reset_index()
            entry_strings = entry_strings.pivot(
                index='src_string_id',
                columns=['item_label', 'item_position'],
                values='tgt_string'
            )

            if len(entry_strings) == 1:
                order = entry_strings.index

            elif sort_prefixes:
                # If we're sorting the prefixes first, insert
                # prefixes
                entry_strings = entry_strings.apply(
                    _prefix_column,
                    args=(prefix_separators,)
                )

                # Get column(s) for the item position(s) we're
                # sorting on.
                order = [
                    entry_strings.loc[:, (slice(None), str(label))]
                    for label in sort_by
                ]

                # If the item to sort on is prefixed, then there is
                # one column for each (item prefix, item position)
                # pair, so we have to collapse those into a single
                # column before sorting
                order = [
                    _collapse_columns(part, part.columns)
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
                    entry_strings.loc[:, (slice(None), str(label))]
                    for label in sort_by
                ]
                order = [
                    _collapse_columns(part, part.columns)
                    for part in order
                ]

                if sort_case_sensitive:
                    order = [part.squeeze() for part in order]
                else:
                    order = [
                        part.squeeze().str.casefold() for part in order
                    ]

                order = pd.concat(order, axis='columns')

                entry_strings = entry_strings.apply(
                    _prefix_column,
                    args=(prefix_separators,)
                )

            # Get an index of entry strings in the requested order
            order = order.sort_values(list(order.columns)).index

            # Group all columns by item position and collapse groups
            # into single columns.
            entry_strings = entry_strings.groupby(
                axis=1,
                level='item_position',
                group_keys=False
            )
            entry_strings = entry_strings.apply(
                lambda x: _collapse_columns(x, x.columns)
            )

            # Put the entries in the sorted order we generated above

            entry_strings = entry_strings.loc[order]

        entry_strings = entry_strings.apply(
            lambda x:
            x.str.replace(
                self.comment_char,
                '\\' + self.comment_char,
                regex=False
            )
        )

        entry_strings = entry_strings.apply(
            lambda x:
            x.str.replace(
                self.item_separator,
                '\\' + self.item_separator,
                regex=False
            )
        )

        # Join items by the item separator
        entry_strings = entry_strings.apply(
            lambda x: self.item_separator.join(x),
            axis='columns'
        )

        if fill_spaces:
            entry_strings = entry_strings.str.replace(
                ' ',
                self.space_char,
                regex=False
            )

        return pd.Series(entry_strings.array)
