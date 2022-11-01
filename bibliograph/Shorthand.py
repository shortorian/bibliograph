import bibliograph as bg
import pandas as pd
import tempfile
from pathlib import Path


def _create_id_map(domain, drop_na=True, **kwargs):
    '''
    Maps distinct values in a domain to a range of integers.  Additional
    keyword arguments are passed to the pandas.Series constructor when
    the map series is created.

    Parameters
    ----------
    domain : list-like (coercible to pandas.Series)
        Arbitrary set of values to map. May contain duplicates.

    drop_na : bool, default True
        Ignore null values and map only non-null values to integers.

    Returns
    -------
    pandas.Series
        Series whose length is the number of distinct values in the
        input domain.

    Examples
    --------
    >>> import pandas as pd
    >>> dom = ['a', 'a', 'b', pd.NA, 'f', 'b']
    >>> _create_id_map(dom, dtype=pd.UInt32Dtype())

    a    0
    b    1
    f    2
    dtype: UInt32

    >>> _create_id_map(dom, drop_na=False, dtype=pd.UInt32Dtype())

    a       0
    b       1
    <NA>    2
    f       3
    dtype: UInt32
    '''
    # check if domain object has a str attribute like a pandas.Series
    # and convert if not
    try:
        assert domain.str
        # make a copy so we can mutate one (potentially large) object
        # instead of creating additional references
        domain = domain.copy()
    except AttributeError:
        domain = pd.Series(domain)

    if drop_na:
        domain = domain.loc[~domain.isna()]

    distinct_values = domain.unique()

    id_map = pd.Series(
        range(len(distinct_values)),
        index=distinct_values,
        **kwargs
    )

    return id_map


def _strip_csv_comments(column, pattern):
    # make the whole column string valued
    column = column.fillna('')
    # split off the comments and return the non-comment part
    column = column.str.split(pat=pattern, expand=True)
    return column[0]


def _replace_escaped_comment_chars(column, comment_char, pattern):

    return column.replace(
        to_replace=pattern,
        value=comment_char,
        regex=True
    )


def _expand_shorthand_items(
    group,
    entry_syntax,
    entry_prefix_id_map,
    item_label_id_map
):
    '''
    THIS FUNCTION MUTATES ITS FIRST ARGUMENT

    Takes parsed shorthand entries grouped by entry prefix and item
    label, checks if the entry syntax has a list delimiter for this
    item, and splits the string on the delimiter if it exists.
    '''

    # The group name is a tuple (entry_prefix_id, item_label_id)
    if pd.isna(group.name[1]):
        # If the item label ID is NA then this group is entry
        # strings rather than items parsed out of entries
        return group

    else:
        # Get the item label ID
        item_label_id = group.name[1]

    # Locate this item label ID in item_label_id_map and get the string
    # value for the item label out of the map index
    item_label = item_label_id_map.loc[
        item_label_id_map == item_label_id
    ]
    item_label = item_label.index.drop_duplicates()
    if len(item_label) > 1:
        raise ValueError('item label ID not unique')
    else:
        item_label = item_label[0]

    if pd.isna(item_label):
        # If nulls were accounted for in the label ID map and the label
        # we just recovered is null, then this group is the entry
        # strings rather than items parsed out of the entries
        return group

    # Get the string value for the entry prefix out of the map index
    entry_prefix = entry_prefix_id_map.loc[
        entry_prefix_id_map == group.name[0]
    ]
    entry_prefix = entry_prefix.index[0]

    # If this prefix is not in the entry syntax then we assume this
    # group is self-descriptive and items shouldn't be expanded
    if entry_prefix not in entry_syntax['entry_prefix'].array:
        return group

    # Locate the row for this item in the entry syntax and get the
    # list delimiter
    item_syntax = entry_syntax.query('entry_prefix == @entry_prefix') \
                              .query('item_label == @item_label') \
                              .squeeze()
    delimiter = item_syntax['list_delimiter']

    if pd.notna(delimiter):
        # If there is a delimiter, split this group's strings in place
        group.loc[:, 'string'] = group['string'].str.split(delimiter)
        return group

    else:
        # If there is no delimiter, return the group
        return group


def _normalize_shorthand(shnd_input, comment_char, fill_cols, drop_na):
    '''
    Fill or drop missing values in shorthand input and parse comments.
    Comments have to be parsed in this function rather than using
    pd.read_csv(comment=comment_char, escapechar='\') because there may
    be escaped characters in entries that should be parsed separately.
    Using read_csv with an escape character removes the escape character
    anywhere in the file, so the non-comment character escapes would be
    lost.

    The input file must have a header and the first four labels of the
    header must be (in any order and any character case):
        [left_entry, right_entry, link_tags_or_override, reference]

    Parameters
    ----------
    shnd_input : pandas.DataFrame
        Unparsed shorthand data

    skiprows : int
        Number of lines to skip at the beginning of the input file.

    comment_char : str
        Character indicating the rest of a line should be skipped. Must
        be a single character.

    fill_cols : scalar or non-string iterable, default 'left_entry'
        Label(s) of columns that will be forward filled.

    drop_na : scalar or non-string iterable, default 'right_entry'
        Column labels. Rows will be dropped if they have null values in
        columns these columns.

    Returns
    -------
    pandas.DataFrame
        Normalized shorthand input data
    '''

    if len(shnd_input.columns) > 256:
        raise ValueError(
            'Shorthand input csv files cannot have more than 256 '
            'columns'
        )

    required_cols = [
        'left_entry', 'right_entry', 'link_tags_or_override', 'reference'
    ]

    valid_columns = [
        (c in map(str.casefold, shnd_input.columns)) for c in required_cols
    ]

    if not all(valid_columns):
        raise ValueError(
            'shorthand csv files must have a header whose first four '
            'column labels must be:\n'
            '>>> ["left_entry", "right_entry", '
            '"link_tags_or_override", "reference"]\n'
            '>>> (ignoring case and list order)'
        )

    # If the comment character is a regex metacharacter, escape it
    comment_char = bg.util.escape_regex_metachars(comment_char)
    # Regular expressions to match bare and escaped comment characters
    unescaped_comment_regex = r"(?<!\\)[{}]".format(comment_char)
    escaped_comment_regex = fr"(\\{comment_char})"

    # Find cells where comments start
    has_comment = shnd_input.apply(
        lambda x:
        x.str.contains(unescaped_comment_regex) if x.notna().any() else False
    )
    has_comment = has_comment.fillna(False)

    # Set cells to the right of cells with comments to False because
    # they were created by commas in comments
    commented_out = has_comment.where(has_comment).ffill(axis=1).fillna(False)
    commented_out = commented_out ^ has_comment

    shnd_input = bg.util.set_string_dtype(shnd_input.mask(commented_out))

    # Mask off cells to the right of cells with comments in them
    shnd_input = shnd_input.mask(commented_out)

    # Split cells where comments start and take the uncommented part
    has_comment = has_comment.any(axis=1)
    shnd_input.loc[has_comment, :] = shnd_input.loc[has_comment].apply(
        _strip_csv_comments,
        args=(comment_char,)
    )

    # Drop rows that began with comments
    shnd_input = shnd_input.mask(shnd_input == '')
    shnd_input = shnd_input.dropna(how='all')
    shnd_input = shnd_input.replace('', pd.NA)

    # Replace escaped comment characters with bare comment characters
    shnd_input = shnd_input.apply(
        _replace_escaped_comment_chars,
        args=(comment_char, escaped_comment_regex)
    )

    # Optionally forward fill missing values
    for column in fill_cols:
        shnd_input.loc[:, column] = shnd_input.loc[:, column].ffill()

    # Optionally drop lines missing values
    shnd_input = shnd_input.dropna(subset=drop_na)

    return shnd_input


def _get_item_link_source_IDs(group):
    '''
    Take a group of items that all have the same csv row and column
    index, meaning they were exploded out of a single entry, and return
    the string ID for the entry they were exploded from.

    If the entry syntax indicated that there are no links between a
    group of item strings and the entry string that contained them,
    return null values
    '''
    link_type_is_na = group['link_type_id'].isna()

    # If all the link types are NA then there are no links between items
    # inside this entry and the entry string itself
    if link_type_is_na.all():
        return pd.Series([pd.NA]*len(group), index=group.index)

    # If any link types are not null then return the string_id of the
    # entry the items were exploded from
    # NOTE: this depends on entry syntax validation checking that when
    # an entry type has links for any item it has links for all
    # (unprefixed) items. That way the only string with no link type
    # here is the entry string itself. If that syntax validation
    # requirement is relaxed in the future for some reason, this code
    # will break.
    else:
        entry_string_id = group.loc[link_type_is_na, 'string_id'].squeeze()
        return pd.Series([entry_string_id]*len(group), index=group.index)


def _get_entry_prefix_ids(
    side,
    shnd_data,
    dplct_entries,
    csv_column_id_map
):
    '''
    Reconstruct entry prefixes for one side of a link between
    entries, including duplicates not present in data
    '''

    # "right" and "left" entries are defined in the link syntax
    # file but they don't constrain the location of those
    # columns in the input csv file, so we need to get the index
    # of the csv column for this side of the link.
    csv_col = csv_column_id_map[side + '_entry']
    label = side + '_entry_prefix_id'
    entry_prefixes = shnd_data.loc[
        :,
        ['csv_row', 'csv_col', 'entry_prefix_id', 'item_label_id']
    ]

    # Locate entry strings by finding null item labels
    entry_prefixes = entry_prefixes.query('item_label_id.isna()') \
                                   .query('csv_col == @csv_col')
    # The item label column is now null values, so drop it
    entry_prefixes = entry_prefixes.drop('item_label_id', axis='columns')

    # "string" csv indexes locating distinct strings are
    # different from "entry" csv indexes, which locate entries
    # with potentially duplicate string values. Copy the string
    # csv indexes to fill in missing data after merger with
    # duplicates.
    entry_prefixes = entry_prefixes.rename(
        columns={
            'entry_prefix_id': label,
            'csv_row': 'string_csv_row',
            'csv_col': 'string_csv_col'
        }
    )

    # Get csv indexes for duplicate entries on this side of the
    # link and merge the map of duplicates with entry_prefixes
    # to match the csv indexes of distinct strings with csv
    # indexes of duplicate strings
    on_side_dplcts = dplct_entries.query('entry_csv_col == @csv_col')
    on_side_dplcts = entry_prefixes.merge(on_side_dplcts, how='right')

    # Collect all the prefixes together
    entry_prefixes = pd.concat([entry_prefixes, on_side_dplcts])

    # The "entry" csv indexes for the distinct strings are now
    # null, so copy the "string" csv indexes over to the "entry"
    # columns
    missing_entry = entry_prefixes['entry_csv_row'].isna()

    string_csv_idx = entry_prefixes.loc[
        missing_entry,
        ['string_csv_row', 'string_csv_col']
    ]
    string_csv_idx = string_csv_idx.to_numpy()

    entry_csv_cols = ['entry_csv_row', 'entry_csv_col']
    entry_prefixes.loc[missing_entry, entry_csv_cols] = string_csv_idx

    return entry_prefixes


def _get_link_component_string_ids(
        prefix_pairs,
        shnd_data,
        link_syntax,
        component_prefix,
        subset,
        columns=None,
        prefix_string_id=True
):
    '''
    Links have three string components: source, target, and
    reference. This function locates the data for one
    component in the link syntax and merges the position
    code for that component with a dataframe of entry prefix
    pairs to get all data required to locate the string ID
    for each link component.

    Parameters
    ----------

    link_syntax: pandas.DataFrame
        data defining links between pairs of shorthand entries

    component_prefix: str
        One of ['src_', 'tgt_', 'ref_']

    subset : pandas.Series
        A row indexer to subset the link syntax.

    include_link_id : bool, default False
        If True, select link type IDs from the mutable data

    columns : list-like or None, default None
        Columns to include in the returned dataframe. If None,
        return all columns.

    prefix_string_id : bool, default True
        Optionally add the component prefix to the string ID
        column label before returning.
    '''

    link_syntax_selection = link_syntax.loc[subset]

    columns_for_this_component = [
        'left_entry_prefix_id',
        'right_entry_prefix_id',
        component_prefix + 'csv_col',
        component_prefix + 'item_label',
        'link_type_id'
    ]

    # The reference position code could be null if the reference
    # string should be the input file, so drop any rows in the
    # link syntax that have no value for the current position
    # code
    component_csv_col = component_prefix + 'csv_col'
    link_syntax_selection = link_syntax_selection.dropna(
        subset=component_csv_col
    )

    # Merge the prefix pairs with the link syntax to get csv
    # rows and item labels for every link between entries
    link_component = prefix_pairs.merge(
        link_syntax_selection[columns_for_this_component],
        on=['left_entry_prefix_id', 'right_entry_prefix_id']
    )

    # Locate rows for which the link syntax says this component
    # (source, target, or reference) is in the left csv column
    is_L = (link_component[component_prefix + 'csv_col'] == 'l')
    # Extract the csv rows and columns that locate the string
    # value in the parsed data
    left_indexes = link_component[['L_str_csv_row', 'L_str_csv_col']]
    left_indexes = left_indexes.loc[is_L]
    left_indexes.columns = ['csv_row', 'csv_col']

    # Locate rows for which the link syntax says this component
    # (source, target, or reference) is in the right csv column
    is_R = (link_component[component_prefix + 'csv_col'] == 'r')
    # Extract the csv rows and columns that locate the string
    # value in the parsed data
    right_indexes = link_component[['R_str_csv_row', 'R_str_csv_col']]
    right_indexes = right_indexes.loc[is_R]
    right_indexes.columns = ['csv_row', 'csv_col']

    # Combine the csv indexes above into a single dataframe and
    # overwrite the csv rows and columns defined by the link
    # syntax with indexes that locate string values in the
    # parsed data
    link_component[['csv_row', 'csv_col']] = pd.concat(
        [left_indexes, right_indexes]
    )
    link_component = link_component.sort_index()

    # Drop all the information required to sort out which column
    # the string values came from
    link_component = link_component.drop(
        ['left_entry_prefix_id',
         'right_entry_prefix_id',
         'L_str_csv_row',
         'L_str_csv_col',
         'R_str_csv_row',
         'R_str_csv_col',
         component_csv_col],
        axis='columns'
    )

    # Rename the item label column so we can merge the link
    # component with the parsed data and recover the string IDs
    link_component = link_component.rename(
        columns={component_prefix + 'item_label': 'item_label_id'}
    )

    # The link component dataframe now contains the three pieces
    # of information we need to locate string IDs in the mutated
    # input data: csv row, csv column, and item label. We select
    # those columns from the mutable data along with the item
    # list positions so links can be tagged with positions in
    # ordered lists.
    data_columns = [
        'csv_row',
        'csv_col',
        'item_label_id',
        'item_list_position',
        'string_id'
    ]

    link_component = link_component.merge(
        shnd_data[data_columns],
        on=['csv_row', 'csv_col', 'item_label_id'],
        how='left'
    )

    # Optionally clean up the output

    if columns is not None:
        link_component = link_component[columns]

    if prefix_string_id and ('string_id' in columns):
        if isinstance(link_component, pd.DataFrame):
            link_component = link_component.rename(
                columns={'string_id': component_prefix + 'string_id'}
            )
        else:
            link_component = link_component.rename(
                component_prefix + 'string_id'
            )

    return link_component


def _copy_cross_duplicates(L_prefixes, R_prefixes):
    '''
    THIS FUNCTION MUTATES BOTH ARGUMENTS

    Get prefixes for duplicate entries in one column whose
    corresponding distinct string values are present in the
    other column
    '''

    L_missing_pfix = L_prefixes['left_entry_prefix_id'].isna()
    L_subset = [
        'string_csv_col', 'string_csv_row', 'left_entry_prefix_id'
    ]

    R_missing_pfix = R_prefixes['right_entry_prefix_id'].isna()
    R_subset = [
        'string_csv_col', 'string_csv_row', 'right_entry_prefix_id'
    ]

    # copy values from left to right
    to_copy = L_prefixes[L_subset].drop_duplicates()
    to_fill = R_prefixes.loc[R_missing_pfix, R_subset]

    cross_dplcts = to_copy.merge(
        to_fill,
        on=['string_csv_row', 'string_csv_col'],
        how='right'
    )
    cross_dplcts = cross_dplcts['left_entry_prefix_id'].array

    R_prefixes.loc[
        R_missing_pfix,
        'right_entry_prefix_id'
    ] = cross_dplcts

    # copy values from right to left
    to_copy = R_prefixes[R_subset].drop_duplicates()
    to_fill = L_prefixes.loc[L_missing_pfix, L_subset]

    cross_dplcts = to_copy.merge(
        to_fill,
        on=['string_csv_row', 'string_csv_col'],
        how='right'
    )
    cross_dplcts = cross_dplcts['right_entry_prefix_id'].array

    L_prefixes.loc[
        L_missing_pfix,
        'left_entry_prefix_id'
    ] = cross_dplcts


class Shorthand:
    '''
    A Shorthand has syntax definitions and provides methods that parse
    text according to the syntax.
    '''

    def __init__(
        self,
        entry_syntax,
        link_syntax=None,
        syntax_case_sensitive=True,
        allow_redundant_items=False
    ):

        try:
            # try and read it like a file stream
            self.entry_syntax = entry_syntax.read()

        except AttributeError:
            # otherwise assume it's a path
            with open(entry_syntax, 'r') as f:
                self.entry_syntax = f.read()

        # Validate the entry syntax
        entry_syntax = bg.syntax_parsing.validate_entry_syntax(
            self.entry_syntax,
            case_sensitive=syntax_case_sensitive,
            allow_redundant_items=allow_redundant_items
        )

        self.syntax_case_sensitive = syntax_case_sensitive
        self.allow_redundant_items = allow_redundant_items

        if link_syntax is not None:

            try:
                # try and read it like a file stream
                self.link_syntax = link_syntax.read()

            except AttributeError:
                # otherwise assume it's a path
                with open(link_syntax, 'r') as f:
                    self.link_syntax = f.read()

            # Validate the link syntax to raise any errors now without
            # storing the validated data
            bg.syntax_parsing._validate_link_syntax(
                self.link_syntax,
                entry_syntax,
                case_sensitive=self.syntax_case_sensitive
            )

    def _apply_syntax(
        self,
        filepath_or_buffer,
        item_separator,
        default_entry_prefix,
        space_char,
        na_string_values,
        na_node_type,
        input_string,
        input_node_type,
        skiprows,
        comment_char,
        fill_cols,
        drop_na,
        big_id_dtype,
        small_id_dtype,
        list_position_base,
        s_d_delimiter,
        encoding
    ):
        '''
        Takes a file-like object and parses it according to the
        definitions in Shorthand.entry_syntax and Shorthand.link_syntax.


        Parameters
        ----------
        filepath_or_buffer : str, path object, or file-like object
            A path or file-like object that returns csv-formatted text.
            "Path" is broadly defined and includes URLs. See
            pandas.read_csv documentation for details.

        skiprows : list-like, int or callable
            If int, number of lines to skip at the beginning of the
            input file. If list-like, 0-indexed set of line numbers to
            skip. See pandas.read_csv documentation for details.

        comment_char : str
            Indicates the remainder of a line should not be parsed.

        fill_cols : scalar or non-string iterable
            Label(s) of columns in the input file to forward fill.

        drop_na : scalar or non-string iterable
            Column labels. Rows will be dropped if they have null values
            in these columns.

        big_id_dtype : type
            dtype for large pandas.Series of integer ID values.

        small_id_dtype : type
            dtype for small pandas.Series of integer ID values.

        list_position_base : int
            Index value to be assigned to first element in items with
            list delimiters.

        Returns
        -------
        dict
            Has the following elements

            'strings': pandas.DataFrame with dtypes
                {'string': str, 'node_type_id': small_id_dtype}.
                Index type is big_id_dtype

            'links': pandas.DataFrame with dtypes
                {'src_string_id': big_id_dtype,
                 'tgt_string_id': big_id_dtype,
                 'ref_string_id': big_id_dtype,
                 'link_type_id: small_id_dtype}
                Index type is big_id_dtype

            'node_types': pandas.Series with dtype str. Index type is
                small_id_dtype.

            'link_types': pandas.Series with dtype str. Index type is
                small_id_dtype.

            'entry_prefixes': pandas.Series with dtype str. Index type
                is small_id_dtype.

            'item_labels': pandas.Series with dtype str. Index type is
                small_id_dtype.
        '''
        data = pd.read_csv(
            filepath_or_buffer,
            skiprows=skiprows,
            skipinitialspace=True,
            encoding=encoding
        )

        # see bg.util.set_string_dtype docstring for comment on new
        # pandas string dtype
        # shnd_input = bg.util.set_string_dtype(shnd_input)

        data = _normalize_shorthand(
            data,
            comment_char,
            fill_cols,
            drop_na
        )

        # Get any metadata for links between entries
        has_link_metadata = data['link_tags_or_override'].notna()
        link_metadata = data.loc[has_link_metadata, 'link_tags_or_override']

        if not link_metadata.empty:

            # Locate rows in the data that have self-descriptive entries
            # and link metadata
            entry_cols = ['left_entry', 'right_entry']
            entry_cols = [c for c in entry_cols if c in data.columns]
            has_s_d_entry = data[entry_cols].apply(
                lambda x: x.str.startswith(''.join([item_separator]*2))
            )
            has_s_d_entry = has_s_d_entry.any(axis=1)
            has_s_d_entry_and_link_metadata = has_link_metadata & has_s_d_entry

            # Copy data rows with self-descriptive entries and link
            # metadata
            if has_s_d_entry_and_link_metadata.any():

                has_s_d_entry_and_link_metadata = data.loc[
                    has_s_d_entry_and_link_metadata,
                    :
                ]

                make_s_d_links = True

            else:

                make_s_d_links = False

        else:
            make_s_d_links = False

        # replace text column labels in the mutable data with integers
        # so we compute on integer indexes
        csv_column_id_map = _create_id_map(
            list(data.columns),
            dtype=pd.UInt32Dtype()
        )
        data.columns = csv_column_id_map.array

        # Get input columns with entry strings
        data = data.loc[
            :,
            csv_column_id_map[['left_entry', 'right_entry', 'reference']]
        ]

        # Stack the entries into a pandas.Series. Values are entry
        # strings and the index is a multiindex with the csv row and
        # column for each entry
        data = data.stack().dropna()

        # Check for duplicate entry strings
        entry_is_duplicated = data.duplicated()

        if entry_is_duplicated.any():
            # If there are duplicate entries, we want to cache their
            # index values and drop the string values so we don't do
            # unnecessary work on the strings
            dplct_entries = data.loc[entry_is_duplicated, :]
            data = data.loc[~entry_is_duplicated, :]

            # Make a map between distinct entry strings and their
            # csv index values
            distinct_string_map = pd.Series(
                data.index.to_flat_index(),
                index=data
            )

            # Map the duplicate strings to csv index values and convert
            # the result into a dataframe. The dataframe has a
            # multiindex with the csv row and column of the duplicate
            # strings. The dataframe columns contain the csv rows and
            # columns of the distinct string value for each duplicate
            # string
            dplct_entries = dplct_entries.map(distinct_string_map)
            dplct_entries = pd.DataFrame(
                tuple(dplct_entries.array),
                columns=['string_csv_row', 'string_csv_col'],
                index=dplct_entries.index
            )

            # Reset the index and rename columns. "string" csv indexes
            # refer to the location of a distinct string value. "entry"
            # csv indexes refer to the location of a shorthand entry
            # whose string value is potentially a duplicate.
            dplct_entries = dplct_entries.reset_index()
            dplct_entries = dplct_entries.rename(
                columns={
                    'level_0': 'entry_csv_row',
                    'level_1': 'entry_csv_col'
                }
            )
            dplct_entries = dplct_entries.astype({
                'entry_csv_row': big_id_dtype,
                'entry_csv_col': pd.UInt8Dtype(),
                'string_csv_row': big_id_dtype,
                'string_csv_col': pd.UInt8Dtype()
            })

        else:
            # If there are no duplicate entries, initialize an empty
            # DataFrame
            dplct_entries = pd.DataFrame(columns=[
                'entry_csv_row',
                'entry_csv_col',
                'string_csv_row',
                'string_csv_col'
            ])

        # Remove clearspace around the entry strings
        data = data.str.strip()

        '''*********************************************
        data is currently a string-valued Series
        *********************************************'''

        # Read the entry syntax
        entry_syntax = bg.syntax_parsing.validate_entry_syntax(
            self.entry_syntax,
            case_sensitive=self.syntax_case_sensitive,
            allow_redundant_items=self.allow_redundant_items
        )

        # Parse entries in the input text
        data = bg.entry_parsing.parse_entries(
            data,
            entry_syntax,
            item_separator,
            default_entry_prefix,
            space_char,
            na_string_values,
            s_d_delimiter
        )
        data = data.reset_index()
        data = data.rename(
            columns={
                'level_0': 'csv_row',
                'level_1': 'csv_col',
                'grp_prefix': 'entry_prefix',
                'level_3': 'item_label'
            }
        )
        # replace missing entry prefixes with default value
        prefix_isna = data['entry_prefix'].isna()
        data.loc[prefix_isna, 'entry_prefix'] = default_entry_prefix

        # For any strings that represent null values, overwrite the node
        # type inferred from the syntax with the null node type
        null_strings = data['string'].isin(na_string_values)
        data.loc[null_strings, 'node_type'] = na_node_type

        dtypes = {
            'csv_row': big_id_dtype,
            'csv_col': pd.UInt8Dtype(),
            'entry_prefix': pd.StringDtype(),
            'item_label': pd.StringDtype(),
            'string': pd.StringDtype(),
            'node_type': pd.StringDtype(),
            'link_type': pd.StringDtype(),
            'node_tags': pd.StringDtype()
        }

        data = data.astype(dtypes)
        data.index = data.index.astype(big_id_dtype)

        '''******************************************************
        data is currently a DataFrame with these columns:
            ['csv_row', 'csv_col', 'entry_prefix', 'item_label',
             'string', 'node_type', 'link_type', 'node_tags']
        ******************************************************'''

        # Map string-valued entry prefixes to integer IDs
        entry_prefix_id_map = _create_id_map(
            data['entry_prefix'],
            dtype=small_id_dtype
        )
        # Replace entry prefixes in the mutable data with integer IDs
        data['entry_prefix'] = data['entry_prefix'].map(
            entry_prefix_id_map
        )
        data = data.rename(
            columns={'entry_prefix': 'entry_prefix_id'}
        )

        # Map string-valued item labels to integer IDs
        item_label_id_map = _create_id_map(
            data['item_label'],
            dtype=small_id_dtype
        )
        # Replace item labels in the mutable data with integer IDs
        data['item_label'] = data['item_label'].map(
            item_label_id_map
        )
        data = data.rename(
            columns={'item_label': 'item_label_id'}
        )

        # These link types are required to complete linking operations
        # later
        '''
        SWITCHING TO LITERAL NODE TYPE
        link_types = pd.Series(['entry', 'tagged', 'requires'])
        '''
        link_types = pd.Series(['entry', 'tagged'])

        # Map string-valued link types to integer IDs
        link_types = _create_id_map(
            pd.concat([link_types, data['link_type']]),
            dtype=small_id_dtype
        )
        # Replace link types in the mutable data with integer IDs
        data['link_type'] = data['link_type'].map(
            link_types
        )
        data = data.rename(
            columns={'link_type': 'link_type_id'}
        )
        # Mutate link_types into a series whose index is integer IDs and
        # whose values are string-valued link types
        link_types = pd.Series(link_types.index, index=link_types)

        '''******************************************************
        data is currently a DataFrame with these columns:
            ['csv_row', 'csv_col', 'entry_prefix_id', 'item_label_id',
             'string', 'node_type', 'link_type_id', 'node_tags']
        ******************************************************'''

        # Split items that have list delimiters in the entry syntax
        data = data.groupby(
            by=['entry_prefix_id', 'item_label_id'],
            dropna=False,
            group_keys=False
        )
        data = data.apply(
            _expand_shorthand_items,
            entry_syntax,
            entry_prefix_id_map,
            item_label_id_map
        )

        # Locate items that do not have a list delimiter
        item_delimited = data['string'].map(
            pd.api.types.is_list_like
        )
        item_not_delimited = data.loc[~item_delimited].index

        # Explode the delimited strings
        data = data.explode('string')

        # Make a copy of the index, drop values that do not refer to
        # delimited items, then groupby the remaining index values and
        # do a cumulative count to get the position of each element in
        # each item that has a list delimiter
        itm_list_pos = data.index
        itm_list_pos = pd.Series(
            itm_list_pos.array,
            index=itm_list_pos,
            dtype=small_id_dtype
        )

        itm_list_pos.loc[itm_list_pos.isin(item_not_delimited)] = pd.NA
        itm_list_pos = itm_list_pos.dropna().groupby(itm_list_pos.dropna())
        itm_list_pos = itm_list_pos.cumcount()
        # Shift the values by the list position base
        itm_list_pos = itm_list_pos + list_position_base

        itm_list_pos = itm_list_pos.astype(small_id_dtype)

        # Store the item list positions and reset the index
        itm_list_pos = itm_list_pos.array
        data.loc[item_delimited, 'item_list_position'] = itm_list_pos
        data = data.reset_index(drop=True)

        '''**********************************************************
        data is currently a DataFrame with these columns:
            ['csv_row', 'csv_col', 'entry_prefix_id', 'item_label_id',
             'string', 'node_type', 'link_type_id', 'node_tags',
             'item_list_position']

        Done processing entry strings.
        Replace string values in data with integer ID values.
        **********************************************************'''

        # The strings dataframe is a relation between a string value
        # and a node type. Its index is integer string IDs
        strings = data[['string', 'node_type']].drop_duplicates(
            subset='string'
        )
        strings = strings.reset_index(drop=True)
        strings.index = strings.index.astype(big_id_dtype)

        # Drop node types from the mutable data
        data = data.drop('node_type', axis='columns')

        # Replace strings in the mutable data with integer IDs
        data['string'] = data['string'].map(
            pd.Series(strings.index, index=strings['string'])
        )
        data = data.rename(columns={'string': 'string_id'})

        '''
        SWITCHING TO LITERAL_CSV AND LITERAL_PYTHON
        # These node types will be required later
        node_types = pd.Series([
            'shorthand_text',
            'shorthand_entry_syntax',
            'shorthand_link_syntax',
            'python_function'
        ])

        # Map string-valued node types to integer IDs
        node_types = _create_id_map(
            pd.concat([node_types, strings['node_type']]),
            dtype=small_id_dtype
        )'''
        # Map string-valued node types to integer IDs
        node_types = _create_id_map(
            pd.concat([strings['node_type'], pd.Series(['tag'])]),
            dtype=small_id_dtype
        )

        # Replace string-valued node types in the strings dataframe with
        # integer IDs
        strings['node_type'] = strings['node_type'].map(node_types)
        strings = strings.rename(columns={'node_type': 'node_type_id'})
        strings = strings.astype({
            'string': str,
            'node_type_id': small_id_dtype
        })

        # Mutate node_types into a series whose index is integer IDs and
        # whose values are string-valued node types
        node_types = pd.Series(node_types.index, index=node_types)

        '''**********************************************************
        data is currently a DataFrame with these columns:
            ['csv_row', 'csv_col', 'entry_prefix_id', 'item_label_id',
             'string_id', 'link_type_id', 'node_tags',
             'item_list_position']

        id columns and item_list_position are integer-valued.

        Generate links defined by the entry and link syntax
        **********************************************************'''

        columns_required_for_links = [
            'csv_row', 'csv_col', 'string_id', 'link_type_id'
        ]

        # If the entry syntax indicated that there should be links
        # between an item and the string that contains it, get the
        # string ID of the entry
        links = data[columns_required_for_links].groupby(
            by=['csv_row', 'csv_col'],
            group_keys=False
        )
        links = links.apply(_get_item_link_source_IDs)
        links.index = data.index.copy()

        # A shorthand link is a relation between four entities
        # represented by integer IDs:
        #
        # src_string_id (string representing the source end of the link)
        # tgt_string_id (string representing the target end of the link)
        # ref_string_id (string representing the context of the link)
        # link_type_id (the link type)
        #
        # To generate these we first locate items with links to their
        # own entry strings and then treat the entry strings as source
        # strings.
        has_link = ~data['link_type_id'].isna()

        # The reference string for links between items and the entry
        # containing them is the full text of the input file, which is
        # not in the current data set, so set reference strings to null.
        links = pd.DataFrame({
            'src_string_id': links.loc[has_link].array,
            'ref_string_id': pd.NA
        })

        # Get the target string, link type, and list position from the
        # data set
        data_cols = ['string_id', 'link_type_id', 'item_list_position']
        links = pd.concat(
            [links, data.loc[has_link, data_cols].reset_index(drop=True)],
            axis='columns'
        )
        links = links.rename(columns={'string_id': 'tgt_string_id'})

        # Every entry string in a shorthand text is also the target of a
        # link of type 'entry' whose source is the full text of the
        # input file. This allows direct selection of the original entry
        # strings. The text of the input file is not in the current
        # data set, so set the source strings to null.
        entry_tgt_ids = data.loc[data['item_label_id'].isna(), 'string_id']
        entry_links = pd.DataFrame({
            'src_string_id': pd.NA,
            'tgt_string_id': entry_tgt_ids.drop_duplicates().array,
            'ref_string_id': pd.NA,
            'link_type_id': link_types.loc[link_types == 'entry'].index[0],
            'item_list_position': pd.NA
        })
        entry_links = bg.util.normalize_types(entry_links, links)
        links = pd.concat([links, entry_links])

        # If the caller gave a link syntax, parse it
        if 'link_syntax' in dir(self):

            link_types, link_syntax = bg.syntax_parsing.parse_link_syntax(
                self.link_syntax,
                self.entry_syntax,
                entry_prefix_id_map,
                link_types,
                item_label_id_map,
                case_sensitive=self.syntax_case_sensitive
            )

        try:

            # Escape any regex metacharacters in the item separator so
            # we can use it in regular expressions
            regex_item_separator = bg.util.escape_regex_metachars(
                item_separator
            )
            link_type_regex = rf"^(?:.*?)(lt{regex_item_separator}\S+)"

            # Extract the link type overrides from the link metadata
            # with a regular expression.
            # TAKES ONLY THE FIRST MATCH, OTHERS CONSIDERED TAGS
            # If link_metadata is empty then the next line of code
            # throws an AttributeError and we jump to the next except
            # block
            l_t_overrides = link_metadata.str.extract(link_type_regex)
            l_t_overrides = l_t_overrides.stack().dropna()
            l_t_overrides.index = l_t_overrides.index.droplevel(1)
            l_t_overrides = l_t_overrides.str.split(
                item_separator,
                expand=True
            )

            # If we found any new link types, process them, otherwise
            # the next line throws a KeyError and we jump to the next
            # except block
            l_t_overrides = l_t_overrides[1]

            # Add overridden types to the link types series
            new_link_types = l_t_overrides.loc[
                ~l_t_overrides.isin(link_types)
            ]
            link_types = pd.concat([
                link_types,
                bg.util.normalize_types(new_link_types, link_types)
            ])

            # Map string-valued type overrides to integer link
            # type IDs
            l_t_overrides = l_t_overrides.map(
                pd.Series(link_types.index, index=link_types.array)
            )

        except (AttributeError, KeyError):
            # Either link_metadata is empty or l_t_overrides
            # had only one column, and there are no new links to
            # process
            pass

        try:
            assert link_syntax is not None

            # Get entry prefixes for each side of links defined in the
            # link syntax
            left_prefixes = _get_entry_prefix_ids(
                'left',
                data,
                dplct_entries,
                csv_column_id_map
            )
            right_prefixes = _get_entry_prefix_ids(
                'right',
                data,
                dplct_entries,
                csv_column_id_map
            )

            # Recover entry prefix IDs for duplicate entries whose
            # original string value is in a different csv column
            _copy_cross_duplicates(left_prefixes, right_prefixes)

            # Done with the "entry" csv columns, so drop them
            left_prefixes = left_prefixes.drop('entry_csv_col', axis='columns')
            right_prefixes = right_prefixes.drop(
                'entry_csv_col', axis='columns'
            )

            left_prefixes = left_prefixes.rename(
                columns={
                    'string_csv_row': 'L_str_csv_row',
                    'string_csv_col': 'L_str_csv_col'
                }
            )

            right_prefixes = right_prefixes.rename(
                columns={
                    'string_csv_row': 'R_str_csv_row',
                    'string_csv_col': 'R_str_csv_col'
                }
            )

            # Pair up the prefixes so we can generate links from the
            # link syntax
            prefix_pairs = left_prefixes.merge(right_prefixes)

            # Get string IDs for links whose sources and targets are
            # matched one-to-one according to the link syntax
            link_has_no_list = link_syntax['list_mode'].isna()
            link_is_one_to_one = (link_syntax['list_mode'] == '1:1')
            list_mode_subset = link_has_no_list | link_is_one_to_one

            sources = _get_link_component_string_ids(
                prefix_pairs,
                data,
                link_syntax,
                'src_',
                subset=list_mode_subset,
                columns=['entry_csv_row', 'string_id', 'link_type_id']
            )
            targets = _get_link_component_string_ids(
                prefix_pairs,
                data,
                link_syntax,
                'tgt_',
                subset=list_mode_subset,
                columns=['string_id', 'item_list_position']
            )
            references = _get_link_component_string_ids(
                prefix_pairs,
                data,
                link_syntax,
                'ref_',
                subset=list_mode_subset,
                columns=['entry_csv_row', 'string_id']
            )

            one_to_one_links = pd.concat([sources, targets], axis='columns')
            one_to_one_links = one_to_one_links.merge(
                references,
                on='entry_csv_row',
                how='left'
            )

            # Get string IDs for links whose sources and targets are not
            # matched one-to-one
            list_mode_subset = link_syntax['list_mode'].isin(
                ['1:m', 'm:1', 'm:m']
            )

            sources = _get_link_component_string_ids(
                prefix_pairs,
                data,
                link_syntax,
                'src_',
                subset=list_mode_subset,
                columns=['entry_csv_row', 'string_id', 'link_type_id']
            )
            hashed_srcs = pd.util.hash_pandas_object(sources, index=False)
            sources = sources.loc[~hashed_srcs.duplicated()]

            targets = _get_link_component_string_ids(
                prefix_pairs,
                data,
                link_syntax,
                'tgt_',
                subset=list_mode_subset,
                columns=['entry_csv_row', 'string_id', 'item_list_position']
            )
            hashed_tgts = pd.util.hash_pandas_object(targets, index=False)
            targets = targets.loc[~hashed_tgts.duplicated()]

            references = _get_link_component_string_ids(
                prefix_pairs,
                data,
                link_syntax,
                'ref_',
                subset=list_mode_subset,
                columns=['entry_csv_row', 'string_id']
            )
            hashed_refs = pd.util.hash_pandas_object(references, index=False)
            references = references.loc[~hashed_refs.duplicated()]

            other_links = sources.merge(targets, on='entry_csv_row')
            # other_links = other_links.merge(references, on='entry_csv_row')
            other_links['ref_string_id'] = other_links['entry_csv_row'].map(
                pd.Series(
                    references['ref_string_id'].array,
                    index=references['entry_csv_row'].array
                )
            )

            one_to_one_links = bg.util.normalize_types(
                one_to_one_links,
                links,
                strict=False
            )
            other_links = bg.util.normalize_types(
                other_links,
                links,
                strict=False
            )
            links = pd.concat([links, one_to_one_links, other_links])

            links = links.reset_index(drop=True)
            links = links.astype({
                'src_string_id': big_id_dtype,
                'tgt_string_id': big_id_dtype,
                'ref_string_id': big_id_dtype,
                'link_type_id': small_id_dtype,
                'entry_csv_row': big_id_dtype,
                'item_list_position': small_id_dtype
            })

        except NameError:
            # This NameError should have been generated by invoking
            # link_syntax when there is no link syntax available.
            # If we don't have a link syntax, move on.
            pass

        # If there were lines with self-descriptive entries and
        # link metadata, and the node types for one or both entries
        # are not in the link syntax, we have to create new links for
        # those lines.
        if make_s_d_links:

            possible_entries = pd.concat([
                has_s_d_entry_and_link_metadata['left_entry'],
                has_s_d_entry_and_link_metadata['right_entry'],
                has_s_d_entry_and_link_metadata['reference']
            ])

            string_to_map = (strings['string'].isin(possible_entries))

            string_map = strings['string'].loc[string_to_map]
            string_map = pd.Series(string_map.index, index=string_map.array)

            src_strings = has_s_d_entry_and_link_metadata['left_entry'].map(
                string_map
            )
            tgt_strings = has_s_d_entry_and_link_metadata['right_entry'].map(
                string_map
            )
            ref_strings = has_s_d_entry_and_link_metadata['reference'].map(
                string_map
            )

            new_links = pd.DataFrame({
                'src_string_id': src_strings.array,
                'tgt_string_id': tgt_strings.array,
                'ref_string_id': ref_strings.array,
                'link_type_id': pd.NA,
                'entry_csv_row': has_s_d_entry_and_link_metadata.index,
                'item_list_position': pd.NA
            })
            new_links = bg.util.normalize_types(new_links, links)

            links = pd.concat([links, new_links])

        if 'entry_csv_row' in links.columns:

            # If we applied a link syntax above or processed links for
            # self-descriptive entries then we created a column in the
            # links frame for the entry_csv_row. We need to match up the
            # link metadata with links rather than csv rows and drop the
            # csv information from the links frame.

            try:
                assert l_t_overrides.empty is False

                # Replace overriden link type IDs if they exist
                l_t_overrides = links['entry_csv_row'].dropna().map(
                    l_t_overrides
                )
                links['link_type_id'].update(l_t_overrides.dropna())

            except (AssertionError, NameError):
                # If we get here then there were no link type
                # overrides, so move on
                pass

            # The link metadata index currently refers to csv rows.
            # Replace it with integer link index values.
            row_map = links.query('entry_csv_row.isin(@link_metadata.index)')
            row_map = pd.Series(
                row_map.index,
                index=row_map['entry_csv_row'].array
            )
            link_metadata.index = link_metadata.index.map(row_map)

            # We're done with the csv information
            links = links.drop('entry_csv_row', axis='columns')

        # The item_list_position label is probably only intelligible
        # when handling items within entries, so rename it
        links = links.rename(columns={'item_list_position': 'list_position'})

        '''***********************************************************
        Done with links generated from syntax definitions.

        Extract tag strings and create links between strings and tags.
        ***********************************************************'''

        # Cache the node type ID for tag strings
        tag_node_type_id = node_types.loc[node_types == 'tag'].index[0]

        # Cache the type ID for tag links
        tagged_link_type = link_types.loc[link_types == 'tagged'].index[0]

        # PROCESS NODE TAGS

        # Make a relation between node tag strings and ID values for the
        # tagged strings
        tags = data.loc[
            data['node_tags'].notna(),
            ['string_id', 'node_tags']
        ]
        tags = pd.Series(tags['node_tags'].array, index=tags['string_id'])
        tags = tags.str.split().explode().drop_duplicates()

        # tags is now a pandas.Series whose index is string IDs and
        # whose values are individual tag strings

        # Tag strings should be added to the strings frame if they are
        # not present in the strings frame OR if they are present and
        # the existing string has a node type that isn't the tag node
        # type
        new_strings = bg.util.get_new_typed_values(
            tags,
            strings,
            'string',
            'node_type_id',
            tag_node_type_id
        )
        new_strings = pd.DataFrame(
            {'string': new_strings.array, 'node_type_id': tag_node_type_id},
            index=tags.array
        )
        new_strings = bg.util.normalize_types(new_strings, strings)

        strings = pd.concat([strings, new_strings])

        # convert the tag strings to string ID values
        tags = tags.map(
            pd.Series(new_strings.index, index=new_strings['string'])
        )

        # Add links for the tags. Reference string IDs are null because
        # the reference string for a tag is the full text of the input
        # file, which is inserted by the caller.
        new_links = pd.DataFrame({
            'src_string_id': tags.index,
            'tgt_string_id': tags.array,
            'ref_string_id': pd.NA,
            'link_type_id': tagged_link_type
        })
        new_links = bg.util.normalize_types(new_links, links)

        links = pd.concat([links, new_links])

        # PROCESS LINK TAGS

        # If there was link metadata, get tag strings out of it
        try:
            assert link_metadata.empty is False

            # Extract the link tags from the link metadata with a
            # regular expression
            link_tag_regex = rf"lt{regex_item_separator}\S+"
            link_tags = link_metadata.str.replace(
                link_tag_regex,
                '',
                n=1,
                regex=True
            )
            link_tags = link_tags.loc[link_tags != '']

        # If there was no link metadata, we currently have no link tags
        except (NameError, AssertionError):
            link_tags = pd.Series(dtype='object')

        # Convert any item list positions into strings and append them
        # to the link tag strings
        list_pos = links['list_position'].dropna().astype(str)
        has_list_pos_and_tags = list_pos.index.intersection(
            link_tags.index
        )
        list_pos_but_no_tags = list_pos.index.difference(link_tags.index)

        if has_list_pos_and_tags.empty:
            link_tags = pd.concat([link_tags, list_pos])

        elif list_pos.index.difference(has_list_pos_and_tags).empty:
            link_tags.loc[list_pos.index] = [
                ' '.join(pair) for pair in
                zip(link_tags.loc[list_pos.index], list_pos)
            ]

        else:
            pairs = zip(
                link_tags.loc[has_list_pos_and_tags],
                list_pos.loc[has_list_pos_and_tags]
            )
            link_tags.loc[has_list_pos_and_tags] = [
                ' '.join(pair) for pair in pairs
            ]

            link_tags = pd.concat([
                link_tags,
                list_pos.loc[list_pos_but_no_tags]
            ])

        links = links.drop('list_position', axis='columns')

        # Convert the space-delimited tag strings to lists of strings
        link_tags = link_tags.str.split().explode()

        # Add the tag strings to the rest of the strings
        new_strings = link_tags.drop_duplicates()
        new_strings = bg.util.get_new_typed_values(
            new_strings,
            strings,
            'string',
            'node_type_id',
            tag_node_type_id
        )
        new_strings = pd.DataFrame(
            {'string': new_strings.array, 'node_type_id': tag_node_type_id}
        )
        new_strings = bg.util.normalize_types(new_strings, strings)

        strings = pd.concat([strings, new_strings])

        # convert the tag strings to string ID values
        tag_strings = strings.loc[
            strings['node_type_id'] == tag_node_type_id
        ]
        link_tags = link_tags.map(
            pd.Series(tag_strings.index, index=tag_strings['string'])
        )

        # Relations between links and string-valued tags stored in the
        # strings frame aren't representable as links in the links
        # frame, so make a many-to-many frame relating link ID values to
        # string IDs
        link_tags = pd.DataFrame({
            'link_id': link_tags.index,
            'tag_string_id': link_tags.array
        })

        '''************
        Done with tags.
        ************'''

        tn = bg.TextNet()

        tn.strings = strings
        tn.reset_strings_dtypes()

        tn.node_types = pd.DataFrame(columns=tn._node_types_dtypes.keys())
        tn.node_types['node_type'] = node_types.array
        tn.node_types['null_type'] = False
        type_is_null = (tn.node_types['node_type'] == na_node_type)
        tn.node_types.loc[type_is_null, 'null_type'] = True
        tn.node_types['has_metadata'] = False
        tn.reset_node_types_dtypes()

        input_string_id = tn.insert_string(
            input_string,
            input_node_type,
            add_node_type=True
        )

        links['inp_string_id'] = input_string_id

        tn.link_types = pd.DataFrame(columns=tn._link_types_dtypes.keys())
        tn.link_types['link_type'] = link_types.array
        tn.link_types['null_type'] = False
        tn.reset_link_types_dtypes()

        tn.assertions = links
        tn.reset_assertions_dtypes()

        tagged_link_type_id = tn.id_lookup('link_types', 'tagged')
        if tagged_link_type_id not in tn.assertions['link_type_id'].array:
            tn.link_types = tn.link_types.query('link_type != "tagged"')

        tn.assertion_tags = link_tags
        tn.assertion_tags = tn.assertion_tags.rename(
            columns={'link_id': 'assertion_id'}
        )
        tn.reset_assertion_tags_dtypes()

        entry_prefixes_string_id = tn.insert_string(
            str(list(entry_prefix_id_map.index)),
            '_literal_python',
            add_node_type=True
        )
        entry_prefixes_link_type_id = tn.insert_link_type(
            'shorthand_entry_prefixes'
        )

        tn.insert_assertion(
            input_string_id,
            input_string_id,
            entry_prefixes_string_id,
            input_string_id,
            entry_prefixes_link_type_id
        )

        item_labels_string_id = tn.insert_string(
            str(list(item_label_id_map.index)),
            '_literal_python'
        )
        item_labels_link_type_id = tn.insert_link_type(
            'shorthand_item_labels'
        )

        tn.insert_assertion(
            input_string_id,
            input_string_id,
            item_labels_string_id,
            input_string_id,
            item_labels_link_type_id
        )

        return tn

    def parse_text(
        self,
        filepath_or_buffer,
        item_separator,
        default_entry_prefix,
        space_char,
        na_string_values,
        na_node_type,
        input_string,
        input_node_type,
        skiprows=0,
        comment_char='#',
        fill_cols='left_entry',
        drop_na='right_entry',
        big_id_dtype=pd.Int32Dtype(),
        small_id_dtype=pd.Int8Dtype(),
        list_position_base=1,
        s_d_delimiter='_',
        encoding='utf8'
    ):
        ####################
        # Validate arguments
        ####################

        # We need to use the contents of the input file for parsing but
        # we also need to access the text after parsing. If we're passed
        # a buffer then it will be empty after it's passed to
        # pandas.read_csv, so we write it to a temp file.
        if 'read' in dir(filepath_or_buffer):

            tempfile_path = tempfile.mkstemp(text=True)[1]

            with open(tempfile_path, 'w') as f:
                f.write(filepath_or_buffer.read())

            filepath_or_buffer = tempfile_path

        if space_char is not None:
            space_char = str(space_char)

            if len(space_char) > 1:
                raise ValueError('space_char must be a single character')

        if not bg.util.iterable_not_string(na_string_values):
            na_string_values = [na_string_values]

        comment_char = str(comment_char)
        if len(comment_char) > 1:
            raise ValueError('comment_char must be a single character')

        if not bg.util.iterable_not_string(fill_cols):
            if fill_cols is None:
                fill_cols = []

            fill_cols = [fill_cols]

        if not bg.util.iterable_not_string(drop_na):
            if drop_na is None or drop_na is False:
                drop_na = []

            drop_na = [drop_na]

        if not pd.api.types.is_integer_dtype(big_id_dtype):
            raise ValueError(
                'big_id_dtype must be an integer type recognized by '
                'pandas.api.types.is_integer_dtype'
            )

        if not pd.api.types.is_integer_dtype(small_id_dtype):
            raise ValueError(
                'small_id_dtype must be an integer type recognized by '
                'pandas.api.types.is_integer_dtype'
            )

        list_position_base = int(list_position_base)

        ###########################
        # Done validating arguments
        ###########################

        # Parse input text
        parsed = self._apply_syntax(
            filepath_or_buffer,
            item_separator,
            default_entry_prefix,
            space_char,
            na_string_values,
            na_node_type,
            input_string,
            input_node_type,
            skiprows,
            comment_char,
            fill_cols,
            drop_na,
            big_id_dtype,
            small_id_dtype,
            list_position_base,
            s_d_delimiter,
            encoding
        )

        # Read input text
        try:
            with open(tempfile_path, 'r') as f:
                full_text_string = f.read()
        except NameError:
            with open(filepath_or_buffer, 'r') as f:
                full_text_string = f.read()

        '''
        SWITCHING TO LITERAL NODE TYPE
        full_txt_string_id = parsed.insert_string(
            full_text_string,
            'shorthand_text',
            add_node_type=True
        )

        call_string_id = parsed.id_lookup('strings', input_string)
        entry_syntax_string_id = parsed.insert_string(
            self.entry_syntax,
            'shorthand_entry_syntax',
            add_node_type=True
        )

        requires_link_type_id = parsed.insert_link_type('requires')

        parsed.insert_assertion(
            call_string_id,
            full_txt_string_id,
            entry_syntax_string_id,
            call_string_id,
            requires_link_type_id
        )
        parsed.insert_assertion(
            call_string_id,
            call_string_id,
            entry_syntax_string_id,
            call_string_id,
            requires_link_type_id
        )'''

        # Create a string for the data and a link from the input string
        call_string_id = parsed.id_lookup('strings', input_string)
        full_txt_string_id = parsed.insert_string(
            full_text_string,
            '_literal_csv',
            add_node_type=True
        )
        shorthand_data_link_id = parsed.insert_link_type('shorthand_data')
        call_string_id = parsed.id_lookup('strings', input_string)
        parsed.insert_assertion(
            call_string_id,
            call_string_id,
            full_txt_string_id,
            call_string_id,
            shorthand_data_link_id
        )

        # Create a string for the entry syntax and a link from the
        # input string
        entry_syntax_string_id = parsed.insert_string(
            self.entry_syntax,
            '_literal_csv'
        )
        shorthand_entry_syntax_link_id = parsed.insert_link_type(
            'shorthand_entry_syntax'
        )
        parsed.insert_assertion(
            call_string_id,
            call_string_id,
            entry_syntax_string_id,
            call_string_id,
            shorthand_entry_syntax_link_id
        )

        try:
            self.link_syntax

            '''
            SWITCHING TO LITERAL NODE TYPE
            link_syntax_string_id = parsed.insert_string(
                self.link_syntax,
                'shorthand_link_syntax',
                add_node_type=True
            )

            parsed.insert_assertion(
                call_string_id,
                full_txt_string_id,
                link_syntax_string_id,
                call_string_id,
                requires_link_type_id
            )
            parsed.insert_assertion(
                call_string_id,
                call_string_id,
                link_syntax_string_id,
                call_string_id,
                requires_link_type_id
            )'''
            # Create a string for the entry syntax and a link from the
            # input string
            link_syntax_string_id = parsed.insert_string(
                self.link_syntax,
                '_literal_csv'
            )
            shorthand_link_syntax_link_id = parsed.insert_link_type(
                'shorthand_link_syntax'
            )
            parsed.insert_assertion(
                call_string_id,
                call_string_id,
                link_syntax_string_id,
                call_string_id,
                shorthand_link_syntax_link_id
            )

        except AttributeError:
            pass

        # Assertions whose reference string ID is missing should have the
        # input file as their reference string
        ref_isna = parsed.assertions['ref_string_id'].isna()
        parsed.assertions.loc[ref_isna, 'ref_string_id'] = full_txt_string_id

        # Assertions whose source string ID is missing should have the input
        # file as their source string
        src_isna = parsed.assertions['src_string_id'].isna()
        parsed.assertions.loc[src_isna, 'src_string_id'] = full_txt_string_id

        return parsed

    def parse_items(
        self,
        data,
        input_string,
        input_node_type,
        space_char,
        na_string_values,
        na_node_type,
        item_separator=None,
        entry_writer=None,
        entry_node_type=None,
        entry_prefix=None,
        big_id_dtype=pd.Int32Dtype(),
        small_id_dtype=pd.Int8Dtype(),
        comma_separated=True,
        list_position_base=1
    ):

        data = data.reset_index(drop=True)

        entry_syntax = bg.syntax_parsing.validate_entry_syntax(
            self.entry_syntax,
            case_sensitive=self.syntax_case_sensitive,
            allow_redundant_items=self.allow_redundant_items
        )

        if space_char is not None:
            space_char = str(space_char)

            if len(space_char) > 1:
                raise ValueError('space_char must be a single character')

        if not bg.util.iterable_not_string(na_string_values):
            na_string_values = [na_string_values]

        if 'entry_prefix' in entry_syntax.columns:
            try:
                entry_prefix = bg.util.get_single_value(
                    entry_syntax,
                    'entry_prefix'
                )
                entry_node_type = bg.util.get_single_value(
                    entry_syntax,
                    'entry_node_type'
                )
            except ValueError as e:
                if 'multiple values' in e.message:
                    if entry_prefix is None:
                        raise ValueError(
                            'Must provide an entry prefix when parsing '
                            'items using a syntax with multiple entry '
                            'prefixes.'
                        )
                    else:
                        entry_syntax = entry_syntax.query(
                            'entry_prefix == @entry_prefix'
                        )
                        entry_node_type = bg.util.get_single_value(
                            entry_syntax,
                            'entry_node_type'
                        )
                elif 'only nan' in e.message:
                    if entry_node_type is None:
                        entry_node_type = bg.util.get_single_value(
                            entry_syntax,
                            'entry_node_type'
                        )

        elif entry_node_type is None:
            entry_node_type = bg.util.get_single_value(
                entry_syntax,
                'entry_node_type'
            )

        # create a map from item labels included in the entry syntax to
        # node types and link types
        item_types = pd.DataFrame(
            {
                'node_type': entry_syntax['item_node_type'].array,
                'link_type': entry_syntax['item_link_type'].array
            },
            index=entry_syntax['item_label'].array
        )
        common_labels = [
            lbl for lbl in data.columns if lbl in item_types.index
        ]
        item_types = item_types.loc[common_labels]

        if entry_writer is None:

            if comma_separated:
                item_separator = ', '
            elif item_separator is None:
                raise ValueError(
                    'If comma_separated is not True, provide a '
                    'separator with the item_separator keyword '
                    'argument.'
                )

            entries = data.fillna(na_string_values[0])
            entries = entries.apply(
                lambda x: item_separator.join(map(str, x)),
                axis=1
            )

            if comma_separated:
                entries = entries.apply(lambda x: '"' + x + '"')

        else:

            entries = data.apply(entry_writer, axis='columns')

        # drop data columns not mentioned in the syntax
        data = data[common_labels]

        entries = pd.DataFrame({
            'string': entries.array,
            'node_type': entry_node_type
        })
        entries = entries.reset_index().rename(columns={'index': 'csv_row'})

        # Replace NA values and empty strings with the first string in
        # na_string_values
        data = data.fillna(na_string_values[0])
        data = data.replace('', na_string_values[0])

        # items with no node type in the entry syntax are prefixed to
        # indicate which node type they correspond to
        item_is_prefixed = entry_syntax['item_node_type'].isna()

        if item_is_prefixed.any():

            prefixed_items = entry_syntax.loc[item_is_prefixed]
            labels_of_prefixed_items = prefixed_items['item_label'].array

            # stack the prefixed items into a series
            disagged = data[labels_of_prefixed_items].stack()

            # Split the prefixes off of the stacked items and expand
            # into a dataframe
            disagged = disagged.groupby(level=1).apply(
                bg.entry_parsing._item_prefix_splitter,
                prefixed_items
            )

            # drop the item labels from the multiindex so the
            # disaggregated items align with the index of the entry
            # group
            disagged.index = disagged.index.droplevel(1)

            # pivot the disaggregated items to create a dataframe with
            # columns for each item prefix
            disagged = disagged.pivot(columns=0)
            disagged.columns = disagged.columns.get_level_values(1)

            # get labels of items that are not prefixed and present in
            # this dataset
            unprefixed_item_labels = [
                label for label in entry_syntax['item_label']
                if label.isdigit()
                and label in data.columns
                and label not in labels_of_prefixed_items
            ]

            # select only the unprefixed item labels
            data = data[unprefixed_item_labels]
            # concatenate the unprefixed and prefixed items
            data = pd.concat([data, disagged], axis='columns')

        # Replace any empty strings with null values
        data = data.mask(data == '', pd.NA)

        # Regular expressions to match bare and escaped space
        # placeholders
        regex_space_char = bg.util.escape_regex_metachars(space_char)
        space_plchldr_regex = r"(?<!\\)({})".format(regex_space_char)
        escaped_space_plchldr_regex = fr"(\\{regex_space_char})"

        # Replace space placeholders with spaces in the data items
        data = data.replace(
            to_replace=space_plchldr_regex,
            value=' ',
            regex=True
        )
        # Replace escaped space placeholders with bare placeholders
        data = data.replace(
            to_replace=escaped_space_plchldr_regex,
            value=regex_space_char,
            regex=True
        )

        # Stack data. Stacking creates a series whose values are the
        # string values of every item in the input and whose index
        # levels are
        #       input index, item label
        data = data.stack()

        data = data.reset_index()
        data = data.rename(
            columns={
                'level_0': 'csv_row',
                'level_1': 'item_label',
                0: 'string'
            }
        )
        data = data.merge(item_types, left_on='item_label', right_index=True)
        # Concatenate expanded items with the entries
        data = pd.concat([data, entries]).fillna(pd.NA)

        # For any strings that represent null values, overwrite the node
        # type inferred from the syntax with the null node type
        null_strings = data['string'].isin(na_string_values)
        data.loc[null_strings, 'node_type'] = na_node_type

        dtypes = {
            'csv_row': big_id_dtype,
            'item_label': pd.StringDtype(),
            'string': pd.StringDtype(),
            'node_type': pd.StringDtype(),
            'link_type': pd.StringDtype()
        }

        data = data.astype(dtypes)
        data.index = data.index.astype(big_id_dtype)

        '''
        data is currently a DataFrame with these columns:
        [
            'csv_row', 'item_label', 'string', 'node_type', 'link_type'
        ]
        csv_row is integer-valued, others are pd.StringDtype
        '''
        # Map string-valued item labels to integer IDs
        item_label_id_map = _create_id_map(
            data['item_label'],
            dtype=small_id_dtype
        )
        # Replace item labels in the mutable data with integer IDs
        data['item_label'] = data['item_label'].map(
            item_label_id_map
        )
        data = data.rename(
            columns={'item_label': 'item_label_id'}
        )

        # These link types are required to complete linking operations
        # later
        '''
        SWITCHING TO LITERAL NODE TYPE
        link_types = pd.Series(['entry', 'tagged', 'requires'])
        '''
        link_types = pd.Series(['entry', 'tagged'])

        # Map string-valued link types to integer IDs
        link_types = _create_id_map(
            pd.concat([link_types, data['link_type']]),
            dtype=small_id_dtype
        )
        # Replace link types in the mutable data with integer IDs
        data['link_type'] = data['link_type'].map(
            link_types
        )
        data = data.rename(
            columns={'link_type': 'link_type_id'}
        )
        # Mutate link_types into a series whose index is integer IDs and
        # whose values are string-valued link types
        link_types = pd.Series(link_types.index, index=link_types)

        '''
        data is currently a DataFrame with these columns:
        [
            'csv_row', 'item_label_id', 'string',
            'node_type', 'link_type_id'
        ]
        csv_row, item_label_id, and link_type_id are integer-valued,
        others are pd.StringDtype
        '''

        # Make a map from item label IDs and list delimiters
        delimiters = pd.Series(
            entry_syntax['list_delimiter'].array,
            index=entry_syntax['item_label'].map(item_label_id_map)
        )

        # Split items by delimiter
        data = data.groupby(by='item_label_id', dropna=False, group_keys=False)
        data = data.apply(bg.entry_parsing._expand_csv_items, delimiters)

        data = data.reset_index(drop=True)

        # Locate items that do not have a list delimiter
        item_is_delimited = data['string'].map(
            pd.api.types.is_list_like
        )
        item_not_delimited = data.loc[~item_is_delimited].index

        # Explode the delimited strings
        data = data.explode('string').reset_index()

        # Make a copy of the index, drop values that do not refer to
        # delimited items, then groupby the remaining index values and
        # do a cumulative count to get the position of each element in
        # each item that has a list delimiter
        item_list_pos = data['index'].copy()
        # item_list_pos = pd.Series(
        #     item_list_pos.array,
        #     index=item_list_pos
        # )

        item_list_pos.loc[item_list_pos.isin(item_not_delimited)] = pd.NA
        item_list_pos = item_list_pos.dropna().groupby(item_list_pos.dropna())
        item_list_pos = item_list_pos.cumcount()

        # Shift the values by the list position base
        item_list_pos = item_list_pos + list_position_base

        item_list_pos = item_list_pos.astype(small_id_dtype)
        # Store the item list positions and reset the index
        data = data.drop('index', axis='columns')
        data = pd.concat(
            [data, item_list_pos.rename('item_list_position')],
            axis=1
        )

        '''
        data is currently a DataFrame with these columns:
        [
            'csv_row', 'item_label_id', 'string', 'node_type',
            'link_type_id', 'item_list_position'
        ]
        string and node_type are pd.StringDtype dtype, others are
        integer
        '''

        # The strings dataframe is a relation between a string value
        # and a node type. Its index is integer string IDs
        strings = data[['string', 'node_type']].drop_duplicates(
            subset='string'
        )
        strings = strings.reset_index(drop=True)
        strings.index = strings.index.astype(big_id_dtype)

        # Drop node types from the mutable data
        data = data.drop('node_type', axis='columns')

        # Replace strings in the mutable data with integer IDs
        data['string'] = data['string'].map(
            pd.Series(strings.index, index=strings['string'])
        )
        data = data.rename(columns={'string': 'string_id'})

        '''
        SWITCHING TO LITERAL NODE TYPE
        # These node types will be required later
        node_types = pd.Series([
            'items_text',
            'shorthand_entry_syntax',
            'python_function',
            'tag'
        ])

        # Map string-valued node types to integer IDs
        node_types = _create_id_map(
            pd.concat([node_types, strings['node_type']]),
            dtype=small_id_dtype
        )'''
        # Map string-valued node types to integer IDs
        node_types = _create_id_map(
            pd.concat([strings['node_type'], pd.Series(['tag'])]),
            dtype=small_id_dtype
        )

        # Replace string-valued node types in the strings dataframe with
        # integer IDs
        strings['node_type'] = strings['node_type'].map(node_types)
        strings = strings.rename(columns={'node_type': 'node_type_id'})
        strings = strings.astype({
            'string': str,
            'node_type_id': small_id_dtype
        })

        # Mutate node_types into a series whose index is integer IDs and
        # whose values are string-valued node types
        node_types = pd.Series(node_types.index, index=node_types)

        '''
        data is currently a DataFrame with these columns:
        [
            'csv_row', 'item_label_id', 'string_id' 'link_type_id',
            'item_list_position'
        ]
        all are integer-valued
        '''

        data = data.sort_values('csv_row')

        # If the entry syntax indicated that there should be links
        # between an item and the string that contains it, get the
        # string ID of the entry
        if len(data['csv_row'].unique()) > 1:
            links = data[['csv_row', 'string_id', 'link_type_id']].groupby(
                by='csv_row',
                group_keys=False
            )
            links = links.apply(_get_item_link_source_IDs)
        else:
            link_type_is_na = data['link_type_id'].isna()
            entry_string_id = data.loc[link_type_is_na, 'string_id'].squeeze()
            links = pd.Series([entry_string_id]*len(data))

        links.index = data.index.copy()

        # A shorthand link is a relation between four entities
        # represented by integer IDs:
        #
        # src_string_id (string representing the source end of the link)
        # tgt_string_id (string representing the target end of the link)
        # ref_string_id (string representing the context of the link)
        # link_type_id (the link type)
        #
        # To generate these we first locate items with links to their
        # own entry strings and then treat the entry strings as source
        # strings.
        has_link = ~data['link_type_id'].isna()

        # The reference string for links between items and the entry
        # containing them is the full text of the input file, which is
        # not in the current data set, so set reference strings to null.
        links = pd.DataFrame({
            'src_string_id': links.loc[has_link].array,
            'ref_string_id': pd.NA
        })

        # Get the target string, link type, and list position from the
        # data set
        data_cols = ['string_id', 'link_type_id', 'item_list_position']
        links = pd.concat(
            [links, data.loc[has_link, data_cols].reset_index(drop=True)],
            axis='columns'
        )
        links = links.rename(columns={'string_id': 'tgt_string_id'})

        # Every entry string in a shorthand text is also the target of a
        # link of type 'entry' whose source is the full text of the
        # input file. This allows direct selection of the original entry
        # strings. The text of the input file is not in the current
        # data set, so set the source strings to null.
        entry_tgt_ids = data.loc[data['item_label_id'].isna(), 'string_id']
        entry_links = pd.DataFrame({
            'src_string_id': pd.NA,
            'tgt_string_id': entry_tgt_ids.drop_duplicates().array,
            'ref_string_id': pd.NA,
            'link_type_id': link_types.loc[link_types == 'entry'].index[0],
            'item_list_position': pd.NA
        })
        links = pd.concat([links, entry_links]).reset_index(drop=True)

        # The item_list_position label is probably only intelligible
        # when handling items within entries, so rename it
        links = links.rename(columns={'item_list_position': 'list_position'})

        # Convert any item list positions into strings and make them
        # into link tags
        link_tags = links['list_position'].dropna().astype(str)

        # Cache the node type ID for tag strings
        tag_node_type_id = node_types.loc[node_types == 'tag'].index[0]

        # Add the tag strings to the rest of the strings
        new_strings = link_tags.drop_duplicates()
        new_strings = bg.util.get_new_typed_values(
            new_strings,
            strings,
            'string',
            'node_type_id',
            tag_node_type_id
        )
        new_strings = pd.DataFrame(
            {'string': new_strings.array, 'node_type_id': tag_node_type_id}
        )
        new_strings = bg.util.normalize_types(new_strings, strings)

        strings = pd.concat([strings, new_strings])

        # convert the tag strings to string ID values
        tag_strings = strings.loc[
            strings['node_type_id'] == tag_node_type_id
        ]
        link_tags = link_tags.map(
            pd.Series(tag_strings.index, index=tag_strings['string'])
        )

        # Relations between links and string-valued tags stored in the
        # strings frame aren't representable as links in the links
        # frame, so make a many-to-many frame relating link ID values to
        # string IDs
        link_tags = pd.DataFrame({
            'link_id': link_tags.index,
            'tag_string_id': link_tags.array
        })

        # Mutate item label map into string-valued series with integer
        # index
        item_label_id_map = pd.Series(
            item_label_id_map.index,
            index=item_label_id_map
        )

        '''
        SWITCHING TO LITERAL NODE TYPE
        parsed = bg.ParsedShorthand(
            strings=strings,
            links=links,
            link_tags=link_tags,
            node_types=node_types,
            link_types=link_types,
            entry_prefixes=None,
            item_labels=item_label_id_map,
            item_separator=item_separator,
            default_entry_prefix=None,
            space_char=space_char,
            comment_char=None,
            na_string_values=na_string_values,
            na_node_type=na_node_type,
            syntax_case_sensitive=self.syntax_case_sensitive,
            allow_redundant_items=self.allow_redundant_items
        )'''

        tn = bg.TextNet()

        tn.strings = strings
        tn.reset_strings_dtypes()

        tn.node_types = pd.DataFrame(columns=tn._node_types_dtypes.keys())
        tn.node_types['node_type'] = node_types.array
        tn.node_types['null_type'] = False
        type_is_null = (tn.node_types['node_type'] == na_node_type)
        tn.node_types.loc[type_is_null, 'null_type'] = True
        tn.reset_node_types_dtypes()

        input_string_id = tn.insert_string(
            input_string,
            input_node_type,
            add_node_type=True
        )

        links['inp_string_id'] = input_string_id

        tn.link_types = pd.DataFrame(columns=tn._link_types_dtypes.keys())
        tn.link_types['link_type'] = link_types.array
        tn.link_types['null_type'] = False
        tn.reset_link_types_dtypes()

        tn.assertions = links
        tn.reset_assertions_dtypes()

        tagged_link_type_id = tn.id_lookup('link_types', 'tagged')
        if tagged_link_type_id not in tn.assertions['link_type_id'].array:
            tn.link_types = tn.link_types.query('link_type != "tagged"')

        tn.assertion_tags = link_tags
        tn.assertion_tags = tn.assertion_tags.rename(
            columns={'link_id': 'assertion_id'}
        )
        tn.reset_assertion_tags_dtypes()

        '''
        SWITCHING TO LITERAL NODE TYPE
        # Concat all entries into a single string
        entry_node_type_id = tn.id_lookup('link_types', 'entry')
        items_text = tn.links.query(
            'link_type_id == @entry_node_type_id'
        )
        items_text = tn.strings.loc[items_text['tgt_string_id'], 'string']
        items_text = '\n'.join(items_text)

        # Insert a strings row for the items text
        items_text_node_type_id = parsed.id_lookup('node_types', 'items_text')
        new_string = {
            'string': items_text,
            'node_type_id': items_text_node_type_id
        }
        new_string = bg.util.normalize_types(new_string, parsed.strings)
        parsed.strings = pd.concat([parsed.strings, new_string])

        items_text_string_id = parsed.strings.index[-1]

        # Insert a strings row for the current function
        func_node_type_id = parsed.id_lookup('node_types', 'python_function')
        new_string = {
            'string': 'Shorthand.parse_items',
            'node_type_id': func_node_type_id
        }
        new_string = bg.util.normalize_types(new_string, parsed.strings)
        parsed.strings = pd.concat([parsed.strings, new_string])

        parse_function_string_id = parsed.strings.index[-1]

        # Insert a strings row for the entry syntax
        entry_syntax_node_type_id = parsed.id_lookup(
            'node_types',
            'shorthand_entry_syntax'
        )
        new_string = {
            'string': self.entry_syntax,
            'node_type_id': entry_syntax_node_type_id
        }
        new_string = bg.util.normalize_types(new_string, parsed.strings)
        parsed.strings = pd.concat([parsed.strings, new_string])

        # Insert a link between the items text and the entry syntax
        new_link = {
            'src_string_id': items_text_string_id,
            'tgt_string_id': new_string.index[0],
            'ref_string_id': parse_function_string_id,
            'link_type_id': parsed.id_lookup('link_types', 'requires')
        }
        new_link = bg.util.normalize_types(new_link, parsed.links)
        parsed.links = pd.concat([parsed.links, new_link])

        parsed.insert_string(
            self.entry_syntax,
            'shorthand_entry_syntax',
            add_node_type=True
        )

        # Links whose reference string ID is missing should have the
        # items text as their reference string
        ref_isna = parsed.links['ref_string_id'].isna()
        parsed.links.loc[ref_isna, 'ref_string_id'] = items_text_string_id

        # Links whose source string ID is missing should have the items
        # text as their source string
        src_isna = parsed.links['src_string_id'].isna()
        parsed.links.loc[src_isna, 'src_string_id'] = items_text_string_id

        return parsed'''

        # Concat all entries into a single string and insert it
        entry_node_type_id = tn.id_lookup('link_types', 'entry')
        items_csv = tn.assertions.query(
            'link_type_id == @entry_node_type_id'
        )
        items_csv = tn.strings.loc[items_csv['tgt_string_id'], 'string']
        items_csv = '\n'.join(items_csv)
        items_csv_string_id = tn.insert_string(
            items_csv,
            '_literal_csv',
            add_node_type=True
        )

        # create a link between the input string and the items csv
        items_csv_link_type_id = tn.insert_link_type('items_csv')
        tn.insert_assertion(
            input_string_id,
            input_string_id,
            items_csv_string_id,
            input_string_id,
            items_csv_link_type_id
        )

        # Create a string for the entry syntax and a link from the
        # input string
        entry_syntax_string_id = tn.insert_string(
            self.entry_syntax,
            '_literal_csv'
        )
        shorthand_entry_syntax_link_id = tn.insert_link_type(
            'shorthand_entry_syntax'
        )
        tn.insert_assertion(
            input_string_id,
            input_string_id,
            entry_syntax_string_id,
            input_string_id,
            shorthand_entry_syntax_link_id
        )

        # Assertions whose reference string ID is missing should have the
        # items text as their reference string
        ref_isna = tn.assertions['ref_string_id'].isna()
        tn.assertions.loc[ref_isna, 'ref_string_id'] = items_csv_string_id

        # Assertions whose source string ID is missing should have the items
        # text as their source string
        src_isna = tn.assertions['src_string_id'].isna()
        tn.assertions.loc[src_isna, 'src_string_id'] = items_csv_string_id

        return tn
