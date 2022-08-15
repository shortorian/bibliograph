import bibliograph as bg
import pandas as pd
from io import StringIO
from pathlib import Path


def _extend_id_map(
    domain,
    existing_domain,
    existing_range=None,
    drop_na=True,
    **kwargs
):
    '''
    Map distinct values in a domain which are not in an existing domain
    to integers that do not overlap with an existing range. Additional
    keyword arguments are passed to the pandas.Series constructor when
    the map series is created.

    Parameters
    ----------
    domain : list-like (coercible to pandas.Series)
        Arbitrary set of values to map. May contain duplicates.

    existing_domain : list-like
        Set of values already present in a map from values to integers.

    existing_range : list-like or None, default None
        Range of integers already present in the range of a map. If
        None, assume existing_domain.index contains the existing range.

    drop_na : bool, default True
        Ignore null values and map only non-null values to integers.

    Examples
    --------
    >>> existing_domain = pd.Series(['a', 'a', 'b', 'f', 'b'])
    >>> new_domain = ['a', 'b', 'z', pd.NA]
    >>> _extend_id_map(new_domain,
                       existing_domain,
                       dtype=pd.UInt16Dtype())

    z    5
    dtype: UInt16

    >>> _extend_id_map(new_domain,
                       existing_domain,
                       dtype=pd.UInt16Dtype(),
                       drop_na=False)

    z       5
    <NA>    6
    dtype: UInt16
    '''
    # check if domain object has autocorr and loc attributes like a
    # pandas.Series and convert if not
    try:
        assert domain.autocorr
        assert domain.loc
        # make a copy so we can mutate one (potentially large) object
        # instead of creating additional references
        domain = domain.copy()
    except AttributeError:
        domain = pd.Series(domain)

    if drop_na:
        domain = domain.loc[~domain.isna()]

    domain_is_new = ~domain.isin(existing_domain)

    if domain_is_new.any():
        domain = domain.loc[domain_is_new].drop_duplicates()

        if existing_range is None:
            new_ids = bg.util.non_intersecting_sequence(
                len(domain),
                existing_domain.index
            )
        else:
            new_ids = bg.util.non_intersecting_sequence(
                len(domain),
                existing_range
            )

    else:
        domain = []
        new_ids = []

    return pd.Series(new_ids, index=domain, **kwargs)


class EntrySyntaxError(ValueError):
    pass


def _validate_entry_syntax_prefix_group(group, allow_redundant_items):

    bg.util.get_single_value(
        group,
        'entry_node_type',
        none_ok=True,
        group_key=group.name
    )

    no_entry_node_types = group['entry_node_type'].isna().all()
    any_entry_node_types = group['entry_node_type'].notna().any()
    no_link_types = group['item_link_type'].isna().all()
    any_link_types = group['item_link_type'].notna().any()

    msg = (
        'Error parsing syntax for entry_prefix {}. There are no values '
        'in column "{}" but column "{}" contains values.'
    )

    if no_entry_node_types and any_link_types:
        raise EntrySyntaxError(
            msg.format(group.name, 'entry_node_type', 'item_link_type')
        )
    if no_link_types and any_entry_node_types:
        raise EntrySyntaxError(
            msg.format(group.name, 'item_link_type', 'entry_node_type')
        )

    item_node_type_na = group['item_node_type'].isna()
    if item_node_type_na.any():
        if group.loc[item_node_type_na, 'item_link_type'].notna().any():
            raise EntrySyntaxError(
                'Error parsing syntax for entry_prefix {}. If any row '
                'has no value in column "item_node_type" then column '
                '"item_link_type" cannot contain a value in that '
                'row.'.format(group.name)
            )

    msg = (
        'Error parsing syntax for entry_prefix {}. If any row has a '
        'value in column "{}" then column "{}" must also contain a '
        'value in that row.'
    )

    list_delimiter_not_na = group['list_delimiter'].notna()
    if list_delimiter_not_na.any():
        if group.loc[list_delimiter_not_na, 'item_node_type'].isna().any():
            raise EntrySyntaxError(
                msg.format(group.name, 'list_delimiter', 'item_node_type')
            )

    item_prefix_separator_not_na = group['item_prefix_separator'].notna()
    if item_prefix_separator_not_na.any():
        has_item_prefix = group.loc[item_prefix_separator_not_na]
        if has_item_prefix.loc[:, 'item_prefixes'].isna().any():
            raise EntrySyntaxError(
                msg.format(
                    group.name, 'item_prefix_delimiter', 'item_prefixes'
                )
            )

        msg = (
            'Error parsing syntax for entry_prefix {}. If any row has '
            'a value in column "{}" then column "{}" cannot contain a '
            'value in that row.'
        )

        if has_item_prefix.loc[:, 'item_node_type'].notna().any():
            raise EntrySyntaxError(
                msg.format(
                    group.name, 'item_prefix_delimiter', 'item_node_type'
                )
            )
        if has_item_prefix.loc[:, 'item_link_type'].notna().any():
            raise EntrySyntaxError(
                msg.format(
                    group.name, 'item_prefix_delimiter', 'item_link_type'
                )
            )

        if group['item_label'].duplicated().any():
            raise EntrySyntaxError(
                'Error parsing labels for entry_prefix {}. Rows with '
                'the same entry prefix must have different item labels.'
                .format(group.name)
            )

    unprefixed_link_types = group.loc[
        ~item_prefix_separator_not_na,
        'item_link_type'
    ]
    if any_link_types and unprefixed_link_types.isna().any():
        raise EntrySyntaxError(
            'Error parsing labels for entry_prefix {}. If any rows '
            'have an item link type then all rows with no item prefix '
            'separator must also have an item link '
            'type.'.format(group.name)
        )

    item_data = ['item_node_type', 'item_link_type']
    duplicate_items = group[item_data].dropna(how='all').duplicated().any()
    if duplicate_items and not allow_redundant_items:
        raise EntrySyntaxError(
            'Error parsing syntax for entry prefix {}. Each row '
            'within a prefix group must have a unique pair of '
            'values for columns item_node_type and item_link_type '
            '(unless both values are null).'.format(group.name)
        )


def validate_entry_syntax(
    entry_syntax,
    case_sensitive,
    allow_redundant_items=False
):

    try:
        assert entry_syntax.casefold
        # if there's a casefold method then the entry syntax is a string

        if Path(entry_syntax).exists():
            # assume it's a csv file and read it
            entry_syntax = pd.read_csv(entry_syntax)
        else:
            # assume it's the string-valued contents of a csv
            # file and convert it to a pandas DataFrame
            with StringIO(entry_syntax) as stream:
                entry_syntax = pd.read_csv(stream)

    except AttributeError:
        raise ValueError(
            'entry_syntax must be a string representing a file path or the '
            'contents of a csv file. Got {}.'.format(type(entry_syntax))
        )

    # pandas converts numbers to numeric types with read_csv, but
    # we're treating everything as strings, so convert values
    entry_syntax = entry_syntax.applymap(str, na_action='ignore')

    # normalize column labels with unicode casefold (like making text
    # uniformly lower or upper case, but applies for the whole unicode
    # standard)
    entry_syntax.columns = entry_syntax.columns.str.casefold()

    if not case_sensitive:
        # Normalize all values with unicode casefold
        entry_syntax = entry_syntax.apply(
            lambda x: x.str.casefold() if x.notna().any() else x
        )

    if entry_syntax['item_label'].isna().any():
        raise ValueError(
            'Error parsing entry syntax. All rows must have a value in '
            'column "item_label"'
        )

    optional_cols = [
        'list_delimiter', 'item_prefix_separator', 'item_prefixes'
    ]
    add_cols = [
        col for col in optional_cols if col not in entry_syntax.columns
    ]
    entry_syntax = pd.concat(
        [
            entry_syntax,
            pd.DataFrame(columns=add_cols, index=entry_syntax.index)
        ],
        axis='columns')

    has_prefix = ('entry_prefix' in entry_syntax.columns)
    if has_prefix and entry_syntax['entry_prefix'].notna().any():
        entry_syntax.groupby('entry_prefix').apply(
            _validate_entry_syntax_prefix_group,
            allow_redundant_items
        )
    else:
        dummy = entry_syntax.copy()
        dummy.name = 'None'
        _validate_entry_syntax_prefix_group(dummy, allow_redundant_items)

    return entry_syntax


class LinkSyntaxError(ValueError):
    pass


def _validate_link_syntax(link_syntax, entry_syntax, case_sensitive):

    try:
        # assume link_syntax is a string if it has a casefold method
        assert link_syntax.casefold

        if Path(link_syntax).exists():
            # assume it's a csv file and read it
            link_syntax = pd.read_csv(link_syntax)
        else:
            # assume it's the string-valued contents of a csv
            # file and convert it to a pandas DataFrame
            with StringIO(link_syntax) as stream:
                link_syntax = pd.read_csv(stream)

    except AttributeError:
        raise ValueError(
            'link_syntax must be a string representing a file path or the '
            'contents of a csv file. Got {}.'.format(type(link_syntax))
        )

    # pandas converts numbers to numeric types with read_csv, but
    # we're treating everything as strings, so convert all values to str
    link_syntax = link_syntax.applymap(str, na_action='ignore')

    # normalize column labels with unicode casefold (like making text
    # uniformly lower or upper case, but applies for the whole unicode
    # standard)
    link_syntax.columns = link_syntax.columns.str.casefold()

    position_cols = [c for c in link_syntax.columns if 'position' in c]

    if case_sensitive:
        # normalize position codes and list modes with unicode casefold
        link_syntax[position_cols] = link_syntax[position_cols].apply(
            lambda x: x.str.casefold()
        )
        # normalize list modes with unicode casefold
        list_modes = link_syntax['list_mode'].str.casefold()
        link_syntax['list_mode'] = list_modes

    else:
        # normalize all values with unicode casefold
        link_syntax = link_syntax.apply(lambda x: x.str.casefold())

    required_columns = [
        'left_entry_prefix',
        'right_entry_prefix',
        'source_position',
        'target_position',
        'link_type',
        'list_mode',
        'dflt_ref_position'
    ]

    missing_columns = [
        c for c in required_columns if c not in link_syntax.columns
    ]

    if missing_columns != []:
        raise LinkSyntaxError(
            'Link syntax missing required columns {}'
            .format(missing_columns)
        )

    position_codes = link_syntax[position_cols].stack().dropna()
    # Casefolded position codes that do not begin with 'l' or 'r' are
    # invalid. Extract any invalid codes with a regular expression.
    invalid_pos_codes = position_codes.str.extract('^(?![lr])(.+)')
    invalid_pos_codes = invalid_pos_codes.dropna()
    if not invalid_pos_codes.empty:
        raise LinkSyntaxError(
            'Position codes in the link syntax must start with "R" '
            'or "L" (ignoring case). Found the following invalid '
            'position codes in the link syntax:\n>>> {}'
            .format(invalid_pos_codes)
        )

    # If there are any item labels in the link syntax that do not
    # exist in the entry syntax, throw an error.
    valid_item_labels = entry_syntax['item_label']
    item_labels = position_codes.str.slice(1)
    item_labels = item_labels.mask(item_labels == '')
    invalid_pos_codes = position_codes.loc[
        ~item_labels.isin(valid_item_labels) & ~item_labels.isna()
    ]
    if not invalid_pos_codes.empty:
        raise LinkSyntaxError(
            'Position codes in the link syntax must start with "R" '
            'or "L" (ignoring case) and continue with an item label '
            'defined in the entry syntax. Found the following position '
            'codes in the link syntax whose item labels do not '
            'exist in the entry syntax:\n>>> {}'.format(invalid_pos_codes)
        )

    # if list modes aren't one of [1:1, m:1, 1:m, m:m] throw an error
    valid_list_modes = ['1:1', 'm:1', '1:m', 'm:m']
    list_mode_notna = ~link_syntax['list_mode'].isna()
    invalid_list_modes = link_syntax['list_mode'].loc[
        list_mode_notna & ~link_syntax['list_mode'].isin(valid_list_modes)
    ]
    if not invalid_list_modes.empty:
        raise LinkSyntaxError(
            'List modes in the link syntax must be one of\n'
            '>>> {}\nFound the following invalid list modes in the '
            'link syntax:\n>>> {}'
            .format(valid_list_modes, list(invalid_list_modes))
        )

    return link_syntax


def parse_link_syntax(
    filepath_or_text,
    entry_syntax,
    entry_prefix_id_map=None,
    link_types=None,
    item_label_id_map=None,
    case_sensitive=True
):

    link_syntax = _validate_link_syntax(
        filepath_or_text,
        validate_entry_syntax(entry_syntax, case_sensitive),
        case_sensitive
    )

    prefix_cols = ['left_entry_prefix', 'right_entry_prefix']

    if entry_prefix_id_map is not None:

        # Select links with entry prefix pairs that exist in this data
        # set
        prefix_is_present = link_syntax[prefix_cols].isin(
            entry_prefix_id_map.index
        )

        prefix_is_present = prefix_is_present.all(axis=1)
        link_syntax = link_syntax.loc[prefix_is_present]

        # map entry prefixes from strings to integers and rename columns
        link_syntax[prefix_cols] = link_syntax[prefix_cols].apply(
            lambda x: x.map(entry_prefix_id_map)
        )
        link_syntax = link_syntax.rename(
            columns={
                'left_entry_prefix': 'left_entry_prefix_id',
                'right_entry_prefix': 'right_entry_prefix_id'
            }
        )

    if link_types is None:

        link_types = link_syntax['link_type']

    else:

        # Get any new link types in the link syntax
        new_link_types = _extend_id_map(
            link_syntax['link_type'],
            link_types,
            dtype=link_types.index.dtype
        )
        new_link_types = pd.Series(new_link_types.index, index=new_link_types)

        # Add new link types
        link_types = pd.concat([link_types, new_link_types])

        # Map the link types from strings to integers
        link_syntax['link_type_id'] = link_syntax['link_type'].map(
            pd.Series(link_types.index, index=link_types.array)
        )
        link_syntax = link_syntax.drop('link_type', axis=1)

    # Links in the synthesized shorthand data are a relation between
    # four entities represented by integer IDs:
    #
    # src_string_id (string representing the source end of the link)
    # tgt_string_id (string representing the target end of the link)
    # ref_string_id (string representing the context of the link)
    # link_type_id (the link type)
    #
    # To generate these we first create a map between position codes in
    # the link syntax and prefixes for components of the link
    columns_and_component_prefixes = {
        'source_position': 'src_',
        'target_position': 'tgt_',
        'dflt_ref_position': 'ref_'
    }
    for column, component_prefix in columns_and_component_prefixes.items():
        position_code = link_syntax[column]

        # The first character in a position code is 'L' or 'R',
        # indicating the link component is either the left or the
        # right entry in the csv file. Slice these off the position
        # codes and create a column for the entry position for this
        # component of the link (source, target, or reference).
        entry_position = position_code.str.slice(0, 1).array
        link_syntax[component_prefix + 'csv_col'] = entry_position

        # The rest of the characters in a position code indicate which
        # item in the entry represents the link component (source,
        # target, or reference). These are either alphanumeric item
        # prefixes as defined in the entry syntax or zero-based
        # positional indices locating the item within the entry. Extract
        # the item labels with a regular expression which is null for
        # single-character position codes.
        item_label = position_code.str.extract('^.(.+)')
        # Create a column for the item label for this component of the
        # link component and drop the column for the position code.
        link_syntax[component_prefix + 'item_label'] = item_label
        link_syntax = link_syntax.drop(column, axis=1)

    if item_label_id_map is not None:

        # Map item labels to integers
        item_label_cols = [
            col for col in link_syntax.columns if 'item_label' in col
        ]
        link_syntax[item_label_cols] = link_syntax[item_label_cols].apply(
            lambda x: x.map(item_label_id_map)
        )

    return link_types, link_syntax
