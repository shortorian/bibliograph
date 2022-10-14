import pandas as pd


def get_string_values(
    obj,
    string_subset=slice(None),
    node_type_subset=slice(None),
    casefold=False
):
    if obj.strings.empty:
        return obj.strings['string']

    strings_idx = obj.strings['string'].loc[string_subset].index

    try:
        assert node_type_subset.casefold()

        node_type_id = obj.id_lookup('node_types', node_type_subset)
        node_types_idx = obj.strings.query(
            'node_type_id == @node_type_id'
        )
        node_types_idx = node_types_idx.index

    except AttributeError:

        try:
            node_type_id = int(node_type_subset)
            node_types_idx = obj.strings.query(
                'node_type_id == @node_type_id'
            )
            node_types_idx = node_types_idx.index

        except ValueError:
            node_types_idx = obj.strings['node_type_id'].loc[
                node_type_subset
            ]
            node_types_idx = node_types_idx.index

    selection = strings_idx.intersection(node_types_idx)
    selection = obj.strings.loc[selection, 'string']

    if casefold:
        return selection.str.casefold()

    else:
        return selection


def make_bidirectional_map_one_to_many(df):

    input_columns = list(df.columns)
    df.columns = [0, 1]

    # Drop identities in a way that doesn't copy the whole dataframe.
    identities = df.loc[df[0] == df[1]]
    df = df.drop(identities.index).drop_duplicates()
    # Using the above method because the following line raises a copy
    # vs. view warning:
    # df = df.loc[df[0] != df[1], :]

    # Check if the input dataframe is already a one-to-many map
    if not df[1].duplicated().any() and not df[1].isin(df[0]).any():
        df.columns = input_columns
        return df

    # Stack the values so we can find duplicate values regardless of the
    # column
    values = df.stack()
    duplicate_values = values.loc[values.duplicated(keep=False)]

    '''
    Drop rows of the form [b, a] when the row [a, b] is also present
    '''

    # Get values that are duplicated and present in both columns
    value_selection = duplicate_values.loc[
        duplicate_values.isin(df[0]) & duplicate_values.isin(df[1])
    ]
    drop_candidates = df.loc[df[1].isin(value_selection)]

    # Hash the dataframe to search for whole rows
    hashed_df = pd.util.hash_pandas_object(df, index=False)

    # Reverse the selected rows and hash them to search for the
    # reversed rows in the input dataframe
    reversed_candidates = drop_candidates[[1, 0]]
    hashed_candidates = pd.util.hash_pandas_object(
        reversed_candidates,
        index=False
    )

    # Drop the reversed rows if they're present in the dataframe
    drop_candidates = hashed_df.isin(hashed_candidates)
    if drop_candidates.any():

        # If the search above caught [a, b] then it also caught [b, a]
        # so we select one of those rows by stacking, sorting by index
        # and then by value, pivoting back to the original shape, and
        # then dropping duplicate rows.
        drop_candidates = df.loc[hashed_df[drop_candidates].index]
        drop_candidates = drop_candidates.stack().reset_index()
        drop_candidates = drop_candidates.sort_values(by=['level_0', 0])
        drop_candidates.loc[:, 'level_1'] = [0, 1]*int(len(drop_candidates)/2)
        drop_candidates = drop_candidates.pivot(
            index='level_0',
            columns='level_1',
            values=0
        )

        drop_rows = drop_candidates.drop_duplicates().index

        df = df.drop(drop_rows)

        # If the dataframe is now a one-to-many map, we're done
        if not df[1].duplicated().any():
            if not df[1].isin(df[0]).any():
                df.columns = input_columns
                return df

        # Stack the modified dataframe and get duplicate values
        values = df.stack()
        duplicate_values = values.loc[values.duplicated(keep=False)]

    '''
    Locate two different types of duplicate values in the second column
    '''

    # Get first instance in the second column of values duplicated
    # anywhere in the dataframe
    first_instance_of_any_dplct_in_c1 = (
        duplicate_values.loc[(slice(None), 1)].drop_duplicates()
    )

    # Get values from the second column which are duplicated anywhere
    # in the dataframe but which occurred first in the second column
    frst_instnc_of_dplct_found_in_c1_frst = duplicate_values.drop_duplicates()
    if 1 in frst_instnc_of_dplct_found_in_c1_frst.index.get_level_values(1):
        frst_instnc_of_dplct_found_in_c1_frst = (
            frst_instnc_of_dplct_found_in_c1_frst.loc[(slice(None), 1)]
        )

    else:
        frst_instnc_of_dplct_found_in_c1_frst = pd.Series([], dtype=int)

    # If values duplicated anywhere in the dataframe exist in the
    # second column, map the duplicates to the first-column
    # partner of the first instance of the value in the second column
    if df[1].duplicated().any():

        '''
        Make maps between the different kinds of first second-column
        instances and their first-column partners
        '''

        # Map from the first instance in the second column of values
        # duplicated anywhere in the dataframe to their first-column
        # partners
        df_selection = df.loc[first_instance_of_any_dplct_in_c1.index]
        map_to_c0_from_frst_instnc_of_any_dplct_in_c1 = pd.Series(
            df_selection[0].array,
            index=df_selection[1].array
        )

        # Map from duplicate values that occurred first in the second
        # column to the first-column partners of their first instance
        map_to_c0_from_frst_instnc_of_dplct_found_in_c1_frst = df.loc[
            frst_instnc_of_dplct_found_in_c1_frst.index,
            :
        ]
        map_to_c0_from_frst_instnc_of_dplct_found_in_c1_frst = pd.Series(
            map_to_c0_from_frst_instnc_of_dplct_found_in_c1_frst[0].array,
            index=map_to_c0_from_frst_instnc_of_dplct_found_in_c1_frst[1].array
        )

        '''
        Apply the maps
        '''

        # Get rows with values in the first column that appeared
        # first in the second column
        c0_value_was_found_in_c1_first = df.loc[
            df[0].isin(first_instance_of_any_dplct_in_c1),
            :
        ]

        # Map those values to the first-column partners of their first
        # instances in the second column
        mapped_c0 = c0_value_was_found_in_c1_first[0].map(
            map_to_c0_from_frst_instnc_of_any_dplct_in_c1
        )
        df.loc[c0_value_was_found_in_c1_first.index, 0] = mapped_c0.array

        # Get all duplicate values in the second column which appeared first
        # in the second column
        c1_value_was_found_in_c1_first = df.loc[
            df[1].isin(frst_instnc_of_dplct_found_in_c1_frst),
            :
        ]

        # Drop the first instances to isolate only the duplicates
        c1_value_was_found_in_c1_first = c1_value_was_found_in_c1_first.drop(
            frst_instnc_of_dplct_found_in_c1_frst.index
        )

        # If there are duplicate values in the second column which
        # appeared first in the second column, map those values to the
        # first-column partners of their first instances and then swap
        # values in those rows so the first instance's first-column
        # partner is in the first column

        if not c1_value_was_found_in_c1_first.empty:
            mapped_1 = c1_value_was_found_in_c1_first[1].map(
                map_to_c0_from_frst_instnc_of_dplct_found_in_c1_frst
            )
            df.loc[c1_value_was_found_in_c1_first.index, :] = list(zip(
                mapped_1.array,
                df.loc[c1_value_was_found_in_c1_first.index, 0]
            ))
            df = df.drop_duplicates()

    # Rearrange the dataframe so that values which appear in both
    # columns appear first in the second column

    c1_in_c0 = df[1].isin(df[0])
    df = pd.concat([df.loc[c1_in_c0], df.loc[~c1_in_c0]])

    c0_in_c1 = df[0].isin(df[1])
    df = pd.concat([df.loc[~c0_in_c1], df.loc[c0_in_c1]])

    # In some cases the above algorithm will loop indefinitely if there
    # are values that occur in both columns but there are no repeated
    # values in the second column. Avoid this finding rows with values
    # in the first column that are present in the second column and, if
    # any exist, swapping values in the first row found.
    c0_in_c1 = df[0].isin(df[1])

    if c0_in_c1.any():
        to_swap = df.loc[c0_in_c1].index[0]
        df.loc[to_swap, :] = df.loc[to_swap, [1, 0]].to_numpy()

    df.columns = input_columns

    # Remap the modified dataframe
    return make_bidirectional_map_one_to_many(df)


def escape_regex_metachars(s):
    s = s.replace("\\", "\\\\")
    metachars = '.^$*+?{}[]|()'
    for metachar in [c for c in metachars if c in s]:
        s = s.replace(metachar, f'\\{metachar}')
    return s


def get_single_value(
    df_or_s,
    label,
    none_ok=False,
    group_key=None
):

    try:
        # If this assertion passes then assume input is a pandas.Series
        assert df_or_s.str

        if df_or_s.empty:
            raise ValueError(f'Series with label "{label}" is empty.')
        elif len(df_or_s) == 1:
            return df_or_s.array[0]

        values = df_or_s.value_counts()

        num_values = len(values)

        if num_values == 1:
            return values.index[0]
        elif num_values > 1:
            raise ValueError(f'Found multiple values with label "{label}"')
        elif num_values < 1:
            if not none_ok:
                raise ValueError(f'Found only nan values with label "{label}"')
            else:
                return None

    except AttributeError:

        # If the assertion failed then assume input is a pandas.DataFrame
        if len(df_or_s[label]) == 1:
            return df_or_s[label].array[0]

        if group_key is None:
            message_tail = f'in column "{label}"'

        else:
            message_tail = 'in column "{}", group key "{}"'.format(
                label,
                group_key
            )

        values = df_or_s[label].value_counts()

        num_values = len(values)

        if num_values == 1:
            return values.index[0]
        elif num_values > 1:
            raise ValueError('Found multiple values {}'.format(message_tail))
        elif num_values < 1:
            if not none_ok:
                raise ValueError('No values found {}'.format(message_tail))
            else:
                return None


def iterable_not_string(x):
    '''
    Check if input has an __iter__ method and then determine if it's a
    string by checking for a casefold method.
    '''
    try:
        assert x.__iter__

        try:
            assert x.casefold
            return False

        except AttributeError:
            return True

    except AttributeError:
        return False


def normalize_types(to_norm, template, strict=True, continue_idx=True):
    '''
    Create an object from to_norm that can be concatenated with template
    such that the concatenated object and its index will have the same
    dtypes as the template.

    If template is pandas.DataFrame and to_norm is dict-like or
    list-like, convert each element of to_norm to a pandas.Series with
    dtypes that conform to the dtypes of a template dataframe and and
    concatenate them together as columns in a dataframe.

    If template is pandas.Series, convert to_norm to a Series with the
    appropriate array dtype or, if to_norm is dict-like,
    convert its values to an array with the appropriate dtype and if
    both to_norm and template have numeric indexes, also normalize the
    index dtype. If to_norm is dict-like and either to_norm or template
    does not have a numeric index, use to_norm.keys() as the output
    index.

    Parameters
    ----------
    to_norm : dict-like or list-like
        A set of objects to treat as columns of an output
        pandas.DataFrame and whose types will be coerced.

    template : pandas.DataFrame or pandas.Series
        Output dtypes will conform to template.dtypes and the output
        index dtype will be template.index.dtype

    strict : bool, default True
        If True and to_norm is dict-like, only objects whose keys are
        column labels in the template will be included as columns in the
        output dataframe.

        If True, to_norm is list-like, and template has N columns,
        include only the first N elements of to_norm in the output
        dataframe.

        If False and to_norm is dict-like, normalize dtypes for objects
        whose keys are column labels in the template and include the
        other elements of to_norm as columns with dtypes inferred by
        the pandas.Series constructor.

        If False, to_norm is list-like, and template has N columns,
        normalize dtypes for the first N elements of to_norm and include
        the other elements of to_norm as columns with dtypes inferred by
        the pandas.Series constructor. Labels for the extra columns in
        the output dataframe will be integers counting from N.

    continue_index : bool, default True
        If True and template has a numerical index, the index of the
        returned object will be a sequence of integers which fills
        gaps in and/or extends to_norm.index

    Returns
    -------
    pandas.DataFrame
        Has as many rows as the longest element of to_norm.

        If to_norm is dict-like and strict is True, output includes
        only objects in to_norm whose keys are also column labels in the
        template.

        If to_norm is list-like and strict is True, output has the same
        width as the template.

        If strict is False, output has one column for each element in
        to_norm.
    '''

    # get the size of to_norm

    try:
        # to_norm is treated as columns, so if it has a columns
        # attribute, we want the size of that instead of the length
        num_elements = len(to_norm.columns)
    except AttributeError:
        num_elements = len(to_norm)

    # check if the template index has a numeric dtype
    templt_idx_is_nmrc = pd.api.types.is_numeric_dtype(template.index)

    try:
        tmplt_columns = template.columns
        # If the template is a dataframe, get its width.
        num_tmplt_columns = len(tmplt_columns)

    except AttributeError:
        # Template is not dataframe-like.
        # Treat it as 1D object with attributes dtype and index

        try:
            assert to_norm.items
            # to_norm is dict-like, so process its keys as an index

            norm_keys_are_nmrc = pd.api.types.is_numeric_dtype(
                pd.Series(to_norm.keys())
            )
            if strict and norm_keys_are_nmrc and templt_idx_is_nmrc:
                index = non_intersecting_sequence(
                    to_norm.keys(),
                    template.index
                )
            else:
                index = to_norm.keys()

            try:
                # values is callable for a dict-like but not a
                # pandas.Series
                values = to_norm.values()

            except TypeError:
                values = to_norm.array

        except AttributeError:
            if strict and templt_idx_is_nmrc:
                index = non_intersecting_sequence(num_elements, template.index)
            else:
                index = range(num_elements)

            values = to_norm

        index = pd.Index(index, dtype=template.index.dtype)
        return pd.Series(values, index=index, dtype=template.dtype)

    try:
        # check if to_norm is dict-like
        assert to_norm.items
        # if so, optionally get extra columns
        if (num_elements > num_tmplt_columns) and not strict:
            extra_columns = {
                k: v for k, v in to_norm.items() if k not in tmplt_columns
            }

    except AttributeError:
        # to_norm is not dict-like
        if num_elements < num_tmplt_columns:
            raise ValueError(
                'If to_norm is list-like, to_norm must have at least '
                'as many elements as there are columns in template.'
            )
        # optionally get extra columns
        if (num_elements > num_tmplt_columns) and not strict:
            extra_columns = dict(zip(
                range(num_tmplt_columns, num_elements),
                to_norm[num_tmplt_columns:]
            ))
        # make the list-like dict-like
        to_norm = dict(zip(tmplt_columns, to_norm[:num_tmplt_columns]))

    to_norm = [
        pd.Series(v, dtype=template[k].dtype, name=k)
        for k, v in to_norm.items() if k in tmplt_columns
    ]

    try:
        to_norm += [pd.Series(v, name=k) for k, v in extra_columns.items()]
    except NameError:
        pass

    new_df = pd.concat(to_norm, axis='columns')

    if continue_idx and templt_idx_is_nmrc:
        template_idx_is_sequential = pd.Series(template.index).diff().iloc[1:]
        template_idx_is_sequential = (template_idx_is_sequential == 1).all()

        if template_idx_is_sequential & template.index.is_monotonic:
            new_index_min = template.index.max() + 1
            index = pd.RangeIndex(new_index_min, new_index_min + len(new_df))
        else:
            index = non_intersecting_sequence(new_df.index, template.index)

        new_df.index = pd.Index(index, dtype=template.index.dtype)

    return new_df


def get_new_typed_values(
    candidate_values,
    existing_values,
    value_column_label,
    type_column_label=None,
    candidate_type=None
):
    '''
    Selects values out of a Series of candidate values if the values are
    either not present in an existing dataframe or they are present but
    do not have the same type as the candidates.

    Parameters
    ----------
    candidate_values : pandas.Series
        Values which may or may not be present in an existing dataframe

    existing_values : pandas.DataFrame
        A dataframe with a column of values to select against and a
        column that identifies the type of each values

    value_column_label
        The label of the column of values in the existing_values
        dataframe

    type_column_label
        The label of the column of types in the existing_values
        dataframe

    candidate_type
        The type of the candidate values

    Returns
    -------
    pandas.Series
        Subset of the candidate values which are either not present in
        the set of existing values or which are present but have a
        different type in the set of existing values
    '''

    value_label_is_container = iterable_not_string(value_column_label)
    type_label_is_container = iterable_not_string(type_column_label)

    if 'columns' in dir(candidate_values):

        if not value_label_is_container:
            value_column_label = [value_column_label]

        if not type_label_is_container:
            type_column_label = [type_column_label]

        cols = [
            label
            for label_list in [value_column_label, type_column_label]
            for label in label_list
            if label is not None
        ]

        hashed_candidates = pd.util.hash_pandas_object(
            candidate_values[cols],
            index=False
        )
        existing_values = pd.util.hash_pandas_object(
            existing_values[cols],
            index=False
        )

        return candidate_values.loc[~hashed_candidates.isin(existing_values)]

    elif candidate_type is None:
        raise ValueError(
            'Must provide candidate type for one dimensional candidates'
        )

    elif type_column_label is None:
        raise ValueError(
            'Must provide type column for one dimensional candidates'
        )

    elif value_label_is_container or type_label_is_container:
        raise ValueError(
            'Must provide string-valued value column and type column '
            'labels for one dimensional candidates'
        )

    else:
        candidate_already_exists = existing_values[value_column_label].isin(
            candidate_values
        )
        wrong_type = existing_values[type_column_label] != candidate_type
        candidate_exists_wrong_type = candidate_already_exists & wrong_type

        candidate_is_new_type = candidate_values.isin(
            existing_values.loc[
                candidate_exists_wrong_type,
                value_column_label
            ]
        )

        candidate_is_new_value = ~candidate_values.isin(
            existing_values[value_column_label]
        )

        candidate_is_new_value = candidate_values.loc[
            candidate_is_new_type | candidate_is_new_value
        ]

        return candidate_is_new_value


def missing_integers(input_values, rng=None):
    '''
    Creates a set of integers in a target range that does
    not intersect with values in an input set. Default range
    fills gaps from 0 to the largest input value.

    If target range does not intersect with input, return
    target.

    If target range exactly covers or is a subset of input,
    return empty set.

    input
    -----
    input_values (list-like or set):
        integers to exclude from output

    output
    ------
    (set):
        integers in target range that are not in input set
    '''
    if len(rng) != 2:
        raise ValueError('rng must be list-like with two elements')

    input_values = set(input_values)

    if rng is None:
        rng = [0, int(max(input_values)) + 1]

    target_range = set(range(*rng))

    if target_range.intersection(input_values) == set():
        return target_range
    if target_range.union(input_values) == input_values:
        return set()
    else:
        return set(input_values ^ target_range)


def non_intersecting_sequence(
    to_map,
    existing=[],
    rng=None,
    full=False,
    ignore_duplicates=False
):
    '''
    Maps unique values from a new list-like or set of integers onto the
    smallest sequence of integers that does not intersect with unique
    values in an existing list-like or set of integers.

    inputs
    ------
    to_map (list-like, set, or integer):
        integers to map to integers excluding existing integers. if an
        integer or float with integer value, to_map is converted to a
        range of that length
    existing (list-like or set):
        integers to keep
    rng (integer-valued list-like):
        range of target values for mapping
    full (bool):
        if true, return mapped values appended to existing values
        if false, return only mapped values
    ignore_duplicates (bool):
        if true, map every element in the input set to a unique value in
        the output

    output
    ------
    pandas array:
        existing and mapped integers

    examples
    --------
    a = [2,3,3]
    b = [4,3,4]
    non_intersecting_sequence(a, b)
    # output: Series([0,1,0])

    a = [2,3,3]
    b = [4,3,4]
    non_intersecting_sequence(a, b, rng=[-2,None], full=True)
    # output: Series([4,3,4,-2,-1,-2])

    c = [9,10,9]
    d = [0,2,4]
    non_intersecting_sequence(c, d, ignore_duplicates=True)
    # output: Series([1,3,5])
    '''

    if type(existing) == set:
        existing = list(existing)

    existing = pd.Series(existing, dtype='int')

    if type(to_map) == set:
        to_map = list(to_map)
    elif (type(to_map) == float) and not to_map.is_integer():
        raise ValueError('to_map cannot be a non-integer float')
    elif not iterable_not_string(to_map):
        try:
            int(to_map)
        except TypeError:
            pass
        to_map = range(to_map)

    to_map = pd.Series(to_map, dtype='int')
    unique_ints_to_map = to_map.drop_duplicates()
    len_of_req_sqnce = len(unique_ints_to_map)

    # NOTE
    # I initially had a default value rng=[0, None] in the function
    # signature. In prototyping with Jupyter lab, multiple calls to this
    # function in a row would retain the previous value of rng (the
    # second element would not be None in the second call). I don't know
    # why this happened and I don't know why using rng=None as a default
    # in the signature fixed the problem.

    if rng is None:
        if existing.empty:
            rng = [0, len_of_req_sqnce + 1]
        else:
            rng = [0, int(existing.max()) + len_of_req_sqnce + 1]

    elif len(rng) != 2:
        raise ValueError('rng must be list-like with two elements')

    elif len(range(*rng)) < (len(unique_ints_to_map) + 1):
        raise ValueError(
            'range(*rng) must include at least as many values as '
            'number of values to map'
        )

    available_ints = missing_integers(existing.drop_duplicates(), rng)
    available_ints = pd.Series(list(available_ints), dtype='int')
    available_ints = available_ints.sort_values().reset_index(drop=True)

    if ignore_duplicates:
        new_ints = available_ints[:len(to_map)]
    else:
        new_ints = available_ints.iloc[:len_of_req_sqnce]
        int_map = dict(zip(unique_ints_to_map, new_ints))
        new_ints = to_map.apply(lambda x: int_map[x])

    if full:
        full_sequence = pd.Series(
            existing.append(new_ints, ignore_index=True),
            dtype='int'
        )
        return full_sequence.array
    else:
        return pd.Series(new_ints, dtype='int').array


def set_string_dtype(df):
    '''
    can't use pd.StringDtype() throughout because it currently doesn't
    allow construction with null types other than pd.NA. This will
    likely change soon
    https://github.com/pandas-dev/pandas/pull/41412
    '''
    df_cols = df.columns
    return df.astype(
        dict(
            zip(
                df_cols, [pd.StringDtype()]*len(df_cols)
            )
        )
    )


def map_indexes(candidate_values, new_values, existing_values):

    new_value_id_map = non_intersecting_sequence(
        new_values.index,
        existing_values.index
    )
    new_value_id_map = pd.Series(
        new_value_id_map,
        index=new_values.index.array
    )

    common_values = candidate_values.loc[
        ~candidate_values.index.isin(new_values.index)
    ]
    hashed_common_values = pd.util.hash_pandas_object(
        common_values,
        index=False
    )
    hashed_existing_values = pd.util.hash_pandas_object(
        existing_values,
        index=False
    )

    common_value_id_map = hashed_common_values.map(pd.Series(
        hashed_existing_values.index.array,
        index=hashed_existing_values.array
    ))

    full_obj_id_map = pd.concat([common_value_id_map, new_value_id_map])

    return common_value_id_map, new_value_id_map, full_obj_id_map.sort_index()


def map_string_ids(obj, existing_obj):

    if 'nodes' not in dir(obj) or obj.nodes is None:
        obj_typed_values = obj.strings[['string', 'node_type_id']]
    else:
        obj_types = obj.strings['node_id'].map(obj.nodes['node_type_id'])
        obj_typed_values = pd.concat(
            [obj.strings['string'], obj_types.rename('node_type_id')],
            axis='columns'
        )

    if 'nodes' not in dir(obj) or obj.nodes is None:
        existing_obj_typed_values = existing_obj.strings[
            ['string', 'node_type_id']
        ]
    else:
        existing_obj_types = existing_obj.strings['node_id'].map(
            existing_obj.nodes['node_type_id']
        )
        existing_obj_typed_values = pd.concat(
            [
                existing_obj.strings['string'],
                existing_obj_types.rename('node_type_id')
            ],
            axis='columns'
        )

    new_typed_values = get_new_typed_values(
        obj_typed_values,
        existing_obj_typed_values,
        'string',
        'node_type_id'
    )

    return map_indexes(
        obj_typed_values,
        new_typed_values,
        existing_obj_typed_values
    )


def map_assertion_ids(obj, existing_obj):

    columns = [
        label for label in obj.assertions.columns if label.endswith('_id')
    ]

    new_values = get_new_typed_values(
        obj[columns],
        existing_obj[columns],
        columns
    )

    return map_indexes(
        obj[columns],
        existing_obj[columns],
        new_values
    )
