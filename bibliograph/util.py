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


def make_bidirectional_map_many_to_one(df):

    input_columns = list(df.columns)
    df.columns = [0, 1]

    # Drop identities in a way that doesn't copy the whole dataframe.
    identities = df.loc[df[0] == df[1]]
    df = df.drop(identities.index).drop_duplicates()
    # Using the above method because the following line raises a copy
    # vs. view warning:
    # df = df.loc[df[0] != df[1], :]

    # Check if the input dataframe is already a many-to-one map
    if not df[1].duplicated().any() and not df[1].isin(df[0]).any():

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

        # If the dataframe is now a many-to-one map, we're done
        if not df[1].duplicated().any():
            if not df[1].isin(df[0]).any():
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
    return make_bidirectional_map_many_to_one(df)
