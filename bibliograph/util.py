from collections.abc import Iterable
from datetime import datetime

import bibliograph as bg
import pandas as pd


def coerce_types(coerce,
                 template=None,
                 series_dtype=None,
                 insert_columns=True):
    '''
    coerce one pandas Series or DataFrame into the same data types as
    another. This function was initially required because pandas cannot
    convert string values directly to pd.Int types, but pd.Int types are
    the only integer types that have a null value in pandas. The
    solution is to first convert to numpy.int types by using lower case
    names for integer dtypes in DataFrame.astype() and then convert to
    any other type.
    See https://stackoverflow.com/q/67646300/875343
    '''

    coerce = coerce.copy()

    if isinstance(coerce, pd.Series):
        if template is None:
            if series_dtype is None:
                raise ValueError('Must provide template object or list-like '
                                 'of dtypes')
            dtype = series_dtype

        elif not isinstance(template, pd.Series):
            raise TypeError('if object to coerce has type pd.Series '
                            'then template must have type pd.Series')
        else:
            dtype = template.dtypes

        try:
            coerce = coerce.astype(str(dtype))
        except TypeError:
            whereNaN = coerce.isna()
            coerce = coerce.fillna(0)

            coerce = coerce.astype(str(dtype).lower())
            coerce = coerce.astype(str(dtype))

            coerce = coerce.mask(whereNaN, pd.NA)
    else:

        if not all([col in template.columns for col in coerce.columns]):
            missing_cols = [col for col in coerce.columns
                            if col not in template.columns]
            raise ValueError('Every column in dataframe to coerce must '
                             'exist in template dataframe. Column '
                             'label(s) {} do not exist in the template.'
                             .format(missing_cols))

        common_cols = [col for col in coerce.columns
                       if col in template.columns]

        if insert_columns and not all([col in coerce.columns
                                       for col in template.columns]):

            for i in range(len(template.columns)):
                label = template.columns[i]
                if label not in common_cols:
                    coerce.insert(i, label, pd.NA)

            c = template.columns

        else:
            c = common_cols

        coerce = coerce[c]

        # non-empty strings are boolean True (the string "False" is
        # truthy) so check for columns of type 'object' that need to be
        # coerced to boolean and then map "true" and "false" to their
        # boolean values (case insensitive).
        types_to_coerce = pd.Series(coerce.dtypes, index=c)
        template_types = pd.Series(template[c].dtypes, index=c)

        where_template_bool = (template_types == bool)
        if where_template_bool.any():

            bool_labels = template_types.loc[where_template_bool].index.array
            where_coerce_also_obj = (types_to_coerce[bool_labels] == 'object')

            if where_coerce_also_obj.any():

                labels = where_coerce_also_obj.index.array

                bool_map = {'true': True, 'false': False}

                def mapper(x):
                    x.str.lower().map(bool_map)

                coerce.loc[:, labels] = coerce[labels].apply(mapper)

        try:
            coerce = coerce.astype(dict(zip(coerce.columns,
                                            template[c].dtypes.array)))
        except TypeError:
            whereNaN = coerce.isna()
            coerce = coerce.fillna(0)

            lower_case_types = template[c].dtypes.astype(str).str.lower().array
            coerce = coerce.astype(dict(zip(coerce.columns, lower_case_types)))
            coerce = coerce.astype(dict(zip(coerce.columns,
                                            template[c].dtypes.array)))

            coerce = coerce.mask(whereNaN, pd.NA)

    return coerce


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
    # I initially had a default value rng=[0, None] in the function signature.
    # In prototyping with Jupyter lab, multiple calls to this function in a row
    # would retain the previous value of rng (the second element would not be 
    # None in the second call). I don't know why this happened and I don't know
    # why using rng=None as a default in the signature fixed the problem.

    if rng is None:
        if existing.empty:
            rng = [0, len_of_req_sqnce + 1]
        else:
            rng = [0, int(existing.max()) + len_of_req_sqnce + 1]
    elif len(rng) != 2:
        raise ValueError('rng must be list-like with two elements')
    elif len(range(*rng)) < (len(unique_ints_to_map) + 1):
        raise ValueError('range(*rng) must include at least as many '
                         'values as number of values to map')

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


def read_label_map(filename):
    label_map = pd.read_csv(filename, header=0, skipinitialspace=True)
    
    default_columns = bg._default_label_map.columns

    all_columns_in_default = all([c in default_columns 
                                  for c in label_map.columns])
    all_defaults_in_columns = all([c in label_map.columns
                                   for c in default_columns])

    if (not all_columns_in_default) or (not all_defaults_in_columns):
        raise ValueError('label map must have columns {}'
                         .format(default_columns))
    
    return label_map


def extract_surname_and_initials(name):
    '''
    take a name formatted like "Surname, First N.", extract the surname
    and join it with any capital letters after the comma. If names are
    all-caps, take the surname and the first letter of each part of the
    rest of the name. Treats hyphens as two parts, so
    "SURNAME, ALPHA-BETA" maps to surnameab.
    '''
    name = name.split(', ')
    
    if ' ' in name[0]:
        name[0] = ''.join(name[0].split(' '))
    
    if len(name) == 1:
        return name[0]

    if ''.join(c for c in name[1] if c.isalpha()).isupper():
        name[1] = [s[0] for substring in name[1].strip().split(' ') 
                        for s in substring.strip('-').split('-')]
        return (name[0] + ''.join(name[1])).lower()

    else:
        return (name[0] + ''.join(c for c in name[1] if c.isupper())).lower()


def time_string():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def extract_comments(df,
                     extract_from_schema=None,
                     template_tn=None,
                     coerce=True,
                     insert_columns=False):

    if extract_from_schema.lower() == 'comments':
        raise ValueError('extract_from_schema="comments" is not valid input. '
                         'Cannot extract comments from comments table.')

    df = df.copy()
    df['comment_id'] = df.index.array

    no_comment = (df['comment'] == '')
    df.loc[no_comment, 'comment_id'] = pd.NA
    comment_index = df['comment_id'].dropna().reset_index().index
    df.loc[~no_comment, 'comment_id'] = comment_index.array

    if extract_from_schema is None:
        df_cols = [c for c in df.columns if c != 'comment']
    
    else:
        if template_tn is None:
            schema = bg._default_schema
            error_name = 'bg._default_schema'
        else:
            schema = template_tn.schema
            error_name = 'template_tn.schema'

        if extract_from_schema not in schema.keys():
            raise ValueError('Table name {} not in {}'.format(extract_from_schema,
                                                              error_name))
        
        df_cols = [c for c in df.columns if c in schema[extract_from_schema].columns]
    
    comments = df.loc[~no_comment, ['comment_id', 'comment']]
    df = df.loc[:, df_cols]

    if coerce:
        df = coerce_types(df, schema[extract_from_schema], insert_columns=insert_columns)
        comments = coerce_types(comments, schema['comments'], insert_columns=insert_columns)
    
    return df, comments


def assign_integer_to_unique_values(input_series,
                                    value_label='value',
                                    new_integer_label='new_integer'):

    values = input_series.reset_index(name=value_label)
    values.rename(columns={'index':'input_index'}, inplace=True)
    uniques = values[value_label].drop_duplicates()
    uniques = uniques.reset_index()
    uniques.rename(columns={'index':new_integer_label}, inplace=True)
    values = values.merge(uniques)
    return values.set_index('input_index').sort_index()