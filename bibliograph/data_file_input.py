from bibtexparser.bparser import BibTexParser as _BibTexParser
from bibtexparser.bwriter import BibTexWriter as _BibTexWriter
from datetime import datetime
from pathlib import Path
from re import split as _regexsplit
from shutil import copyfile as _copyfile
from sys import getsizeof as _getsize
import bibliograph as bg
import pandas as pd

###############################################################################
#  define functions to clean input strings
###############################################################################


def _parse_column(item_parser, strings_to_parse):
    '''
    generic parsing function for a dataframe of strings
    '''
    whereNA = strings_to_parse.isna()
    if whereNA.any():
        strings_to_parse.loc[~whereNA] = strings_to_parse.loc[~whereNA] \
                                                         .apply(item_parser)
        return strings_to_parse
    else:
        return strings_to_parse.apply(item_parser)


def _parse_grouped_strings(
    input_group,
    string_parsers,
    explode_overwrites=None
):

    try:
        parser, explode = string_parsers[input_group.name]
    except KeyError:
        return input_group

    output = input_group.copy()
    output.loc[:, 'string'] = _parse_column(parser, output['string']).array

    if explode is not False:
        output = output.explode('string', ignore_index=True)

        if bg.iterable_not_string(explode):
            if explode_overwrites is None:
                raise ValueError(
                    'Parser function for input label "{}" passed with '
                    'iterable {}, but parameter explode_overwrites is '
                    'None. Must provide column label to overwrite '
                    'when passing an iterable with a parser function.'
                    .format(input_group.name, explode)
                )
            output.loc[:, explode_overwrites] = list(explode)*len(input_group)

        elif explode is not True:
            raise ValueError(
                'Could not interpret second value passed with the '
                '"{}" parser. Must be True, False, or non-string '
                'iterable.'.format(input_group.name)
            )

    return output

#  functions to parse bibtex data


def _bibtex_agent_parser(x):
    return x.split(' and ')


def _bibtex_title_parser(x):
    return x.translate(str.maketrans('', '', '{}'))


def _bibtex_pages_parser(x):
    x = x.strip()
    if '--' not in x:
        return [x, pd.NA]
    else:
        return list(map(str.strip, x.split('--')))


_bibtex_string_parsers = {
    'author': (_bibtex_agent_parser, True),
    'editor': (_bibtex_agent_parser, True),
    'collaborator': (_bibtex_agent_parser, True),
    'title': (_bibtex_title_parser, False),
    'journal': (_bibtex_title_parser, False),
    'booktitle': (_bibtex_title_parser, False),
    'school': (_bibtex_title_parser, False),
    'pages': (_bibtex_pages_parser, ['page', 'endpage'])
}

#  functions to parse manually transcribed data


def _check_for_prefix(s):
    parts = s.split('_', maxsplit=1)
    if (len(parts) > 1) and (len(parts[0]) == 1):
        return True
    else:
        return False


def _insert_default_prefix(column, default_codes, has_prefix):
    prefix = default_codes[column.name] + '_'
    no_prefix = ~has_prefix[column.name]
    return column.mask(no_prefix, lambda x: prefix + x)


def _get_unique(series):
    values = series.loc[series.notna()].value_counts()
    length = len(values)
    if length > 1:
        raise ValueError('inconsistent column')
    elif length == 1:
        return values.index[0]
    else:
        return pd.NA


def _expand_manual_works(df, coded_cols=None):

    if type(coded_cols) == dict:
        default_codes = coded_cols
        coded_cols = default_codes.keys()
    elif bg.iterable_not_string(coded_cols):
        default_codes = None
    else:
        coded_cols = [coded_cols]
        default_codes = None

    # combine columns to get a Series of all strings representing
    # documents
    entries = df.stack() \
                .reset_index(drop=True) \
                .drop_duplicates()
    entries.name = 'transcribed_value'

    # split on double underbars to get a dataframe of single-underbar
    # delimited strings, and join those columns with the original
    # column of input values
    entries = pd.concat(
        [entries, entries.str.split('__', expand=True)],
        axis=1
    )

    if coded_cols is not None:
        # check for values in coded columns with missing prefix codes
        has_prefix = entries[coded_cols].applymap(_check_for_prefix)
        has_prefix = has_prefix.loc[:, has_prefix.any()]
        some_prefixes_missing = ~has_prefix.all()

        # if there are values with missing prefix codes, insert defaults
        if some_prefixes_missing.any():
            if default_codes is None:
                raise ValueError(
                    'Some values in columns with prefix codes are '
                    'missing prefixes. Use the default_codes keyword '
                    'argument to assign default prefixes.'
                )
            mixed_columns = has_prefix.loc[:, ~has_prefix.all()].columns
            with_prefix = entries[mixed_columns]
            with_prefix = with_prefix.apply(
                _insert_default_prefix,
                args=(default_codes, has_prefix)
            )
            entries.loc[:, mixed_columns] = with_prefix

        disagged = entries[coded_cols].stack().str.split('_', expand=True)
        disagged.index = disagged.index.get_level_values(0)
        disagged = disagged.pivot(columns=0, values=1)

        uncoded_cols = [c for c in entries.columns if c not in coded_cols]

        entries = pd.concat([entries[uncoded_cols], disagged], axis='columns')

    return entries


def _manual_single_space_parser(s, space='|'):

    escaped_space = '\\' + space
    pattern = "(?<!\\\)[{}]".format(space)

    s = ' '.join(_regexsplit(pattern, s))

    return space.join(s.split(escaped_space))


def _manual_author_parser(x):
    return x.split('_')


_manual_string_parsers = {0: (_manual_author_parser, True)}

###############################################################################
#  define functions to normalize initial dataframes
###############################################################################


def _make_label_map(
    label_map_fname,
    node_types_fname,
    link_types_fname,
    context,
    assrtn_tgt_label,
    assrtn_tgt_link_type,
    assrtn_tgt_node_type
):

    tn = bg.TextNet(
        label_map_fname=label_map_fname,
        node_types_fname=node_types_fname,
        link_types_fname=link_types_fname
    )

    if assrtn_tgt_link_type not in tn.link_types['link_type'].array:
        raise ValueError(
            'Assertion target link type "{}" not recognized. Override '
            'the default link types by passing a value for '
            'link_types_fname'.format(assrtn_tgt_link_type)
        )
    if assrtn_tgt_node_type not in tn.node_types['node_type'].array:
        raise ValueError(
            'Assertion target node type "{}" not recognized. Override '
            'the default node types by passing a value for '
            'node_types_fname'.format(assrtn_tgt_node_type)
        )

    label_map = tn.label_map

    # locate labels relevant to this data and copy them
    in_context = (label_map['context'] == context)
    label_map = label_map.loc[in_context, :]

    # create an entry in the label map for the automatically
    # generated identifier strings
    if assrtn_tgt_label in label_map['input_label']:
        raise ValueError(
            'Requested assertion target label "{}" already exists in '
            'label_map["input_label"].'.format(assrtn_tgt_label)
        )
    else:
        tgt_label_map_entry = {
            'input_label': assrtn_tgt_label,
            'link_type': assrtn_tgt_link_type,
            'node_type': assrtn_tgt_node_type
        }
        label_map = label_map.append(tgt_label_map_entry, ignore_index=True)

    return label_map


def _normalize_input_data(data, assrtn_tgt_label):

    # extract the assertion target string values
    assrtn_tgt_strings = data[assrtn_tgt_label].drop_duplicates()
    assrtn_tgt_strings = assrtn_tgt_strings.array

    # transform the whole dataset into triples
    # (identifier, input column label, string value)
    data = data.melt(id_vars=assrtn_tgt_label,
                     var_name='input_label',
                     value_name='string')
    # only the string value column could possibly have nan values, and
    # nan string values are irrelevant, so drop them.
    data.dropna(inplace=True)
    data.loc[:, 'string'] = data.loc[:, 'string'] \
                                .apply(str.strip) \
                                .array
    # append the assertion target strings to the dataset
    data = data.append(
        pd.DataFrame({
            assrtn_tgt_label: assrtn_tgt_strings,
            'input_label': assrtn_tgt_label,
            'string': assrtn_tgt_strings
        }),
        ignore_index=True
    )

    return data


def _extract_metadata(data, label_map, string_parsers):

    no_links = label_map['link_type'].isna()
    metadata_label_map = label_map.loc[no_links, :].copy()
    metadata_label_map.drop(
        labels=['link_type', 'context', 'note'],
        axis=1,
        inplace=True
    )

    metadata = data.merge(metadata_label_map, on='input_label')
    metadata = metadata.groupby(by='input_label', group_keys=False)
    metadata = metadata.apply(
        _parse_grouped_strings,
        string_parsers,
        'input_label'
    )

    return metadata.reset_index(drop=True)


def _make_node_ids(node_data, assrtn_tgt_label, case_sensitive):
    # assume distinct strings represent distinct nodes for node ID
    # assignment
    distinct = node_data['string'].copy()
    if not case_sensitive:
        distinct = distinct.str.lower()

    # identical strings should map to the same node unless their node
    # types differ, so pair strings with node types
    distinct = distinct + node_data['node_type']
    # document titles are basically useless, so replace those with their
    # assertion target string
    is_doc = (node_data['node_type'] == 'documents')
    is_title = (node_data['link_type'] == 'title')
    is_doc_title = is_title & is_doc
    distinct.loc[is_doc_title] = node_data.loc[is_doc_title, assrtn_tgt_label]

    # assign an integer for every unique string in the array we
    # just built
    node_ids = bg.assign_integer_to_unique_values(
        distinct,
        value_label='string',
        new_integer_label='node_id'
    )

    return node_ids['node_id'].array

###############################################################################
#  define public functions to create textnets from input data
###############################################################################


def load_bibtex(
    bibtex_filename,
    encoding='utf-8',
    comment=None,
    output_file=None,
    output_encoding='utf-8',
    label_map_fname=None,
    node_types_fname=None,
    link_types_fname=None,
    case_sensitive=False,
    md_sep=None,
    string_parsers=_bibtex_string_parsers
):

    # tell us what's going on and get the current time
    print('processing bibtex file {}'.format(bibtex_filename))
    s = datetime.now()

    # parse the bibtex file and store the entries in a dataframe
    bibtex_parser = _BibTexParser(common_strings=True)
    with open(bibtex_filename, encoding=encoding) as f:
        bibdatabase = bibtex_parser.parse_file(f)
    data = pd.DataFrame(bibdatabase.entries)
    # the bibtex file asserts that various strings are attributes of
    # documents represented by the bibtex entries, so the assertion
    # targets are the string-valued bibtex entries. store those in a
    # data column labeled 'bg_bibtex_auto_src_string'
    assrtn_tgt_label = 'bg_bibtex_auto_src_string'
    bwriter = _BibTexWriter()

    def bw(x):
        return bwriter._entry_to_bibtex(bibdatabase.entries_dict[x['ID']])

    data[assrtn_tgt_label] = data.apply(bw, axis=1)

    # check how long bibtex parsing took and tell us about it
    e = datetime.now()
    time = e - s
    load_time = time.seconds + time.microseconds * 1e-6
    s = datetime.now()
    print(
        '\tloaded {} records from bibtex file in {:.1f} seconds'
        .format(len(data), load_time)
    )
    print('\tsize of initial dataframe: {:.1f} mb'.format(_getsize(data)/1e6))

    # dump bibtex-formatted text of this dataset
    if output_file is not False:

        if output_file is None:
            output_file = Path(bibtex_filename).stem + '.bib'
            output_file = 'source_files/auto_bg_src-' + output_file
            output_file = Path.joinpath(Path.cwd(), output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)
        else:
            output_file = Path(output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file.resolve(), 'w', encoding=output_encoding) as f:
            f.write(''.join(data[assrtn_tgt_label]))

    # generate (hopefully) unique strings for normalization. the unique
    # strings represent targets of assertions, and they will be stored
    # in a new data column with the label defined below.
    # assrtn_tgt_label = 'bg_bibtex_auto_id'
    # bw = _BibTexWriter()
    # lambda x: bw._entry_to_bibtex(bibdatabase.entries_dict(x['ID']))
    # data[assrtn_tgt_label] = data.apply(lambda x: str(dict(x)), axis=1)

    # get a label map and update it with an entry for the assertion
    # targets. in this case the targets are strings in a data column
    # labeled by assrtn_tgt_label and the strings are equivalent to
    # titles of documents.
    label_map = _make_label_map(
        label_map_fname,
        node_types_fname,
        link_types_fname,
        'bibtex',
        assrtn_tgt_label,
        'title',
        'documents'
    )

    # melt the input data
    data = _normalize_input_data(data, assrtn_tgt_label)
    # data columns are now:
    #       assrtn_tgt_label, input_label, string

    # nodes are represented in the label map with rows that have values
    # in the link_type column, so get those rows.
    has_link = label_map['link_type'].notna()
    node_label_map = label_map.loc[has_link, :].copy()
    # drop extraneous label map columns
    node_label_map.drop(
        labels=['metadata_column', 'context', 'note'],
        axis=1,
        inplace=True
    )
    # get nodes by merging data into the node label map
    node_data = data.merge(node_label_map, on='input_label')

    # node_data columns are now:
    #       assrtn_tgt_label,
    #       input_label,
    #       string,
    #       link_type,
    #       node_type

    # get functions to parse strings with different input labels.
    # the parsers transform the dataframe, so reset the index after
    # applying
    node_data = node_data.groupby(by='input_label', group_keys=False)
    node_data = node_data.apply(_parse_grouped_strings, string_parsers)
    node_data.reset_index(drop=True, inplace=True)

    # make new node ID values and insert them in node_data
    node_data['node_id'] = _make_node_ids(node_data,
                                          assrtn_tgt_label,
                                          case_sensitive)

    # metadata (data about nodes) is anything that was excluded when
    # the label map was merged into the input data.
    metadata = _extract_metadata(data, label_map, string_parsers)

    # create a new TextNet object from this input
    tn = bg.create_textnet_from_node_data(
        node_data,
        assrtn_tgt_label,
        label_map_fname=label_map_fname,
        metadata=metadata,
        md_sep=md_sep
    )

    # create a new entry in tn.sources
    new_sources_row = pd.DataFrame({'source': output_file}, index=[0])
    tn.append_to('sources', new_sources_row, comment=comment)

    e = datetime.now()

    time = e - s
    create_time = time.seconds + time.microseconds * 1e-6

    print(
        '\tcreated TextNet in {:.1f} seconds\n\toverall time {:.1f} '
        'seconds'.format(create_time, load_time + create_time)
    )

    return tn


def load_manual_transcript(
    transcript_filename,
    encoding='utf-8',
    comment=None,
    output_file=None,
    label_map_fname=None,
    node_types_fname=None,
    link_types_fname=None,
    case_sensitive=False,
    md_sep=None,
    string_parsers=_manual_string_parsers,
    check_missing_values=True,
    whitespace_placeholder='|'
):

    # tell us what's going on and get the current time
    print('processing manual transcript file {}'.format(transcript_filename))
    s = datetime.now()

    # parse the transcript and store the entries in a dataframe
    with open(transcript_filename, encoding=encoding) as f:
        data = pd.read_csv(transcript_filename)

    if check_missing_values:
        # if there are missing values in the csv file, check that either the
        # first column has a value or the second column has a value in each
        # line, never both and never neither
        missing_values = data[[0, 1]].isna()
        if missing_values.any(axis=None):
            both_values_nan = missing_values.all(axis='columns')
            neither_value_nan = ~both_values_nan

            error_text = (
                'If there are missing values in a manually '
                'transcribed file then each line in the file must '
                'have a value in the first csv column or a value in '
                'the second csv column.'
            )

            if both_values_nan.any():
                raise ValueError(
                    error_text + 'Input file has at least one line '
                    'missing values in both columns.'
                )

            if neither_value_nan.any():
                raise ValueError(
                    error_text + 'Input file has at least one line '
                    'with a value in both columns.'
                )

    # populate any missing values in the first column
    data[:, 0] = data[:, 0].ffill()
    # drop lines missing values in the second column
    data = data.dropna(subset=[1])

    # convert placeholders back to whitespace

    def func(x):
        return _manual_single_space_parser(x, whitespace_placeholder)
    data = data.applymap(func)

    # check how long parsing took and tell us about it
    e = datetime.now()
    time = e - s
    load_time = time.seconds + time.microseconds * 1e-6
    s = datetime.now()
    print('\tloaded {} records from bibtex file in {:.1f} seconds'
          .format(len(data), load_time))
    print('\tsize of initial dataframe: {:.1f} mb'.format(_getsize(data)/1e6))

    # dump csv of this data set
    if output_file is not False:

        if output_file is None:
            output_file = Path(transcript_filename).stem + '.csv'
            output_file = 'source_files/auto_bg_src-' + output_file
            output_file = Path.joinpath(Path.cwd(), output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)
        else:
            output_file = Path(output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)

        _copyfile(transcript_filename, output_file.resolve())

    # generate (hopefully) unique strings for normalization. the unique
    # strings represent targets of assertions, and they will be stored
    # in a new data column with the label defined below.
    assrtn_tgt_label = 'bg_bibtex_auto_id'
    data[assrtn_tgt_label] = data.apply(lambda x: str(dict(x)), axis=1)

    # get a label map and update it with an entry for the assertion
    # targets. in this case the targets are strings in a data column
    # labeled by assrtn_tgt_label and the strings are equivalent to
    # titles of documents.
    label_map = _make_label_map(
        label_map_fname,
        node_types_fname,
        link_types_fname,
        'bibtex',
        assrtn_tgt_label,
        'title',
        'documents'
    )

    # melt the input data
    data = _normalize_input_data(data, assrtn_tgt_label)
    # data columns are now:
    #       assrtn_tgt_label, input_label, string

    # nodes are represented in the label map with rows that have values
    # in the link_type column, so get those rows.
    has_link = label_map['link_type'].notna()
    node_label_map = label_map.loc[has_link, :].copy()
    # drop extraneous label map columns
    node_label_map.drop(labels=['metadata_column', 'context', 'note'],
                        axis=1,
                        inplace=True)
    # get nodes by merging data into the node label map
    node_data = data.merge(node_label_map, on='input_label')

    # node_data columns are now:
    #       assrtn_tgt_label,
    #       input_label,
    #       string,
    #       link_type,
    #       node_type

    # get functions to parse strings with different input labels.
    # the parsers transform the dataframe, so reset the index after
    # applying
    node_data = node_data.groupby(by='input_label', group_keys=False)
    node_data = node_data.apply(_parse_grouped_strings, string_parsers)
    node_data.reset_index(drop=True, inplace=True)

    # make new node ID values and insert them in node_data
    node_data['node_id'] = _make_node_ids(node_data,
                                          assrtn_tgt_label,
                                          case_sensitive)

    # metadata (data about nodes) is anything that was excluded when
    # the label map was merged into the input data.
    metadata = _extract_metadata(data, label_map, string_parsers)

    # create a new TextNet object from this input
    tn = bg.create_textnet_from_node_data(node_data,
                                          assrtn_tgt_label,
                                          label_map_fname=label_map_fname,
                                          metadata=metadata,
                                          md_sep=md_sep)

    # create a new entry in tn.sources
    new_sources_row = pd.DataFrame({'source': output_file}, index=[0])
    tn.append_to('sources', new_sources_row, comment=comment)

    e = datetime.now()

    time = e - s
    create_time = time.seconds + time.microseconds * 1e-6

    print(
        '\tcreated TextNet in {:.1f} seconds\n\toverall time {:.1f} '
        'seconds'.format(create_time, load_time + create_time)
    )

    return tn
