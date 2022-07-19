import bibliograph as bg
import pandas as pd
import shorthand as shnd
from bibtexparser import dumps as _dump_bibtex_string
from bibtexparser.bibdatabase import BibDatabase as _bibtex_db
from bibtexparser.bparser import BibTexParser as _bibtexparser
from datetime import datetime


def _make_syntax_metadata_table(textnet, parsed_shnd, entry_or_link):

    if entry_or_link not in ['entry', 'link']:
        raise ValueError('entry_or_link must be either "entry" or "link')

    try:
        syntax_case_sensitive = parsed_shnd.__getattribute__(
            entry_or_link + '_syntax_case_sensitive'
        )

    except AttributeError:
        syntax_case_sensitive = parsed_shnd.syntax_case_sensitive

    syntax_node_type = 'shorthand_{}_syntax'.format(entry_or_link)

    metadata = {
        'case_sensitive': syntax_case_sensitive,
        'allow_redundant_items': parsed_shnd.allow_redundant_items
    }

    textnet._insert_metadata_table(syntax_node_type, metadata)


def textnet_from_parsed_shorthand(
    parsed,
    input_source_string,
    input_source_node_type
):

    # get a node_type_id for input source
    try:
        input_source_node_type_id = parsed.id_lookup(
            'node_types',
            input_source_node_type
        )

    except KeyError:

        new_node_type = shnd.util.normalize_types(
            pd.Series([input_source_node_type]),
            parsed.node_types
        )
        parsed.node_types = pd.concat([parsed.node_types, new_node_type])

        input_source_node_type_id = parsed.node_types.index[-1]

    # Insert a strings row for the input filename
    new_string = {
        'string': input_source_string,
        'node_type_id': input_source_node_type_id
    }
    new_string = shnd.util.normalize_types(new_string, parsed.strings)
    parsed.strings = pd.concat([parsed.strings, new_string])

    # create a TextNet and cache a string for the insertion dates
    tn = bg.TextNet()

    time_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # create a column for the assertion inp_string_id
    inp_string_id = pd.DataFrame(
        new_string.index[0],
        columns=['inp_string_id'],
        index=parsed.links.index.astype(tn.big_id_dtype)
    )
    inp_string_id = inp_string_id.astype({'inp_string_id': tn.big_id_dtype})

    # create the assertions table
    tn.assertions = pd.concat([inp_string_id, parsed.links], axis='columns')
    tn.assertions = tn.assertions.drop('list_position', axis='columns')
    tn.assertions.loc[:, 'date_inserted'] = time_string
    tn.assertions.loc[:, 'date_modified'] = pd.NA

    # create the strings table
    tn.strings = parsed.strings.copy()
    tn.strings.loc[:, 'date_inserted'] = time_string
    tn.strings.loc[:, 'date_modified'] = pd.NA

    # create the node_types table
    tn.node_types = pd.DataFrame(
        {'node_type': parsed.node_types.array, 'description': pd.NA},
        index=parsed.node_types.index.astype(tn.small_id_dtype)
    )

    # create the link_types table
    tn.link_types = pd.DataFrame(
        {'node_type': parsed.link_types.array, 'description': pd.NA},
        index=parsed.link_types.index.astype(tn.small_id_dtype)
    )

    # create the assertion_tags table
    tn.assertion_tags = parsed.link_tags.rename(
        columns={'link_id': 'assertion_id'}
    )

    '''
    Now assume that all strings should refer to unique nodes and
    generate nodes and edges tables on that basis.
    '''

    tn.nodes = tn.strings.index.astype(tn.big_id_dtype).array
    tn.nodes = pd.DataFrame(
        {
            'node_type_id': tn.strings['node_type_id'],
            'name_string_id': tn.nodes,
            'abbr_string_id': tn.nodes,
            'date_inserted': time_string,
            'date_modified': pd.NA
        },
        index=tn.nodes.astype(tn.big_id_dtype)
    )
    tn.nodes = tn.nodes.astype({
        'node_type_id': tn.small_id_dtype,
        'name_string_id': tn.big_id_dtype,
        'abbr_string_id': tn.big_id_dtype,
        'date_inserted': str,
        'date_modified': str
    })

    tn.edges = tn.assertions.copy()
    tn.edges = tn.edges.drop('inp_string_id', axis='columns')
    tn.edges = tn.edges.rename({
        'src_string_id': 'src_node_id',
        'tgt_string_id': 'tgt_node_id',
        'ref_string_id': 'ref_node_id',
    })

    tn.edge_tags = tn.assertion_tags.rename({'assertion_id': 'edge_id'})

    if 'shorthand_text' in parsed.node_types.array:
        text_node_type = 'shorthand_text'
    elif 'items_text' in parsed.node_types.array:
        text_node_type = 'items_text'
    else:
        raise ValueError(
            'Unrecognized input full text type.\nNone of '
            '["shorthand_text", "items_text"] in parsed shorthand node '
            'types.\n'
        )

    text_metadata = {
        'space_char': parsed.space_char,
        'na_string_values': parsed.na_string_values,
        'na_node_type': parsed.na_node_type,
        'item_separator': parsed.item_separator
    }

    tn._insert_metadata_table(text_node_type, text_metadata)

    _make_syntax_metadata_table(tn, parsed, 'entry')

    if 'shorthand_link_syntax' in parsed.node_types.array:
        _make_syntax_metadata_table(tn, parsed, 'link')

    return tn


def slurp_bibtex(
    bibtex_fname,
    entry_syntax_fname,
    syntax_case_sensitive=True,
    allow_redundant_items=False,
    encoding='utf8',
    **kwargs
):

    # initialize bibtex and shorthand parsers
    bibtex_parser = _bibtexparser(common_strings=True)
    s = shnd.Shorthand(
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
    with open(bibtex_fname, encoding=encoding) as f:
        parsed = s.parse_items(
            pd.DataFrame(bibtex_parser.parse_file(f).entries),
            entry_writer=entry_writer,
            **kwargs
        )

    return textnet_from_parsed_shorthand(parsed, bibtex_fname, 'file')


def slurp_shorthand(
    shorthand_fname,
    entry_syntax_fname,
    link_syntax_fname=None,
    syntax_case_sensitive=True,
    allow_redundant_items=False,
    **kwargs
):

    s = shnd.Shorthand(
        entry_syntax=entry_syntax_fname,
        link_syntax=link_syntax_fname,
        syntax_case_sensitive=syntax_case_sensitive,
        allow_redundant_items=allow_redundant_items
    )

    parsed = s.parse_text(shorthand_fname, **kwargs)

    return textnet_from_parsed_shorthand(parsed, shorthand_fname, 'file')
