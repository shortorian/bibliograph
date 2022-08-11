import bibliograph as bg
import pandas as pd
from bibtexparser.bparser import BibTexParser
from io import StringIO


def test_manual_annotation_nodes_column_nan_states():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    notna_subset = [
        'node_type_id', 'name_string_id', 'abbr_string_id', 'date_inserted'
    ]

    assert not tn.nodes[notna_subset].isna().any(axis=None)
    assert tn.nodes['date_modified'].isna().all()


def test_manual_annotation_edges_column_nan_states():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    notna_subset = [
        'src_node_id',
        'tgt_node_id',
        'ref_node_id',
        'link_type_id',
        'date_inserted'
    ]

    assert not tn.edges[notna_subset].isna().any(axis=None)
    assert tn.edges['date_modified'].isna().all()


def test_manual_annotation_strings_column_nan_states():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    notna_subset = ['node_id', 'string', 'date_inserted']

    assert not tn.strings[notna_subset].isna().any(axis=None)
    assert tn.strings['date_modified'].isna().all()


def test_manual_annotation_assertions_column_nan_states():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    notna_subset = [
        'inp_string_id',
        'src_string_id',
        'tgt_string_id',
        'ref_string_id',
        'link_type_id',
        'date_inserted'
    ]

    assert not tn.assertions[notna_subset].isna().any(axis=None)
    assert tn.assertions['date_modified'].isna().all()


def test_manual_annotation_num_strings_is_65():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    assert len(tn.strings) == 64


def test_manual_annotation_node_types_with_single_strings():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    single_string_node_types = [
        'acknowledgement',
        'affiliation',
        'shorthand_text',
        'shorthand_entry_syntax',
        'shorthand_link_syntax'
    ]

    counts = tn.resolve_strings()['node_type'].value_counts()
    assert counts.loc[counts == 1].index.isin(single_string_node_types).all()


def test_manual_annotation_node_type_0_is_shorthand_text():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    shorthand_text_node_type_id = tn.id_lookup('node_types', 'shorthand_text')

    assert (shorthand_text_node_type_id == 0)


def test_manual_annotation_single_string_has_node_type_shorthand_text():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    shorthand_text_string = tn.resolve_strings().query(
        'node_type == "shorthand_text"'
    )

    assert (shorthand_text_string.node_type.squeeze() == "shorthand_text")


def test_manual_annotation_shorthand_text_string_startswith():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    shorthand_text_string = tn.resolve_strings().query(
        'node_type == "shorthand_text"'
    )
    assert (shorthand_text_string.string.squeeze().startswith(
        'This is stuff shorthand ignores'
    ))


def test_aliasing_num_case_sensitive_actor_nodes_is_5():

    aliases_dict = {'actor': 'bibliograph/test_data/aliases_actor.csv'}

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    assert len(tn.resolve_nodes().query('node_type == "actor"')) == 5


def test_aliasing_num_case_sensitive_work_nodes_is_10():

    aliases_dict = {'work': 'bibliograph/test_data/aliases_work.csv'}

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    assert len(tn.resolve_nodes().query('node_type == "work"')) == 10


def test_aliasing_num_case_insensitive_actor_nodes_is_4():

    aliases_dict = {'actor': 'bibliograph/test_data/aliases_actor.csv'}

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    assert len(tn.resolve_nodes().query('node_type == "actor"')) == 4


def test_aliasing_num_case_insensitive_work_nodes_is_10():

    aliases_dict = {'work': 'bibliograph/test_data/aliases_work.csv'}

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    assert len(tn.resolve_nodes().query('node_type == "work"')) == 10


def test_aliasing_both_sets_num_case_insensitive_alias_assrtns_is_17():

    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    assert len(tn.resolve_assertions().query('link_type == "alias"')) == 17


def test_aliasing_node_3_work_bams_values():

    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    node_3_aliases = [
        'bams',
        'Bulletin of the American Meteorological Society',
        'bulletin of the american meteorological society'
    ]

    strings_with_node_3 = tn.strings.loc[tn.strings['node_id'] == 3, 'string']

    assert (strings_with_node_3 == node_3_aliases).all().all()


def test_aliasing_node_1_actor_asmith_values():
    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    node_1_aliases = ['asmith', 'Alice Smith', 'alice smith']

    strings_with_node_1 = tn.strings.loc[tn.strings['node_id'] == 1, 'string']

    assert (strings_with_node_1 == node_1_aliases).all().all()


def test_aliasing_node_2_actor_bwu_values():
    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    node_2_aliases = [
        'bwu', 'Elizabeth Wu', 'Beth Wu', 'beth wu', 'elizabeth wu'
    ]

    strings_with_node_2 = tn.strings.loc[tn.strings['node_id'] == 2, 'string']

    assert (strings_with_node_2 == node_2_aliases).all().all()


def test_aliasing_node_0_actor_nasa_values():
    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_with_aliases.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    node_0_aliases = [
        'NASA',
        'National Aeronautics and Space Administration',
        'nasa',
        'national aeronautics and space administration'
    ]

    strings_with_node_0 = tn.strings.loc[tn.strings['node_id'] == 0, 'string']

    assert (strings_with_node_0 == node_0_aliases).all().all()


def test_auto_aliasing_lemonem_and_personso_assertions():

    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    constraints_fname = "bibliograph/resources/default_link_constraints.csv"

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_for_auto_aliasing.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        automatic_aliasing=True,
        link_constraints_fname=constraints_fname,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        comment_char='#',
    )

    input_string = (
        "bibliograph.core.slurp_shorthand(**{'shorthand_fname': "
        "'bibliograph/test_data/shorthand_for_auto_aliasing.shnd', "
        "'entry_syntax_fname': "
        "'bibliograph/resources/default_entry_syntax.csv', "
        "'link_syntax_fname': 'bibliograph/resources/default_link_syntax.csv',"
        " 'syntax_case_sensitive': False, 'allow_redundant_items': False, "
        "'aliases_dict': {'actor': 'bibliograph/test_data/aliases_actor.csv', "
        "'work': 'bibliograph/test_data/aliases_work.csv'}, "
        "'aliases_case_sensitive': False, 'automatic_aliasing': True, "
        "'link_constraints_fname': "
        "'bibliograph/resources/default_link_constraints.csv', "
        "'links_excluded_from_edges': None, 'item_separator': '__', "
        "'space_char': '|', 'na_string_values': '!', 'na_node_type': "
        "'missing', 'default_entry_prefix': 'wrk', 'comment_char': '#'})"
    )

    correct_assertions = pd.DataFrame(
        {
            'inp_string': input_string,
            'src_string': ['margaret lemone', 's. o. person'],
            'tgt_string': ['lemonem', 'personso'],
            'link_type': 'alias',
            'inp_node_type': 'python_function',
            'src_node_type': 'actor',
            'tgt_node_type': 'actor',
            'ref_node_type': 'alias_reference'
        },
        index=pd.Index([76, 79]).astype(tn.big_id_dtype)
    )

    assertion_selection = tn.resolve_assertions().loc[
        correct_assertions.index,
        correct_assertions.columns
    ]

    assert (assertion_selection == correct_assertions).all().all()


def test_auto_aliasing_by_stitle_vol_pg_link_constraints():

    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    constraints_fname = "bibliograph/resources/default_link_constraints.csv"

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_for_auto_aliasing.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        automatic_aliasing=True,
        link_constraints_fname=constraints_fname,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        comment_char='#',
    )

    node_10_aliases = [
        'asmith_bwu__1999__bams__101__803__xxx',
        'smitha_wub__1999__bams__101__803'
    ]
    strings_with_node_10 = tn.strings.loc[
        tn.strings['node_id'] == 10,
        'string'
    ]

    assert (strings_with_node_10 == node_10_aliases).all().all()


def test_auto_aliasing_by_doi_link_constraints():

    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    constraints_fname = "bibliograph/resources/default_link_constraints.csv"

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/shorthand_for_auto_aliasing.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        automatic_aliasing=True,
        link_constraints_fname=constraints_fname,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        comment_char='#',
    )

    node_14_aliases = [
        'Alice Smith_Elizabeth Wu__1998__bams__100__42__yyy',
        'smitha_wub__1998__!__!__!__doi:yyy'
    ]
    strings_with_node_14 = tn.strings.loc[
        tn.strings['node_id'] == 14,
        'string'
    ]

    assert (strings_with_node_14 == node_14_aliases).all().all()


def test_s_d_strings_subset_same_as_shnd_strings_subset():

    s = bg.Shorthand(
        entry_syntax="bibliograph/resources/default_entry_syntax.csv",
        link_syntax="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False
    )

    shorthand_text = (
        'left_entry, right_entry, link_tags_or_override, reference\n'
        'Auth1__2000__BAMS__100__!__xxx__ tag1 tag2, '
        'Auth2__1990__BAMS__90__40__yyy__ tag1 tag3, '
        'LinkTag'
    )

    self_descriptive = (
        'left_entry, right_entry, link_tags_or_override, reference\n'
        '____work__author_actor_Auth1__published_date_2000__supertitle_work_'
        'BAMS__volume_work_100__page_work_!__doi_identifier_xxx__ tag1 tag2, '
        '____work__author_actor_Auth2__published_date_1990__supertitle_work_'
        'BAMS__volume_work_90__page_work_40__doi_identifier_yyy__ tag1 tag3, '
        'lt__cited LinkTag'
    )

    parsed_shnd_sources = s.parse_text(
        StringIO(shorthand_text),
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        comment_char='#'
    )

    parsed_s_d_sources = s.parse_text(
        StringIO(self_descriptive),
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        comment_char='#'
    )

    node_types = [
        'entry',
        'shorthand_text',
        'shorthand_link_syntax',
        'shorthand_entry_syntax',
        'work'
    ]

    shnd_strings = parsed_shnd_sources.resolve_strings()
    shnd_strings = shnd_strings.loc[
        ~shnd_strings['node_type'].isin(node_types)
    ]
    shnd_strings = shnd_strings.loc[shnd_strings['string'] != '1']
    shnd_strings = shnd_strings.sort_values(by='string').reset_index(drop=True)

    s_d_strings = parsed_s_d_sources.resolve_strings()
    s_d_strings = s_d_strings.loc[~s_d_strings['node_type'].isin(node_types)]
    s_d_strings = s_d_strings.sort_values(by='string').reset_index(drop=True)

    assert (shnd_strings == s_d_strings).all().all()


def test_bibtex_identifier_parsing():

    bibtex_parser = BibTexParser(common_strings=True)
    bibtex_fname = "bibliograph/test_data/bibtex_test_data_short.bib"
    with open(bibtex_fname, encoding='utf8') as f:
        bibdatabase = bibtex_parser.parse_file(f)

    data = pd.DataFrame(bibdatabase.entries)

    s = bg.Shorthand(
        entry_syntax="bibliograph/resources/default_bibtex_syntax.csv",
        syntax_case_sensitive=False
    )

    parsed = s.parse_items(
        data,
        space_char='|',
        na_string_values='!',
        na_node_type='missing'
    )

    parsed_identifiers = parsed.strings.query('node_type_id == 8')

    check = pd.Series([
        '10.1038/194638b0',
        '10.1175/1520-0493(1962)090<0311:OTOKEB>2.0.CO;2',
        '10.3402/tellusa.v14i3.9551',
        '10.1175/1520-0477-43.9.451',
        '10.3402/tellusa.v14i4.9569',
        '10.1007/BF02317953',
        '10.1007/BF02247180',
        '10.1029/JZ068i011p03345',
        '10.1029/JZ068i009p02375',
    ])

    assert (check == parsed_identifiers['string'].array).all()


def test_parsed_bibtex_items_resolve_links_has_no_nans():

    bibtex_parser = BibTexParser(common_strings=True)
    bibtex_test_data_fname = "bibliograph/test_data/bibtex_test_data_short.bib"
    with open(bibtex_test_data_fname, encoding='utf8') as f:
        bibdatabase = bibtex_parser.parse_file(f)

    data = pd.DataFrame(bibdatabase.entries)

    s = bg.Shorthand(
        entry_syntax="bibliograph/resources/default_bibtex_syntax.csv",
        syntax_case_sensitive=False
    )

    parsed = s.parse_items(
        data.iloc[:4],
        space_char='|',
        na_string_values='!',
        na_node_type='missing'
    )

    assert (~parsed.resolve_links().isna()).any().any()


def test_single_column_wrk_synthesis():

    s = bg.Shorthand(
        entry_syntax="bibliograph/resources/default_entry_syntax.csv",
        link_syntax="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False
    )

    parsed = s.parse_text(
        'bibliograph/test_data/single_column.shnd',
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=0,
        comment_char='#',
        drop_na=False
    )

    synthesized = parsed.synthesize_shorthand_entries('wrk', fill_spaces=True)

    check = pd.Series([
        'asmith_bwu__1999__s_bams__101__803__xxx',
        'asmith_bwu__1998__s_bams__100__42__yyy',
        'bjones__1975__s_jats__90__1__!',
        'bwu__1989__t_long|title__x__80__!',
        'Some|Author__1989__t_A|Title|With|\\#__x__x__!',
        'asmith_bwu__2008__s_bams__110__1__zzz'
    ])

    assert (check == synthesized).all()


def test_manual_annotation_note_synthesis():

    s = bg.Shorthand(
        entry_syntax="bibliograph/resources/default_entry_syntax.csv",
        link_syntax="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False
    )

    parsed = s.parse_text(
        'bibliograph/test_data/manual_annotation.shnd',
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    synthesized = parsed.synthesize_shorthand_entries(entry_node_type='note')

    check = pd.Series([
        'this is an article I made up for testing',
        'here\'s a note with an escaped\\__item separator and some '
        '"quotation marks"'
    ])

    assert (check == synthesized).all()


def test_manual_annotation_wrk_synthesis():

    s = bg.Shorthand(
        entry_syntax="bibliograph/resources/default_entry_syntax.csv",
        link_syntax="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False
    )

    parsed = s.parse_text(
        'bibliograph/test_data/manual_annotation.shnd',
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    synthesized = parsed.synthesize_shorthand_entries('wrk', fill_spaces=True)

    check = pd.Series([
        'asmith_bwu__1999__s_bams__101__803__xxx',
        'asmith_bwu__1998__s_bams__100__42__yyy',
        'bjones__1975__s_jats__90__1__!',
        'bwu__1989__t_long|title__x__80__!',
        'Some|Author__1989__t_Title|With|\\#__x__x__!',
        'asmith_bwu__2008__s_bams__110__1__zzz'
    ])

    assert (check == synthesized).all()


def test_parsed_manual_annotation_resolve_links_has_no_nans():

    s = bg.Shorthand(
        entry_syntax="bibliograph/resources/default_entry_syntax.csv",
        link_syntax="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False
    )

    parsed = s.parse_text(
        'bibliograph/test_data/manual_annotation.shnd',
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    assert (~parsed.resolve_links().isna()).any().any()
