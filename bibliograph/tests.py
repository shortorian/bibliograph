import bibliograph as bg
import pandas as pd
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


def test_manual_annotation_num_strings_is_75():

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

    assert len(tn.strings) == 75


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
        'agreement', 'affiliation', '_python_function_call'
    ]

    counts = tn.resolve_strings()['node_type'].value_counts()
    assert counts.loc[counts == 1].index.isin(single_string_node_types).all()


def test_manual_annotation_table_dtypes():

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

    table_names = tn._node_side_tables + tn._string_side_tables

    # edge_tags is empty
    table_names = [n for n in table_names if n != 'edge_tags']

    actual_dtypes = {
        name: dict(tn.__getattr__(name).dtypes) for name in table_names
    }
    expected_dtypes = {
        name: tn.__getattr__('_{}_dtypes'.format(name)) for name in table_names
    }

    assert all([
        actual_dtypes[name] == expected_dtypes[name] for name in table_names
    ])


def test_manual_annotation_index_dtypes():

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

    table_names = tn._node_side_tables + tn._string_side_tables

    # edge_tags is empty
    table_names = [n for n in table_names if n != 'edge_tags']

    actual_index_dtypes = {
        name: tn.__getattr__(name).index.dtype
        for name in table_names
    }
    expected_index_dtypes = {
        name: tn.__getattr__('_{}_index_dtype'.format(name))
        for name in table_names
    }

    assert all([
        actual_index_dtypes[name] == expected_index_dtypes[name]
        for name in table_names
    ])


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

    shorthand_text_string = tn.resolve_assertions(link_type='shorthand_data')
    shorthand_text_string = bg.util.get_single_value(
        shorthand_text_string,
        'tgt_string'
    )
    assert shorthand_text_string.startswith('This is stuff shorthand ignores')


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


def test_auto_aliasing_lemonem_personso_yyy_assertions():

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

    manual_assertion_index = [80, 82]
    correct_manual_aliases = pd.DataFrame(
        {
            'src_string': ['soperson', 'beth wu'],
            'tgt_string': ['s. o. person', 'Elizabeth Wu'],
            'link_type': 'alias',
            'src_node_type': 'actor',
            'tgt_node_type': 'actor',
            'ref_node_type': '_literal_csv'
        },
        index=pd.Index(manual_assertion_index).astype(tn.big_id_dtype)
    )

    auto_assertion_index = [93, 96, 100]
    correct_automatic_aliases = pd.DataFrame(
        {
            'src_string': ['margaret lemone', 's. o. person', 'doi:yyy'],
            'tgt_string': ['lemonem', 'personso', 'yyy'],
            'link_type': 'alias',
            'src_node_type': ['actor', 'actor', 'identifier'],
            'tgt_node_type': ['actor', 'actor', 'identifier'],
            'ref_node_type': '_python_function_call'
        },
        index=pd.Index(auto_assertion_index).astype(tn.big_id_dtype)
    )

    correct_assertions = pd.concat([
        correct_manual_aliases,
        correct_automatic_aliases
    ])

    assertion_selection = tn.resolve_assertions().loc[
        manual_assertion_index + auto_assertion_index,
        correct_manual_aliases.columns
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

    node_6_aliases = [
        'asmith_bwu__1999__bams__101__803__xxx',
        'smitha_wub__1999__bams__101__803'
    ]
    strings_with_node_6 = tn.get_strings_by_node_id(6)['string']

    assert (strings_with_node_6 == node_6_aliases).all().all()


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

    node_5_aliases = [
        'Alice Smith_Elizabeth Wu__1998__bams__100__42__yyy',
        'smitha_wub__1998__!__!__!__doi:yyy'
    ]
    strings_with_node_5 = tn.get_strings_by_node_id(5)['string']

    assert (strings_with_node_5 == node_5_aliases).all().all()


def test_s_d_strings_subset_same_as_shnd_strings_subset():

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

    shnd_tn = bg.slurp_shorthand(
        StringIO(shorthand_text),
        entry_syntax_fname="bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        comment_char='#'
    )

    s_d_tn = bg.slurp_shorthand(
        StringIO(self_descriptive),
        entry_syntax_fname="bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
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

    shnd_strings = shnd_tn.resolve_strings()
    shnd_strings = shnd_strings.loc[
        ~shnd_strings['node_type'].isin(node_types)
    ]
    shnd_strings = shnd_strings.loc[shnd_strings['string'] != '1']
    shnd_strings = shnd_strings.sort_values(by='string').reset_index(drop=True)

    s_d_strings = s_d_tn.resolve_strings()
    s_d_strings = s_d_strings.loc[~s_d_strings['node_type'].isin(node_types)]
    s_d_strings = s_d_strings.sort_values(by='string').reset_index(drop=True)

    q = '({})'.format(') & ('.join([
        '~node_type.str.contains("literal")',
        '~node_type.str.contains("python")'
    ]))

    shnd_strings = shnd_strings.query(q)[['string', 'node_type']]
    s_d_strings = s_d_strings.query(q)[['string', 'node_type']]

    assert (shnd_strings == s_d_strings).all().all()


def test_bibtex_identifier_parsing():

    tn = bg.slurp_bibtex(
        "bibliograph/test_data/bibtex_test_data_short.bib",
        entry_syntax_fname="bibliograph/resources/default_bibtex_syntax.csv",
        syntax_case_sensitive=False,
        allow_redundant_items=True,
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
    )

    identifier_type_id = tn.id_lookup('node_types', 'identifier')
    identifier_nodes = tn.nodes.query('node_type_id == @identifier_type_id')
    parsed_identifiers = tn.strings.query(
        'node_id.isin(@identifier_nodes.index)'
    )

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


def test_single_column_wrk_synthesis():

    tn = bg.slurp_single_column(
        'bibliograph/test_data/single_column.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=0,
        comment_char='#'
    )

    synthesized = tn.synthesize_shorthand_entries(
        node_type='work',
        fill_spaces=True,
        hide_default_entry_prefixes=True
    )

    check = pd.Series([
        'asmith_bwu__1999__s_bams__101__803__xxx',
        'asmith_bwu__1998__s_bams__100__42__yyy',
        'bjones__1975__s_jats__90__1__!',
        'bwu__1989__t_long|title__x__80__!',
        'Some|Author__1989__t_A|Title|With|\\#__x__x__!',
        'asmith_bwu__2008__s_bams__110__1__zzz'
    ])

    assert (check == synthesized.reset_index(drop=True)).all()


def test_manual_annotation_note_synthesis():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        automatic_aliasing=True,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#'
    )

    synthesized = tn.synthesize_shorthand_entries(node_type='note')

    check = pd.Series(
        [
            'not__this is an article I made up for testing',
            'not__here\'s a note with an escaped\\__item separator and '
            'some "quotation marks"'
        ],
        index=synthesized.index
    )

    assert (check == synthesized).all()


def test_manual_annotation_wrk_synthesis():

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

    synthesized = tn.synthesize_shorthand_entries(
        node_type='work',
        hide_default_entry_prefixes=True,
        fill_spaces=True
    )

    check = pd.Series(
        [
            'asmith_bwu__1999__s_bams__101__803__xxx',
            'asmith_bwu__1998__s_bams__100__42__yyy',
            'bjones__1975__s_jats__90__1__!',
            'bwu__1989__t_long|title__x__80__!',
            'Some|Author__1989__t_Title|With|\\#__x__x__!',
            'asmith_bwu__2008__s_bams__110__1__zzz'
        ],
        index=synthesized.index
    )

    assert (check == synthesized).all()


def test_bibtex_input_uses_all_strings():

    tn = bg.slurp_bibtex(
        "bibliograph/test_data/bibtex_test_data_short.bib",
        entry_syntax_fname="bibliograph/resources/default_bibtex_syntax.csv",
        syntax_case_sensitive=False,
        allow_redundant_items=True,
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
    )

    asserted_string_ids = tn.assertions[
        [c for c in tn.assertions.columns if c.endswith('string_id')]
    ]
    asserted_string_ids = pd.Series(asserted_string_ids.stack().unique())
    assertion_tag_string_ids = pd.Series(
        tn.assertion_tags['tag_string_id'].unique()
    )
    used_string_ids = pd.concat([
        asserted_string_ids,
        assertion_tag_string_ids
    ])
    unused_strings = tn.strings.query('~index.isin(@used_string_ids)')

    assert unused_strings.empty


def test_bibtex_input_connects_all_non_tag_nodes():

    tn = bg.slurp_bibtex(
        "bibliograph/test_data/bibtex_test_data_short.bib",
        entry_syntax_fname="bibliograph/resources/default_bibtex_syntax.csv",
        syntax_case_sensitive=False,
        allow_redundant_items=True,
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
    )

    connected_node_ids = tn.edges[
        [c for c in tn.edges.columns if c.endswith('node_id')]
    ]
    connected_node_ids = pd.Series(connected_node_ids.stack().unique())

    tag_node_type_id = tn.id_lookup('node_types', 'tag')

    q = '({})'.format(') & ('.join([
        '~index.isin(@connected_node_ids)',
        'node_type_id != @tag_node_type_id'
    ]))

    unconnected_nodes = tn.nodes.query(q)

    assert unconnected_nodes.empty


def test_bibtex_input_uses_all_types():

    tn = bg.slurp_bibtex(
        "bibliograph/test_data/bibtex_test_data_short.bib",
        entry_syntax_fname="bibliograph/resources/default_bibtex_syntax.csv",
        syntax_case_sensitive=False,
        allow_redundant_items=True,
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
    )

    unused_link_types = tn.link_types.query(
        "~index.isin(@tn.assertions['link_type_id'])"
    )
    unused_node_types = tn.node_types.query(
        "~index.isin(@tn.nodes['node_type_id'])"
    )

    assert unused_link_types.empty and unused_node_types.empty


def test_manual_annotation_input_uses_all_strings():

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

    asserted_string_ids = tn.assertions[
        [c for c in tn.assertions.columns if c.endswith('string_id')]
    ]
    asserted_string_ids = pd.Series(asserted_string_ids.stack().unique())
    assertion_tag_string_ids = pd.Series(
        tn.assertion_tags['tag_string_id'].unique()
    )
    used_string_ids = pd.concat([
        asserted_string_ids,
        assertion_tag_string_ids
    ])
    unused_strings = tn.strings.query('~index.isin(@used_string_ids)')

    assert unused_strings.empty


def test_manual_annotation_input_connects_all_non_tag_nodes():

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

    connected_node_ids = tn.edges[
        [c for c in tn.edges.columns if c.endswith('node_id')]
    ]
    connected_node_ids = pd.Series(connected_node_ids.stack().unique())

    tag_node_type_id = tn.id_lookup('node_types', 'tag')

    q = '({})'.format(') & ('.join([
        '~index.isin(@connected_node_ids)',
        'node_type_id != @tag_node_type_id'
    ]))

    unconnected_nodes = tn.nodes.query(q)

    assert unconnected_nodes.empty


def test_manual_annotation_input_uses_all_types():

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

    unused_link_types = tn.link_types.query(
        "~index.isin(@tn.assertions['link_type_id'])"
    )
    unused_node_types = tn.node_types.query(
        "~index.isin(@tn.nodes['node_type_id'])"
    )

    assert unused_link_types.empty and unused_node_types.empty


def test_auto_aliasing_input_uses_all_strings():

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

    asserted_string_ids = tn.assertions[
        [c for c in tn.assertions.columns if c.endswith('string_id')]
    ]
    asserted_string_ids = pd.Series(asserted_string_ids.stack().unique())
    assertion_tag_string_ids = pd.Series(
        tn.assertion_tags['tag_string_id'].unique()
    )
    used_string_ids = pd.concat([
        asserted_string_ids,
        assertion_tag_string_ids
    ])
    unused_strings = tn.strings.query('~index.isin(@used_string_ids)')

    assert unused_strings.empty


def test_auto_aliasing_input_connects_all_non_tag_nodes():

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

    connected_node_ids = tn.edges[
        [c for c in tn.edges.columns if c.endswith('node_id')]
    ]
    connected_node_ids = pd.Series(connected_node_ids.stack().unique())

    tag_node_type_id = tn.id_lookup('node_types', 'tag')

    q = '({})'.format(') & ('.join([
        '~index.isin(@connected_node_ids)',
        'node_type_id != @tag_node_type_id'
    ]))

    unconnected_nodes = tn.nodes.query(q)

    assert unconnected_nodes.empty


def test_auto_aliasing_input_uses_all_types():

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

    unused_link_types = tn.link_types.query(
        "~index.isin(@tn.assertions['link_type_id'])"
    )
    unused_node_types = tn.node_types.query(
        "~index.isin(@tn.nodes['node_type_id'])"
    )

    assert unused_link_types.empty and unused_node_types.empty


def test_shnd_with_aliases_input_uses_all_strings():

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

    asserted_string_ids = tn.assertions[
        [c for c in tn.assertions.columns if c.endswith('string_id')]
    ]
    asserted_string_ids = pd.Series(asserted_string_ids.stack().unique())
    assertion_tag_string_ids = pd.Series(
        tn.assertion_tags['tag_string_id'].unique()
    )
    used_string_ids = pd.concat([
        asserted_string_ids,
        assertion_tag_string_ids
    ])
    unused_strings = tn.strings.query('~index.isin(@used_string_ids)')

    assert unused_strings.empty


def test_shnd_with_aliases_input_connects_all_non_tag_nodes():

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

    connected_node_ids = tn.edges[
        [c for c in tn.edges.columns if c.endswith('node_id')]
    ]
    connected_node_ids = pd.Series(connected_node_ids.stack().unique())

    tag_node_type_id = tn.id_lookup('node_types', 'tag')

    q = '({})'.format(') & ('.join([
        '~index.isin(@connected_node_ids)',
        'node_type_id != @tag_node_type_id'
    ]))

    unconnected_nodes = tn.nodes.query(q)

    assert unconnected_nodes.empty


def test_shnd_with_aliases_input_uses_all_type():

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

    unused_link_types = tn.link_types.query(
        "~index.isin(@tn.assertions['link_type_id'])"
    )
    unused_node_types = tn.node_types.query(
        "~index.isin(@tn.nodes['node_type_id'])"
    )

    assert unused_link_types.empty and unused_node_types.empty


def test_single_column_input_uses_all_strings():

    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    constraints_fname = "bibliograph/resources/default_link_constraints.csv"

    tn = bg.slurp_single_column(
        'bibliograph/test_data/single_column.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        automatic_aliasing=True,
        link_constraints_fname=constraints_fname,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=0,
        comment_char='#'
    )

    asserted_string_ids = tn.assertions[
        [c for c in tn.assertions.columns if c.endswith('string_id')]
    ]
    asserted_string_ids = pd.Series(asserted_string_ids.stack().unique())
    assertion_tag_string_ids = pd.Series(
        tn.assertion_tags['tag_string_id'].unique()
    )
    used_string_ids = pd.concat([
        asserted_string_ids,
        assertion_tag_string_ids
    ])
    unused_strings = tn.strings.query('~index.isin(@used_string_ids)')

    assert unused_strings.empty


def test_single_column_input_connects_all_non_tag_nodes():

    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    constraints_fname = "bibliograph/resources/default_link_constraints.csv"

    tn = bg.slurp_single_column(
        'bibliograph/test_data/single_column.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        automatic_aliasing=True,
        link_constraints_fname=constraints_fname,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=0,
        comment_char='#'
    )

    connected_node_ids = tn.edges[
        [c for c in tn.edges.columns if c.endswith('node_id')]
    ]
    connected_node_ids = pd.Series(connected_node_ids.stack().unique())

    tag_node_type_id = tn.id_lookup('node_types', 'tag')

    q = '({})'.format(') & ('.join([
        '~index.isin(@connected_node_ids)',
        'node_type_id != @tag_node_type_id'
    ]))

    unconnected_nodes = tn.nodes.query(q)

    assert unconnected_nodes.empty


def test_single_column_input_uses_all_types():

    aliases_dict = {
        'actor': 'bibliograph/test_data/aliases_actor.csv',
        'work': 'bibliograph/test_data/aliases_work.csv'
    }

    constraints_fname = "bibliograph/resources/default_link_constraints.csv"

    tn = bg.slurp_single_column(
        'bibliograph/test_data/single_column.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        syntax_case_sensitive=False,
        aliases_dict=aliases_dict,
        aliases_case_sensitive=False,
        automatic_aliasing=True,
        link_constraints_fname=constraints_fname,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values=['!', 'x'],
        na_node_type='missing',
        skiprows=0,
        comment_char='#'
    )

    unused_link_types = tn.link_types.query(
        "~index.isin(@tn.assertions['link_type_id'])"
    )
    unused_node_types = tn.node_types.query(
        "~index.isin(@tn.nodes['node_type_id'])"
    )

    assert unused_link_types.empty and unused_node_types.empty


def test_bibliograph_to_shorthand_conversion():

    tn = bg.slurp_bibtex(
        "bibliograph/test_data/bibtex_test_data_short.bib",
        entry_syntax_fname="bibliograph/resources/default_bibtex_syntax.csv",
        allow_redundant_items=True,
        syntax_case_sensitive=False,
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
    )

    synthesized = tn.synthesize_shorthand_entries(
        entry_syntax="bibliograph/resources/default_entry_syntax.csv",
        node_type='work',
        fill_spaces=True,
        item_separator='__',
        comment_char='#',
        space_char='|',
        hide_default_entry_prefixes=True
    )

    expected_values = [
        'Newkirk,|Gordon|A._Eddy,|John|A.__1962__s_Nature__194__638_641__10.1038/194638b0',
        'Wiin-Nielsen,|A.__1962__s_Monthly|Weather|Review__90__311_323__10.1175/1520-0493(1962)090<0311:OTOKEB>2.0.CO;2',
        'Wiin-Nielsen,|A.__1962__s_Tellus__14__261_280__10.3402/tellusa.v14i3.9551',
        'Lally,|Vincent|E.__1962__s_Bulletin|of|the|American|Meteorological|Society__43__451_453__10.1175/1520-0477-43.9.451',
        'Squires,|P._Turner,|J.|S.__1962__s_Tellus__14__422_434__10.3402/tellusa.v14i4.9569',
        'London,|Julius__1962__s_Archiv|für|Meteorologie,|Geophysik|und|Bioklimatologie,|Serie|B__12__64_77__10.1007/BF02317953',
        'Haurwitz,|B.__1962__s_Archiv|für|Meteorologie,|Geophysik|und|Bioklimatologie,|Serie|A__13__144_166__10.1007/BF02247180',
        'Akasofu,|S.-I._Chapman,|S._Venkatesan,|B.__1963__s_Journal|of|Geophysical|Research__68__3345_3350__10.1029/JZ068i011p03345',
        'Akasofu,|Syun-Ichi_Chapman,|Sydney__1963__s_Journal|of|Geophysical|Research__68__2375_2382__10.1029/JZ068i009p02375',
        'Latham,|J._Mason,|B.|J.__1961__s_Proceedings|of|the|Royal|Society|of|London.|A.|Mathematical|and|Physical|Sciences__260__537_549__!',
        'Smagorinsky,|Joseph__1965__s_Proceedings|of|the|{IBM}|scientific|computing|symposium|on|large-scale|problems|in|physics:|{December}|9-11,|1963__!__141_144__!'

    ]

    assert (synthesized == expected_values).all()


def test_columnar_get_nodes_having_doi_as_supertitle():

    items = pd.DataFrame({
        'author': ['smitha and jonesb', pd.NA],
        'year': ['1979', pd.NA],
        'journal': ['Nature', 'Nature'],
        'volume': [100, 100],
        'pages': [4245, 4245],
        'doi': ['xxx', 'xxx'],
        'bibcode': [pd.NA, 'CODE'],
        'issue': [pd.NA, '4']
    })

    tn = bg.slurp_columnar_items(
        items,
        entry_syntax_fname="bibliograph/resources/default_bibtex_syntax.csv",
        syntax_case_sensitive=False,
        allow_redundant_items=True,
        link_constraints_fname="bibliograph/resources/default_link_constraints.csv",
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
    )

    supertitles = tn.get_nodes_by_edge_link_types(
        has='doi',
        representation='supertitle'
    )

    assert (supertitles == ['Nature']).all()


def test_manual_annotation_get_nodes_equal_get_assertions():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        "bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        aliases_case_sensitive=False,
        item_separator='__',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        default_entry_prefix='wrk',
        skiprows=2,
        comment_char='#',
    )

    assertion_strings = tn.get_strings_by_assertion_link_types(has='cited')
    assertion_strings = assertion_strings.sort_values().array

    node_strings = tn.get_nodes_by_edge_link_types(has='cited')
    node_strings = node_strings.sort_values().array

    assert (assertion_strings == node_strings).all()


def test_auto_aliasing_get_nodes_equal_get_assertions():

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

    assertion_strings = tn.get_strings_by_assertion_link_types(
        src_of='volume'
    )
    assertion_strings = assertion_strings.sort_values().array

    node_strings = tn.get_nodes_by_edge_link_types(
        src_of='volume',
        representation='all'
    )
    node_strings = node_strings.sort_values().array

    assert (assertion_strings == node_strings).all()
