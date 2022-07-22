import bibliograph as bg


def test_manual_annotation_nodes_column_nan_states():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values='!',
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
        na_string_values='!',
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
        na_string_values='!',
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
        na_string_values='!',
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


def test_manual_annotation_node_type_14_is_file():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    assert (
        tn.id_lookup('node_types', 'file_name', column_label='node_type') == 14
    )


def test_manual_annotation_num_strings_is_65():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    assert len(tn.strings) == 65


def test_manual_annotation_single_string_has_node_type_file_name():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values='!',
        na_node_type='missing',
        skiprows=2,
        comment_char='#'
    )

    file_string = tn.resolve_strings().query('node_type == "file_name"')

    assert (file_string.node_type.squeeze() == "file_name")


def test_manual_annotation_node_type_0_is_shorthand_text():

    tn = bg.slurp_shorthand(
        'bibliograph/test_data/manual_annotation.shnd',
        "bibliograph/resources/default_entry_syntax.csv",
        link_syntax_fname="bibliograph/resources/default_link_syntax.csv",
        syntax_case_sensitive=False,
        item_separator='__',
        default_entry_prefix='wrk',
        space_char='|',
        na_string_values='!',
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
        na_string_values='!',
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
        na_string_values='!',
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
