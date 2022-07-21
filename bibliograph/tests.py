import bibliograph as bg


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

    assert (tn.id_lookup('node_types', 'file_name', column_label='node_type') == 14)


def test_manual_annotation_last_string_index_is_64():

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

    file_string = tn.strings.query('node_type_id == 14')

    assert (file_string.index[0] == 64)


def test_manual_annotation_last_string_node_type_id_is_14():

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

    file_string = tn.strings.query('node_type_id == 14')

    assert (file_string.node_type_id.squeeze() == 14)


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

    shorthand_text_node_type_id = tn.id_lookup(
        'node_types',
        'shorthand_text',
        column_label='node_type'
    )

    assert (shorthand_text_node_type_id == 0)


def test_manual_annotation_shorthand_text_string_id_is_60():

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

    shorthand_text_string = tn.strings.query('node_type_id == 0')

    assert (shorthand_text_string.index[0] == 60)


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

    shorthand_text_string = tn.strings.query('node_type_id == 0')
    assert (shorthand_text_string.string.squeeze().startswith(
        'This is stuff shorthand ignores'
    ))
