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
