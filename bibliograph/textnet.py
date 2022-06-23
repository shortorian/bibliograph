from ast import literal_eval
import bibliograph as bg
import pandas as pd


class TextNet:
    '''
    object representing a network of texts
    '''

    def __init__(
        self,
        schema_script_fname=None,
        label_map_fname=None,
        node_types_fname=None,
        link_types_fname=None,
        tags_fname=None,
        md_sep=None
    ):

        # load basic schema
        if schema_script_fname is None:
            self.schema = bg._default_schema
        else:
            self.schema = bg.pandas_tables_from_sql_script(schema_script_fname)
            _check_schema_cols(self.schema, schema_script_fname, 'assertions')

        # insert basic schema into the network
        for name, table in self.schema.items():
            self.__setattr__(name, table)

        # load schema for node types and insert it into the network
        if node_types_fname is None:
            node_schema = self._make_node_type_tables(
                bg._default_node_types_fname
            )
        else:
            node_schema = self._make_node_type_tables(node_types_fname)

        self.schema.update(node_schema)
        for name, table in self.schema.items():
            table.name = name
            self.__setattr__(name, table)

        # load label map
        if label_map_fname is None:
            self.label_map = bg._default_label_map
        else:
            self.label_map = bg.read_label_map(label_map_fname)

        # check that the label map only has one value per row in columns
        # ['link_type','metadata_column']
        has_link_type = self.label_map['link_type'].notna()
        has_md_column = self.label_map['metadata_column'].notna()
        if not (has_link_type ^ has_md_column).all():
            raise ValueError(
                'for every row of the label map, there must be a '
                'value in column "link_type" or a value in column '
                '"metadata_column", but not both.'
            )

        # load edge type data
        if link_types_fname is None:
            self._read_link_types(bg._default_link_types_fname)
        else:
            self._read_link_types(link_types_fname)

        # load tags
        if tags_fname is not None:
            self._read_node_tags(tags_fname)
        elif bg._default_tags_fname is not False:
            self._read_node_tags(bg._default_tags_fname)

        # characters to separate lists of metadata values
        if md_sep is None:
            self.md_sep = '__'
        else:
            self.md_sep = md_sep

        # map from ID column labels and name column labels to table names
        self._map_to_table_names = pd.Series({
            'source': 'sources',
            'source_id': 'sources',
            'assertion_id': 'assertions',
            'string': 'strings',
            'string_id': 'strings',
            'src_string_id': 'strings',
            'tgt_string_id': 'strings',
            'name_string_id': 'strings',
            'abbr_string_id': 'strings',
            'node_id': 'nodes',
            'node_type_id': 'node_types',
            'node_type': 'node_types',
            'edge_id': 'edges',
            'src_node_id': 'edges',
            'tgt_node_id': 'edges',
            'link_type': 'link_types',
            'link_type_id': 'link_types',
            'tag_id': 'tags',
            'tag': 'tags',
            'comment_id': 'comments',
            'comment': 'comments'
        })

        # integrity check: are all node_type values attributes of the
        # TextNet?
        for n_type in self.node_types['node_type']:
            try:
                self.__getattribute__(n_type)
            except AttributeError:
                raise RuntimeError(
                    'TextNet object failed integrity check. "{}" is '
                    'listed in node_types["node_type"] but it is not '
                    'an attribute of this TextNet object.'.format(n_type)
                )
        # integrity check: are all self._map_to_table_names values
        # attributes of the TextNet?
        for name in set(self._map_to_table_names.array):
            try:
                self.__getattribute__(name)
            except AttributeError:
                raise RuntimeError(
                    'TextNet object failed integrity check. Required '
                    'attribute "{}" not found.'.format(name)
                )

    def _make_node_type_tables(self, filename):

        with open(filename) as f:
            literal = f.read().split('\n###\n')[-1]
            table_data = literal_eval(literal)

        if len(table_data) == 0:
            raise ValueError(
                'Found an object of length zero in file {}'.format(filename)
            )

        table_names = list(table_data.keys())

        node_type_ids = range(len(table_names))

        type_comments = [v['comment'] for v in table_data.values()]

        node_types = pd.DataFrame(
            {'node_type_id': node_type_ids,
             'comment_id': node_type_ids,
             'node_type': table_names,
             'comment': type_comments},
            index=node_type_ids
        )

        node_types, comments = bg.extract_comments(
            node_types,
            extract_from_schema='node_types',
            template_tn=self,
            insert_columns=True
        )

        table_data = {
            key: {k: v for k, v in val.items() if k != 'comment'}
            for key, val in table_data.items()
        }

        for name, cols in table_data.items():
            not_list_like = any([not bg.iterable_not_string(v)
                                for v in cols.values()])
            not_two_valued = any([len(v) != 2 for v in cols.values()])
            if not_list_like or not_two_valued:
                raise ValueError(
                    'Node type table specifications must be of the '
                    'form name:{{column:[SQL_DTYPE, pandas_dtype]}}. '
                    'Found invalid column specification {}:{} in file '
                    '{}.'.format(name, cols, filename)
                )

        pandas_dtypes = {
            name: {k: v[1] for k, v in cols.items()}
            for name, cols in table_data.items()
        }

        nodes_dtypes = dict(zip(self.nodes.columns, self.nodes.dtypes))
        for k, v in pandas_dtypes.items():
            these_types = {
                'node_id': nodes_dtypes['node_id'],
                'node_type_id': nodes_dtypes['node_type_id']
            }
            these_types.update(v)
            pandas_dtypes[k] = these_types

        tables = {
            name: pd.DataFrame(columns=dtypes.keys()).astype(dtypes)
            for name, dtypes in pandas_dtypes.items()
        }

        tables.update({'node_types': node_types, 'comments': comments})

        return tables

    def _read_link_types(self, filename):

        with open(filename) as f:
            literal = f.read().split('\n###\n')[-1]
            link_types = literal_eval(literal)

        link_type_names = link_types.keys()
        comments = [link_types[name]['comment'] for name in link_type_names]
        directed = [link_types[name]['is_directed']
                    for name in link_type_names]

        if len(link_types) == 1:
            link_types = pd.DataFrame(
                {'comment': comments,
                 'link_type': link_type_names,
                 'is_directed': directed},
                index=[0]
            )
        else:
            link_types = pd.DataFrame({
                'comment': comments,
                'link_type': link_type_names,
                'is_directed': directed
            })

        link_types = link_types.reset_index()
        link_types = link_types.rename(columns={'index': 'link_type_id'})

        link_types, comments = bg.extract_comments(
            link_types,
            extract_from_schema='link_types',
            template_tn=self,
            insert_columns=True
        )

        if not comments.empty:

            # normalize new comment_id
            naive_id = comments['comment_id'].array
            normalized_id = bg.non_intersecting_sequence(
                naive_id,
                self.comments['comment_id']
            )
            comments.loc[:, 'comment_id'] = normalized_id

            # map the normalized comment_id to link_types
            comment_index_map = pd.Series(normalized_id, index=naive_id)
            normalized_id = comment_index_map[link_types['comment_id']].array
            link_types.loc[:, 'comment_id'] = normalized_id

            # insert normalized comments into the network
            self.append_to('comments', comments, ignore_index=True)

        # normalize new link_type_id
        normalized_id = bg.non_intersecting_sequence(
            len(link_types['link_type_id']),
            self.link_types['link_type_id']
        )
        link_types.loc[:, 'link_type_id'] = normalized_id

        # insert normalized link_types into the network
        self.append_to('link_types', link_types, ignore_index=True)

    def _read_node_tags(self, filename):

        with open(filename) as f:
            literal = f.read().split('\n###\n')[-1]
            tags = literal_eval(literal)

        tags = pd.Series(tags).reset_index(name='comment')
        tags.rename(columns={'index': 'tag'}, inplace=True)
        tags.reset_index(inplace=True)
        tags.rename(columns={'index': 'tag_id'}, inplace=True)

        tags, comments = bg.extract_comments(
            tags,
            extract_from_schema='tags',
            template_tn=self,
            insert_columns=True
        )

        if not comments.empty:

            # normalize new comment_id
            naive_id = comments['comment_id'].array
            normalized_id = bg.non_intersecting_sequence(
                naive_id,
                self.comments['comment_id']
            )
            comments.loc[:, 'comment_id'] = normalized_id

            # map the normalized comment_id to tags
            comment_index_map = pd.Series(normalized_id, index=naive_id)
            normalized_id = comment_index_map[tags['comment_id']].array
            tags.loc[:, 'comment_id'] = normalized_id

            # insert normalized comments into the network
            self.append_to('comments', comments, ignore_index=True)

        # normalize new tag_id
        naive_id = tags['tag_id'].array
        normalized_id = bg.non_intersecting_sequence(
            naive_id,
            self.tags['tag_id']
        )
        tags.loc[:, 'tag_id'] = normalized_id

        # insert normalized tags
        self.append_to('tags', tags, ignore_index=True)

    def append_to(self, table_name, data, comment=None, **kwargs):

        existing = self.__getattribute__(table_name)

        if type(data) == pd.Series:

            missing_ignore_index_kw = ('ignore_index' not in kwargs.keys())
            ignore_index_false = (kwargs['ignore_index'] is False)
            if missing_ignore_index_kw or ignore_index_false:
                raise ValueError(
                    'if passing a pandas Series, you must set '
                    'ignore_index=True'
                )

            values = data.array
            columns = data.index.array
            data = pd.DataFrame(dict(zip(columns, values)),
                                columns=columns,
                                index=[0])

        if comment is not None:

            if not existing.columns.str.contains('comment').any():
                raise ValueError(
                    'table {} has no comment_id column. cannot '
                    'process comment {}'.format(table_name, comment)
                )

            try:
                self.get_row_by_value('comments', comment)
            except ValueError as e:
                if 'does not exist' in str(e):
                    comment_id = self.comments['comment_id'].max() + 1
                    new_comment_row = pd.Series({
                        'comment_id': comment_id,
                        'comment': comment,
                        'date_inserted': pd.NA
                    })
                    self.comments = self.comments.append(new_comment_row)
                else:
                    raise

            data['comment_id'] = comment_id

        # if ('date_inserted' not in data.columns) and ('date_inserted' in existing.columns):
        #     data[:, 'date_inserted'] = bg.time_string()

        data = bg.coerce_types(data, self.schema[table_name])
        self.__setattr__(table_name, existing.append(data, **kwargs))

    def get_table_by_label(self, label, search=False):
        try:
            table_name = self._map_to_table_names[label]

        except KeyError:

            if label in self.schema.keys():
                table_name = label

            elif search:
                has_column_label = [
                    name for name, table in self.schema.items()
                    if (label in table.columns)
                ]

                if len(has_column_label) > 1:
                    raise ValueError(
                        'Column label {} found in more than one table.'
                        .format(label)
                    )

                elif len(has_column_label) == 1:
                    table_name = has_column_label[0]

                else:
                    raise ValueError(
                        'TextNet object has no tables with column '
                        'label {}.'.format(label)
                    )

            else:
                raise KeyError(
                    'TextNet object has no table named {0} and no key '
                    '{0} in_map_to_table_names.'.format(label)
                )

        return self.__getattribute__(table_name)

    def get_row_by_value(self, unique_column, unique_value):
        table = self.get_table_by_label(unique_column)
        match = table.loc[table[unique_column] == unique_value, :]

        if len(match) > 1:
            raise ValueError(
                'Value "{}" is not unique in {}["{}"]'
                .format(unique_value, table.name, unique_column)
            )
        elif len(match) == 1:
            return match.squeeze()
        else:
            raise ValueError(
                'Value "{}" does not exist in {}["{}"]'
                .format(unique_value, table.name, unique_column)
            )

    def get_element_by_value(
        self,
        unique_column,
        unique_value,
        desired_column
    ):
        row = self.get_row_by_value(unique_column, unique_value)
        return row[desired_column]

    def translate(self, from_table, from_label, to_label):

        if type(from_table) == str:
            from_table = self.get_table_by_label(from_table)

    def get_value_by_name(
        self,
        table_name,
        row_name,
        column_name=None,
        row=False,
        case_sensitive=False
    ):
        table = self.__getattribute__(table_name)
        name_label = table_name[:-1]

        if name_label not in table.columns:
            raise ValueError(
                'cannot get {0} ID by name because table {0} has no '
                'column labeled "{1}".'.format(table_name, name_label)
            )

        if case_sensitive:
            where_matched = (table[name_label] == row_name)
        else:
            where_matched = (table[name_label].str.lower() == row_name.lower())

        matched_rows = table.loc[where_matched]

        if len(matched_rows) > 1:
            raise ValueError(
                'Name "{}" is not unique in {}["{}"]'
                .format(row_name, table_name, name_label)
            )

        elif len(matched_rows) == 1:

            if column_name is not None:
                if row is not None:
                    raise ValueError(
                        'Provide a column name or set row=True or '
                        'neither, not both.'
                    )

                output = matched_rows.iloc[0][column_name]

            elif row:
                output = matched_rows.iloc[0]

            else:
                output = matched_rows.iloc[0][name_label + '_id']

        else:
            raise NameError(
                'row name "{}" does not exist in {}["{}"]'
                .format(row_name, table_name, name_label)
            )

        return output

    def merge_strings_tables(self, new, update=False):

        if new.empty:
            return

        common_cols = [c for c in self.strings.columns if c in new.columns]
        if len(common_cols) == 0:
            raise ValueError(
                'new strings frame must have columns in common with '
                'existing strings frame'
            )

        new.loc[:, 'date_inserted'] = bg.time_string()

        self.append_to(
            'strings',
            new.loc[~new['string'].isin(self.strings['string']), :]
        )

    def map_aliases(self, aliases, case_sensitive=False):

        has_type = self.strings.merge(
            self.nodes[['node_id', 'node_type_id']],
            on='node_id'
        )
        has_type = has_type.groupby('node_type_id')
        has_type = has_type.apply(bg.map_aliases, aliases, case_sensitive)

        # self.merge_strings_tables(has_type)
        self.strings = has_type[self.strings.columns]

    def merge_and_replace(
        self,
        other,
        left_on,
        right_on,
        replace_with,
        other_is_left=True,
        **kwargs
    ):

        table = self.get_table_by_label(right_on)

        if other_is_left:
            output = other.merge(
                table[[right_on, replace_with]],
                left_on=left_on,
                right_on=right_on,
                **kwargs
            )
        else:
            output = table.merge(
                other[[right_on, replace_with]],
                left_on=left_on,
                right_on=right_on,
                **kwargs
            )

        if left_on == right_on:
            output.drop(left_on, axis=1, inplace=True)
        else:
            output.drop([left_on, right_on], axis=1, inplace=True)

        return output

    def resolve_nodes(self, string_type=None, selection=None, drop=False):

        if string_type == 'name':
            string_type_col = 'name_string_id'
        elif (string_type == 'abbr') or (string_type == 'abbreviation'):
            string_type_col = 'abbr_string_id'
        elif string_type != 'all':
            raise ValueError(
                'Unrecognized node string type {}. Must be "name", '
                '"abbreviation", or "all".'.format(string_type)
            )

        if selection is not None:
            output = self.nodes.loc[selection, :].copy()
        else:
            output = self.nodes.copy()

        output = output.merge(self.node_types[['node_type_id', 'node_type']])
        if drop:
            output.drop('node_type_id', axis=1, inplace=True)

        strings_slctn = self.strings['node_id'].isin(output['node_id'])
        cols_dict = {
            'date_inserted': 'string_date_inserted',
            'date_modified': 'string_date_modified'
        }
        strings = self.strings.loc[strings_slctn].rename(columns=cols_dict)

        if string_type == 'all':
            output = strings.merge(output)
        else:
            strings = strings[
                [c for c in strings.columns if c != 'node_id']
            ]
            output = output.merge(
                strings,
                left_on=string_type_col,
                right_on='string_id'
            )

        if drop:
            return output.loc[:, ['node_id', 'node_type', 'string']]
        else:
            return output

    def resolve_edges(self, string_type=None, selection=None, drop=False):

        if selection is not None:
            output = self.edges.loc[selection, :].copy()
        else:
            output = self.edges.copy()

        output = output.merge(self.link_types[['link_type_id', 'link_type']])
        if drop:
            output.drop('link_type_id', axis=1, inplace=True)

        node_is_relevant = pd.concat(
            [output['src_node_id'], output['tgt_node_id']]
        )
        node_is_relevant.drop_duplicates()
        node_is_relevant = self.nodes['node_id'].isin(node_is_relevant)

        nodes = self.resolve_nodes(
            string_type=string_type,
            selection=node_is_relevant,
            drop=drop
        )
        nodes.rename(
            columns={'comment_id': 'node_comment_id',
                     'date_inserted': 'node_date_inserted',
                     'date_modified': 'node_date_modified'},
            inplace=True
        )

        take_if_drop = ['node_id', 'node_type', 'string']

        if drop:
            these_nodes = nodes[take_if_drop].add_prefix('src_')
            output = output.merge(these_nodes)
            output.drop('src_node_id', axis=1, inplace=True)
        else:
            output = output.merge(nodes.add_prefix('src_'))

        if drop:
            these_nodes = nodes[take_if_drop].add_prefix('tgt_')
            output = output.merge(these_nodes)
            output.drop('tgt_node_id', axis=1, inplace=True)
        else:
            output = output.merge(nodes.add_prefix('tgt_'))

        return output

    def resolve_assertions(self, selection=None, drop=False):

        if selection is not None:
            output = self.assertions.loc[selection, :].copy()
        else:
            output = self.assertions.copy()

        output = output.merge(self.link_types[['link_type_id', 'link_type']])
        is_src = self.strings['string_id'].isin(
            self.assertions['src_string_id']
        )
        src_strings_subset = self.strings.loc[is_src]
        is_tgt = self.strings['string_id'].isin(
            self.assertions['tgt_string_id']
        )
        tgt_strings_subset = self.strings.loc[is_tgt]

        if drop:
            output.drop('link_type_id', axis=1, inplace=True)
            src_strings_subset = src_strings_subset[['string_id', 'string']]
            tgt_strings_subset = tgt_strings_subset[['string_id', 'string']]

        src_strings_subset = src_strings_subset.add_prefix('src_')
        tgt_strings_subset = tgt_strings_subset.add_prefix('tgt_')

        output = output.merge(src_strings_subset)
        output = output.merge(tgt_strings_subset)

        if drop:
            drop_cols = [
                'src_string_id',
                'tgt_string_id',
                'comment_id',
                'date_inserted',
                'date_modified'
            ]
            output.drop(drop_cols, axis=1, inplace=True)

        return output

    def resolve_node_type(
        self,
        node_type,
        string_type,
        selection=None,
        drop=False
    ):

        if node_type not in self.node_types['node_type'].array:
            raise ValueError(
                '{} is not listed in node_types["node_type"]'.format(node_type)
            )

        table = self.__getattribute__(node_type)
        table = table[[lbl for lbl in table.columns if lbl != 'node_type_id']]

        if selection is not None:
            output = table.loc[selection, :].copy()
        else:
            output = table.copy()

        node_is_relevant = self.nodes['node_id'].isin(output['node_id'])

        nodes = self.resolve_nodes(
            string_type=string_type,
            selection=node_is_relevant,
            drop=drop
        )
        print(output.columns)
        print(nodes.columns)
        take_if_drop = ['node_id', 'string']

        if drop:
            return output.merge(nodes[take_if_drop])
        else:
            return output.merge(nodes)

    def resolve_table_comments(self, table, prefix='', drop=False):

        if type(table) == str:
            table = self.__getattribute__(table).copy()

        if table['comment_id'].notna().any():
            table = table.merge(self.comments.add_prefix(prefix))
            if drop:
                label = prefix + 'comment_id'
                table.drop(label, axis=1, inplace=True)


def _check_schema_cols(schema, schema_script, table_name):
    if table_name in schema.keys():
        user_cols = schema[table_name].columns
        default_cols = bg._default_schema.columns
        if not all([lbl in default_cols for lbl in user_cols]):
            raise ValueError(
                'Table {0} defined in {1} is missing required '
                'columns. The {0} table must at least have columns '
                '{2}.'.format(table_name, schema_script, default_cols)
            )
