import pandas as pd
import shorthand as shnd


class AssertionsNotFoundError(AttributeError):
    pass


class NodesNotFoundError(AttributeError):
    pass


class TextNet():

    def __init__(
        self,
        assertions=None,
        strings=None,
        nodes=None,
        edges=None,
        node_types=None,
        link_types=None,
        assertion_tags=None,
        edge_tags=None,
        node_metadata_tables=None,
        big_id_dtype=pd.Int32Dtype(),
        small_id_dtype=pd.Int8Dtype()
    ):
        self.assertions = assertions
        self.strings = strings
        self.nodes = nodes
        self.edges = edges
        self.node_types = node_types
        self.link_types = link_types
        self.assertion_tags = assertion_tags
        self.edge_tags = edge_tags
        node_metadata_tables = node_metadata_tables
        self.big_id_dtype = big_id_dtype
        self.small_id_dtype = small_id_dtype

        self._string_side_tables = [
            'strings', 'assertions', 'link_types', 'assertion_tags'
        ]
        self._node_side_tables = [
            'nodes', 'edges', 'node_types', 'edge_tags'
        ]

        if node_metadata_tables is None:
            self.node_metadata_tables = {}

        self._assertions_dtypes = {
            'inp_string_id': self.big_id_dtype,
            'src_string_id': self.big_id_dtype,
            'tgt_string_id': self.big_id_dtype,
            'ref_string_id': self.big_id_dtype,
            'link_type_id': self.small_id_dtype,
            'date_inserted': str,
            'date_modified': str
        }
        self._assertions_index_dtype = self.big_id_dtype

        self._strings_dtypes = {
            'node_id': self.big_id_dtype,
            'string': str,
            'date_inserted': str,
            'date_modified': str
        }
        self._strings_index_dtype = self.big_id_dtype

        self._nodes_dtypes = {
            'node_type_id': self.small_id_dtype,
            'name_string_id': self.big_id_dtype,
            'abbr_string_id': self.big_id_dtype,
            'date_inserted': str,
            'date_modified': str
        }
        self._nodes_index_dtype = self.big_id_dtype

        self._edges_dtypes = {
            'src_node_id': self.big_id_dtype,
            'tgt_node_id': self.big_id_dtype,
            'ref_node_id': self.big_id_dtype,
            'link_type_id': self.small_id_dtype,
            'date_inserted': str,
            'date_modified': str
        }
        self._edges_index_dtype = self.big_id_dtype

        self._node_types_dtypes = {
            'node_type': str,
            'description': str
        }
        self._node_types_index_dtype = self.small_id_dtype

        self._link_types_dtypes = {
            'link_type': str,
            'description': str
        }
        self._link_types_index_dtype = self.small_id_dtype

        self._assertion_tags_dtypes = {
            'assertion_id': self.big_id_dtype,
            'tag_string_id': self.big_id_dtype
        }
        self._assertion_tags_index_dtypes = self.big_id_dtype

        self._edge_tags_dtypes = {
            'edge_id': self.big_id_dtype,
            'tag_string_id': self.big_id_dtype
        }
        self._edge_tags_index_dtypes = self.big_id_dtype

    def __getattr__(self, attr):

        try:

            return self.__getattribute__(attr)

        except AttributeError as error:

            try:

                return self.node_metadata_tables[attr]

            except KeyError:

                if attr in self._string_side_tables:
                    raise AssertionsNotFoundError(
                        'assertions and strings not initialized for '
                        'this TextNet'
                    )

                elif attr in self._node_side_tables:
                    raise NodesNotFoundError(
                        'nodes and edges not initialized for '
                        'this TextNet'
                    )

                else:
                    raise error

    def _insert_type(self, name, description, node_or_link):

        table_name = node_or_link + '_types'
        column_name = node_or_link + '_type'
        existing_table = self.__getattribute__(table_name)

        # exit code 0 means a new type was created
        exit_code = 0

        if name in existing_table[column_name].array:

            existing_description = existing_table.loc[
                existing_table[column_name] == description,
                'description'
            ]

            if pd.notna(description):

                if existing_description != description:
                    # exit code 1 means the type and description already
                    # existed and the existing description was
                    # overwritten
                    exit_code = 1

                else:
                    # exit code 2 means the type and description already
                    # existed and nothing was changed
                    return 2

            else:
                # exit code 3 means the type already existed and the
                # caller passed a null description, but the existing
                # entry had a description that was retained
                return 3

        new_row = {column_name: name, 'description': description}
        new_row = shnd.util.normalize_types(new_row, existing_table)

        self.__setattr__(table_name, pd.concat([existing_table, new_row]))

        return exit_code

    def _reset_table_dtypes(self, table_name):
        table_dtypes = self.__getattr__('_{}_dtypes'.format(table_name))
        index_dtype = self.__getattr__('_{}_index_dtype'.format(table_name))

        table = self.__getattr__(table_name)

        table = table.astype(table_dtypes)
        table.index = table.index.astype(index_dtype)
        table = table[table_dtypes.keys()]
        table = table.fillna(pd.NA)

        self.__setattr__(table_name, table)

    def insert_link_type(self, name, description=pd.NA):
        return self._insert_type(name, description, 'link')

    def insert_node_type(self, name, description=pd.NA):
        return self._insert_type(name, description, 'node')

    def insert_metadata_table(self, node_type, metadata):

        node_type_id = self.id_lookup(
            'node_types',
            node_type,
            column_label='node_type'
        )
        node_id = self.nodes.query('node_type_id == @node_type_id').index

        if len(node_id) > 1:
            raise NotImplementedError(
                "Can't yet handle metadata for multiple nodes of the "
                "same type"
            )

        metadata_table = {'node_id': node_id, 'node_type_id': node_type_id}
        metadata_table.update(metadata)

        metadata_table = {
            k: (v if v is not None else pd.NA)
            for k, v in metadata_table.items()
        }

        self.node_metadata_tables[node_type] = pd.DataFrame(
            metadata_table,
            index=pd.Index([0], dtype=self.big_id_dtype)
        )

    def id_lookup(self, attr, string, column_label=None):
        '''
        Take the name of an attribute of ParsedShorthand. If the
        attribute is a pandas Series or DataFrame, return the numerical
        index of the input string within the attribute.

        Parameters
        ----------
        attr : str
            Name of an attribute of ParsedShorthand

        string : str
            String value to retrieve index for

        column_label : str, default 'string'
            Column label to index if ParsedShorthand.attr is a
            DataFrame. Default to 'string' because at the moment
            ParsedShorthand.strings is the only DataFrame available.
        '''

        attribute = self.__getattribute__(attr)

        default_columns = {
            'strings': 'string',
            'node_types': 'node_type',
            'link_types': 'link_type'
        }

        try:
            # If this assertion passes, assume attribute is a Series
            assert attribute.str

            element = attribute.loc[attribute == string]

        except AttributeError:
            # Otherwise assume attribute is a DataFrame
            if column_label is None:

                if attr not in default_columns.keys():
                    raise ValueError(
                        'Must use column_label keyword when indexing {}'
                        .format(attr)
                    )

                else:
                    column_label = default_columns[attr]

            element = attribute.loc[attribute[column_label] == string]

        length = len(element)

        if length == 1:
            return element.index[0]
        elif length == 0:
            raise KeyError(string)
        elif length > 1:
            raise ValueError('{} index not unique!'.format(attr))

    def map_string_id_to_node_type(self, string_id):

        try:
            int(string_id)

            output = self.node_types.loc[
                self.strings.loc[string_id, 'node_type_id'],
                'node_type'
            ]

            return output[0]

        except TypeError:

            try:
                output = string_id.map(self.strings['node_type_id'])
                return output.map(self.node_types['node_type'])

            except KeyError:
                output = string_id.map(self.strings['node_id'])
                output = output.map(self.nodes['node_type_id'])
                return output.map(self.node_types['node_type'])

    def reset_assertions_dtypes(self):
        self._reset_table_dtypes('assertions')

    def reset_strings_dtypes(self):
        self._reset_table_dtypes('strings')

    def reset_nodes_dtypes(self):
        self._reset_table_dtypes('nodes')

    def reset_edges_dtypes(self):
        self._reset_table_dtypes('edges')

    def reset_node_types_dtypes(self):
        self._reset_table_dtypes('node_types')

    def reset_link_types_dtypes(self):
        self._reset_table_dtypes('link_types')

    def reset_assertion_tags_dtypes(self):
        self._reset_table_dtypes('assertion_tags')

    def reset_edge_tags_dtypes(self):
        self._reset_table_dtypes('edge_tags')

    def resolve_assertions(self, node_types=True, tags=True):
        '''
        Get a copy of the assertions frame with all integer ID elements
        replaced by the string values they represent

        Parameters
        ----------
        node_types : bool, default True
            If True, add columns for the node types of the source,
            target, and reference strings (three additional columns).

        tags : bool, default True
            If True, add a column for assertion tags. All tags for a
            link are joined into a single string separated by spaces.

        Returns
        -------
        pandas.DataFrame
        '''

        assertions = self.assertions
        string_map = self.strings['string']
        lt_map = self.link_types['link_type']

        resolved = pd.DataFrame(
            {'inp_string': assertions['inp_string_id'].map(string_map),
             'src_string': assertions['src_string_id'].map(string_map),
             'tgt_string': assertions['tgt_string_id'].map(string_map),
             'ref_string': assertions['ref_string_id'].map(string_map),
             'link_type': assertions['link_type_id'].map(lt_map),
             'date_inserted': assertions['date_inserted'],
             'date_modified': assertions['date_modified']},
            index=assertions.index
        )

        if node_types:
            resolved['inp_node_type'] = self.map_string_id_to_node_type(
                assertions['inp_string_id']
            )
            resolved['src_node_type'] = self.map_string_id_to_node_type(
                assertions['src_string_id']
            )
            resolved['tgt_node_type'] = self.map_string_id_to_node_type(
                assertions['tgt_string_id']
            )
            resolved['ref_node_type'] = self.map_string_id_to_node_type(
                assertions['ref_string_id']
            )

        if tags is True:
            # Resolve link tags as space-delimited lists
            tags = self.assertion_tags.groupby('assertion_id')
            tags = tags.apply(
                lambda x:
                ' '.join(self.strings.loc[x['tag_string_id'], 'string'])
            )

            resolved = resolved.join(tags.rename('tags'))

        return resolved.fillna(pd.NA)

    def resolve_edges(self, node_types=True, tags=True):
        '''
        Get a copy of the edges frame with all integer ID elements
        replaced by the string values they represent

        Parameters
        ----------
        node_types : bool, default True
            If True, add columns for the node types of the source,
            target, and reference strings (three additional columns).

        tags : bool, default True
            If True, add a column for assertion tags. All tags for a
            link are joined into a single string separated by spaces.

        Returns
        -------
        pandas.DataFrame
        '''

        edges = self.edges
        string_map = self.strings['string']
        lt_map = self.link_types['link_type']

        resolved = pd.DataFrame(
            {'src_string': edges['src_string_id'].map(string_map),
             'tgt_string': edges['tgt_string_id'].map(string_map),
             'ref_string': edges['ref_string_id'].map(string_map),
             'link_type': edges['link_type_id'].map(lt_map),
             'date_inserted': edges['date_inserted'],
             'date_modified': edges['date_modified']},
            index=edges.index
        )

        if node_types:
            resolved['src_node_type'] = self.map_string_id_to_node_type(
                edges['src_string_id']
            )
            resolved['tgt_node_type'] = self.map_string_id_to_node_type(
                edges['tgt_string_id']
            )
            resolved['ref_node_type'] = self.map_string_id_to_node_type(
                edges['ref_string_id']
            )

        if tags is True:
            # Resolve link tags as space-delimited lists
            tags = self.edge_tags.groupby('edge_id')
            tags = tags.apply(
                lambda x:
                ' '.join(self.strings.loc[x['tag_string_id'], 'string'])
            )

            resolved = resolved.join(tags.rename('tags'))

        return resolved.fillna(pd.NA)

    def resolve_nodes(self):
        '''
        Get a copy of the nodes frame with all integer ID elements
        replaced by the string values they represent

        Returns
        -------
        pandas.DataFrame
            Same shape and index as the strings frame.
        '''

        nodes = self.nodes
        node_types = nodes['node_type_id'].map(self.node_types['node_type'])
        name_strings = nodes['name_string_id'].map(self.strings['string'])
        abbr_strings = nodes['abbr_string_id'].map(self.strings['string'])

        resolved = pd.DataFrame(
            {'node_type': node_types,
             'name_string': name_strings,
             'abbr_string': abbr_strings,
             'date_inserted': nodes['date_inserted'],
             'date_modified': nodes['date_modified']},
            index=nodes.index
        )

        return resolved.fillna(pd.NA)

    def resolve_strings(self):
        '''
        Get a copy of the strings frame with all integer ID elements
        replaced by the string values they represent

        Returns
        -------
        pandas.DataFrame
            Same shape and index as the strings frame.
        '''

        resolved = self.strings.copy()

        try:

            resolved['node_type'] = resolved['node_id'].map(
                self.nodes['node_type_id']
            )
            resolved['node_type'] = resolved['node_id'].map(
                self.node_types['node_type']
            )
            return resolved[[
                'node_id',
                'string',
                'node_type',
                'date_inserted',
                'date_modified'
            ]]

        except AttributeError:

            resolved['node_type_id'] = self.strings['node_type_id'].map(
                self.node_types['node_type']
            )
            return resolved.rename(columns={'node_type_id': 'node_type'})
