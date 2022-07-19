import pandas as pd


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

        [self.__setattr__(k, v) for k, v in locals().items() if k != 'self']

        if node_metadata_tables is None:
            self.node_metadata_tables = {}

    def __getattr__(self, attr):

        try:

            return self.__getattribute__(attr)

        except AttributeError as error:

            try:
                return self.node_metadata_tables[attr]

            except KeyError:
                raise error

    def insert_metadata_table(self, node_type, metadata):

        node_type_id = self.id_lookup(
            'node_types',
            node_type,
            column_label='node_type'
        )
        node_id = self.nodes.query('node_type_id == @node_type_id').index[0]

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

    def insert_link_type

    def id_lookup(self, attr, string, column_label='string'):
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

        try:
            # If this assertion passes, assume attribute is a Series
            assert attribute.str

            element = attribute.loc[attribute == string]

        except AttributeError:
            # Otherwise assume attribute is a DataFrame
            if column_label == 'string' and attr != 'strings':
                raise ValueError(
                    'Must use column_label keyword when indexing a '
                    'DataFrame'
                )

            element = attribute.loc[attribute[column_label] == string]

        length = len(element)

        if length == 1:
            return element.index[0]
        elif length == 0:
            raise KeyError(string)
        elif length > 1:
            raise ValueError('{} index not unique!'.format(attr))
