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

    def __getattribute__(self, attr):

        try:

            return super(TextNet, self).__getattribute__(attr)

        except AttributeError as err:

            try:
                return self.node_metadata_tables[attr]

            except KeyError:
                raise err

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