''' Routines related to column dictionary generation.
Names comonly used:
- original columns: columns as they are named in the original database;
- target columns: columns as named internaly in project;
- dbcolumns: columns as named in database.'''
import pandas as pd


standard_columns = {
    'description': 'Novo Rótulo',
    'target_name': 'Var.Lab',
    'standard_name': 'Rot.Padrão',
    'database_name': 'Nome Banco',
    'data_type': 'Tipo de Dado'
}

class Protocol():
    ''' Protocol for table translation'''
    def __init__(self, in_file=None, columns=None):
        self._dataframe = None
        self._remaped = None
        self.columns = standard_columns.copy()
        if in_file:
            self.load_csv(in_file, columns)

    def load_csv(self, in_file, columns=None):
        ''' Loads csv into TableDict '''
        self._dataframe = pd.read_csv(in_file)
        self._dataframe = self._dataframe.fillna('')
        if isinstance(columns, dict):
            for column in columns:
                self.columns[column] = columns[column]
        else:
            columns = standard_columns.copy()
        self._remaped = self._dataframe[columns['target_name']]

    def get_targets(self):
        '''Returns the list of targets from the protocol file'''
        return list(self._remaped)

    def target_from_original(self, name, year):
        '''Gets a target column from an original name and a year
        Input example: **{'name': 'TP_COR_RACA', 'year': '2015'}
        output could look like 'CEBMA015N0' '''
        if self._dataframe is None:
            return None
        indexes = self._dataframe[self._dataframe[year] == name].index.tolist()
        if not indexes:
            return None
        if len(indexes) > 1:
            return None
        return self._remaped[indexes[0]]

    def original_from_target(self, name, year):
        '''Gets original column from target column and a year
        Input example: **{'name': 'CEBMA015N0', 'year': '2015'}
        output could look like 'TP_COR_RACA' '''
        if self._dataframe is None:
            return None
        indexes = self._dataframe[self._remaped == name].index.tolist()
        if not indexes:
            return None
        if len(indexes) > 1:
            return None
        return self._dataframe[year][indexes[0]]

    def target_from_dbcolumn(self, name):
        '''Returns the target corresponding to a given dbcolumn'''
        if self._dataframe is None:
            return None
        indexes = self._dataframe[self._dataframe[standard_columns['database_name']]\
                  == name].index.tolist()
        if not indexes:
            return None
        if len(indexes) > 1:
            return None
        return self._remaped[indexes[0]]


    def dbcolumn_from_target(self, name):
        '''Gets database column from a target column name. Ouput is a list
        with the column name, type and comment contents.
        Input example: **{'name': 'CEBMA015N0'}
        output could look like ['cor_raca_id', 'TINYINT', 'Cor/raça', 'TP_COR_RACA'] '''
        indexes = self._dataframe[self._remaped == name].index.tolist()
        if not indexes or len(indexes) > 1:
            return [None, None, None, None]
        comment = self._dataframe[standard_columns['description']][indexes[0]].strip()
        standard = self._dataframe[standard_columns['standard_name']][indexes[0]].strip()
        column_name = self._dataframe[standard_columns['database_name']][indexes[0]].strip()
        column_type = self._dataframe[standard_columns['data_type']][indexes[0]].strip()
        return [column_name, column_type, comment, standard]

    def remap_from_protocol(self, new_protocol, column_list, reference_year='2015'):
        '''Method to update a mapping protocol from another file'''
        cur_targets = self.get_targets()

        for target in cur_targets:
            original = self.original_from_target(target, reference_year)
            new_target = new_protocol.target_from_original(original, reference_year)

            if new_target and target != new_target:
                print('[' + target + ']', '[' + new_target + ']')
                self._dataframe[self._dataframe[self.columns['target_name']] ==\
                                target][self.columns['target_name']] = new_target
                self._remaped[self._remaped == target] = new_target


        new_targets = new_protocol.get_targets()

        # Exclude unused targets
        to_exclude = [t for t in cur_targets if t not in new_targets]
        for target in to_exclude:
            indexes = self._dataframe[self._remaped == target].index.tolist()
            self._dataframe = self._dataframe.drop(indexes)
            self._dataframe = self._dataframe.reset_index(drop=True)
            self._remaped = self._remaped.drop(indexes)
            self._remaped = self._remaped.reset_index(drop=True)

        self._dataframe.index = self._remaped
        new_protocol._dataframe.index = new_protocol._remaped

        new_targets = [c for c in list(new_protocol._remaped) if c not in cur_targets]

        new_rows = new_protocol._dataframe.loc[new_targets]
        self._dataframe = pd.concat([self._dataframe, new_rows])

        for column in column_list:
            self._dataframe[column] = new_protocol._dataframe[column]
