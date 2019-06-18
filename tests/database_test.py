'''
Copyright (C) 2016 Centro de Computacao Cientifica e Software Livre
Departamento de Informatica - Universidade Federal do Parana - C3SL/UFPR

This file is part of HOTMapper.

HOTMapper is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

HOTMapper is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with HOTMapper.  If not, see <https://www.gnu.org/licenses/>.
'''

'''This module is used when various changes are made to the database and it is needed to test the main actions
Execute it using 'python -m tests.database_test test_all' to ensure correct functionality'''
import sys
from manage import Manager
import settings
import database.actions
import os
from sqlalchemy import create_engine, MetaData, select, Table
import logging
import pandas as pd

ENGINE = create_engine(settings.DATABASE_URI, echo=settings.ECHO)
META = MetaData(bind=ENGINE)

sqlalchemy_logger = logging.getLogger('sqlalchemy.engine')
sqlalchemy_logger.setLevel(logging.ERROR)

table_test = 'test_database'
csvpath = os.path.join(os.getcwd(), 'tests/database_test_data', 'test_database_data.csv')
protocol_path = os.path.join(settings.MAPPING_PROTOCOLS_FOLDER, table_test + '.csv')
mapping_df_original = pd.read_csv(protocol_path, index_col=0)

class VerificationFailed(Exception):
    '''Raised when the verification fails, automatically drops the test table'''
    def __init__(self, *args):
        database.actions.drop("test_database")

def compare_columns(table, verify_csv, error_string):
    with ENGINE.connect():
        verify_columns_df = pd.read_csv(os.path.join(settings.MAPPING_PROTOCOLS_FOLDER, verify_csv),
                                        sep=',', usecols=[4, 5], names=['name', 'type'], header=0)
        verify_columns_name = verify_columns_df['name'].tolist()
        verify_columns_type = ['INTEGER' if (v_type == 'INT') else v_type
                               for v_type in verify_columns_df['type'].tolist()]
        for c in table.columns:
            if c.name not in verify_columns_name:
                raise VerificationFailed('Something went wrong, please rerun in debug mode.', error_string,
                                         c.name, 'not in verify table')
            else:
                if str(c.type) != verify_columns_type[verify_columns_name.index(c.name)]:
                    raise VerificationFailed('Something went wrong, please rerun in debug mode.' + error_string,
                                             c.name, 'has a diferent type in verify table',
                                             str(c.type), verify_columns_df[verify_columns_df['name'] == c.name]['type'])
            print(c.name, c.type)

def compare_data(table, verify_csv, error_string):
    print("Executing fetchall query:")
    with ENGINE.connect() as connection:
        sel = select([table]).order_by(table.c.id)
        result = connection.execute(sel)
        content = result.fetchall()
        print('Initializing data verification:\n')
        verify_table = pd.read_csv(verify_csv, sep=',')
        verify_table = verify_table.replace({pd.np.nan: None})
        verify_content = list(verify_table.itertuples(index=False, name=None))
        if verify_content != content:
            raise VerificationFailed('Something went wrong, please rerun in debug mode. ' + error_string)

def modify_mapping_protocol():
    mapping_df = pd.read_csv(protocol_path, index_col=0)

    mapping_df = mapping_df.drop('TIPOMASS')  # remove tipo_id
    mapping_df.loc['RDREF'] = ['', 'Texto aleatório da test_reference', 0, 'random_string', 'VARCHAR(16)',
                               '~test_reference.random_string']
    mapping_df.loc['CODTIPO'] = ['', 'Código do tipo', 0, 'tipo_id', 'INT',
                                '~CASE WHEN ("detipomass" =' + " 'Natural') THEN 1 ELSE 0 END"]
    mapping_df.at['ESPCD', 'Nome Banco'] = 'esp_id'  # rename massa_id to esp_id
    mapping_df.to_csv(protocol_path)


def test_creation():
    if not ENGINE.dialect.has_table(ENGINE, 'test_reference'):
        database.actions.execute_sql_script('test_reference.sql')
    database.actions.create(table_test)
    print("Executing fetchall query:")
    with ENGINE.connect() as connection:
        table = Table(table_test, META, autoload=True, autoload_with=ENGINE)
        sel = select([table])
        result = connection.execute(sel)
        content = result.fetchall()
        if not content:
            print("Success! table created and is empty")
            print(content)
            print('Columns of', table_test, ':')
            compare_columns(table, 'test_database.csv', 'CREATION VERIFICATION FAILED')
            print('\nCREATION SUCCESS!\n\n')
        else:
            print("Something went wrong. Please rerun in DEBUG mod. CREATION FAILED")

def test_insert():
    print('Testing insert of data', csvpath)
    database.actions.insert(csvpath, table_test, '2018', delimiters=[',', '\\n', '"'], null='')

    table = Table(table_test, META, autoload=True, autoload_with=ENGINE)
    compare_data(table, './tests/database_test_data/verify_data_insert.csv', 'INSERTION VERIFICATION FAILED')
    print('INSERTION SUCCESS!\n\n')

def test_remap_without_changes():
    print('Testing a remap without changes:')

    database.actions.remap(table_test)
    table = Table(table_test, META, autoload=True, autoload_with=ENGINE)
    compare_columns(table, 'test_database.csv', 'REMAP WITHOUT CHANGES FAILED.')
    print('REMAP WITHOUT CHANGES SUCCESS!\n\n')

def test_remap_with_all_changes():
    print('\nTesting a remap with all possible changes:')

    modify_mapping_protocol()
    try:
        database.actions.remap(table_test)
        table = Table(table_test, META, autoload=True, autoload_with=ENGINE)
        compare_columns(table, 'test_database.csv', 'REMAP WITH ALL POSSIBLE CHANGES FAILED.')
    finally:
        mapping_df_original.to_csv(protocol_path)
    print('REMAP WITH ALL POSSIBLE CHANGES CHANGES SUCCESS!\n\n')

def test_run_aggregations():
    print('\nRunning aggregations and denormalizations:')

    modify_mapping_protocol()
    try:
        database.actions.run_aggregations(table_test, '2018')
        table = Table(table_test, META, autoload=True, autoload_with=ENGINE)
        compare_data(table, './tests/database_test_data/verify_data_aggregations.csv',
                     'RUN AGGREGATIONS VERIFICATION FAILED')
    finally:
        mapping_df_original.to_csv(protocol_path)
    print('RUN AGGREGATIONS SUCCESS!\n\n')

def test_update_from_file():
    print('\nTesting an update from file:')

    modify_mapping_protocol()
    try:
        database.actions.update_from_file(csvpath, table_test, '2018', delimiters=[',', '\\n', '"'])
        table = Table(table_test, META, autoload=True, autoload_with=ENGINE)
        compare_data(table, './tests/database_test_data/verify_data_update.csv', 'UPDATE FROM FILE VERIFICATION FAILED')
    finally:
        mapping_df_original.to_csv(protocol_path)

    print('UPDATE FROM FILE SUCCESS!\n\n')

def test_drop():
    print("Dropping table", table_test)
    database.actions.drop(table_test)

    with ENGINE.connect():
        table = Table(table_test, META, autoload=True, autoload_with=ENGINE)
        if not table.exists(bind=None):
            print('TABLE DROP SUCCESS!')
        else:
            print("Something went wrong. Please rerun in DEBUG mod. DROP FAILED")


manager = Manager()
@manager.command()
def test_all():
    test_creation()
    test_insert()
    test_remap_without_changes()
    os.execl(sys.executable, 'python', '-m', 'tests.database_test', 'remap_all')

@manager.command()
def remap_all():
    test_remap_with_all_changes()
    os.execl(sys.executable, 'python', '-m', 'tests.database_test', 'run_aggregations_all')

@manager.command()
def run_aggregations_all():
    test_run_aggregations()
    os.execl(sys.executable, 'python', '-m', 'tests.database_test', 'update_data_all')

@manager.command()
def update_data_all():
    test_update_from_file()
    test_drop()


if __name__ == "__main__":
    manager.main()
