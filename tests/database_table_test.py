#!/usr/bin/env python3

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

'''Describes tests for the database.database_table module, concerning DatabaseTable objects
and their manipulation'''
import unittest
from unittest.mock import patch, MagicMock, call
import string
import json
from random import choice, randint
import sqlalchemy
from sqlalchemy import MetaData, text, Column, Integer, PrimaryKeyConstraint

import database.base as base
import database.database_table as database_table
import database.protocol as protocol

# Disable no-member warnings to silence false positives from Table instances dinamically generated
# attributes. Disabled warning for access to protected member
# pylint: disable=no-member,W0212
# You'll also need python 3.6+


class MainModuleTest(unittest.TestCase):
    '''Test cases for database.database_table module'''
    def test_gen_source_table(self):
        '''Test source table object generation'''
        meta = MetaData()
        mocked_engine = MagicMock(sqlalchemy.engine.base.Engine)
        meta.bind = mocked_engine

        source_table = database_table.gen_source_table(meta)

        self.assertIn('id', source_table.columns)
        self.assertIn('table_name', source_table.columns)
        self.assertIn('source', source_table.columns)

        mocked_engine.connect.assert_not_called()
        mocked_engine.execute.assert_not_called()

    @patch('sqlalchemy.engine.base.Engine')
    def test_gen_mapping_table(self, mock_engine):
        '''Test mapping table object generation'''
        meta = MetaData()
        meta.bind = sqlalchemy.engine.base.Engine(None, None, None)

        table = MagicMock(sqlalchemy.Table)
        table.name = 'test'
        table.metadata = meta

        mapping_table = database_table.gen_mapping_table(table)

        self.assertIn('id', mapping_table.columns)
        self.assertIn('target_name', mapping_table.columns)
        self.assertIn('name', mapping_table.columns)
        self.assertIn('type', mapping_table.columns)

        mock_engine.connect.assert_not_called()
        mock_engine.execute.assert_not_called()

    def test_get_primary_keys(self):
        '''Verifies if get_primary_keys returns the table's primary keys '''
        meta = MetaData()
        column1 = MagicMock(sqlalchemy.Column)
        column2 = MagicMock(sqlalchemy.Column)
        table = sqlalchemy.Table('test', meta, column1, column2)
        table.primary_key = MagicMock(sqlalchemy.PrimaryKeyConstraint)

        table.primary_key.columns.items = lambda: [(None, column1), (None, column2)]

        primary_keys = database_table.get_primary_keys(table)

        self.assertTrue(column1 in primary_keys)
        self.assertTrue(column2 in primary_keys)

def gen_random_string(min_length, max_length):
    '''Generates a random string to use as name for some feature'''
    string_size = randint(min_length, max_length)
    output = [choice(string.ascii_lowercase) for _ in range(string_size)]

    return ''.join(output)


class DatabaseTableTest(unittest.TestCase):
    '''Test case for DatabaseTable class'''
    def setUp(self):
        '''Declares engine, metadata and DatabaseTable instances to use in testing. Engine
        is a mockup'''
        self.engine = MagicMock(sqlalchemy.engine.base.Engine)
        self.meta = MetaData(bind=self.engine)

        self.name = gen_random_string(5, 15)

        self.table = database_table.DatabaseTable(self.name, self.meta)

    def test_table_creation(self):
        '''Tests the instantiation of a table'''
        table = database_table.DatabaseTable(self.name, self.meta)

        self.assertEqual(table.name, self.name)
        self.assertIs(table.metadata, self.meta)
        self.assertTrue(hasattr(table, '_mapping_table'))

    @patch('database.database_table.inspect')
    def test_map_from_database(self, mocked_inspect):
        '''Tests the map from database method considering a non empty, existing table'''
        self.table.columns = MagicMock(sqlalchemy.util.langhelpers.memoized_property)
        self.table.columns.keys = MagicMock(dict.keys)
        self.table.columns.keys.return_value = True
        self.table.append_column = MagicMock(sqlalchemy.Table.append_column)

        self.table.map_from_database()

        mocked_inspect.assert_not_called()
        self.table.append_column.assert_not_called()

    def test_map_from_database_empty(self):
        '''Needs some love'''
        pass

    @patch('database.database_table.open')
    def test_get_definitions(self, mocked_open):
        '''Tests the definitions hadling'''
        mocked_open.return_value = mocked_file = MagicMock()

        # Generate randomized test dict
        test_dict = {}
        for _ in range(randint(2, 5)):
            name = gen_random_string(5, 15)
            value = gen_random_string(5, 15)
            test_dict[name] = value

        # Mocked definitions file contains json version of test dict
        mocked_file.read.return_value = json.dumps(test_dict)

        definitions = self.table.get_definitions()

        self.assertEqual(definitions, test_dict)


    @patch('database.database_table.insert')
    def test_create_mapping_table(self, mocked_insert):
        '''Tests the creation of a mapping table'''
        with self.assertRaises(base.MissingProtocolError):
            self.table.create_mapping_table()

        self.table._protocol = MagicMock(protocol.Protocol())
        column_names = [gen_random_string(5, 10) for _ in range(randint(2, 5))]
        column_types = [gen_random_string(5, 10) for _ in range(len(column_names))]
        columns = [MagicMock(sqlalchemy.Column()) for _ in range(len(column_names))]
        for i, (field_name, field_type) in enumerate(zip(column_names, column_types)):
            columns[i].name = field_name
            columns[i].type = field_type

        self.table.columns = MagicMock(self.table.columns)
        self.table.columns.items.return_value = iter(columns)

        self.table._protocol.target_from_dbcolumn.return_value = gen_random_string(2, 5)

        self.table._mapping_table = MagicMock(sqlalchemy.Table)
        self.table._mapping_table.name = gen_random_string(2, 5)
        self.table._mapping_table.exists.return_value = True
        self.table.create_mapping_table()
        self.table._mapping_table.create.assert_not_called()

        self.engine.connect.assert_called_once()
        mocked_insert.assert_called_with(self.table._mapping_table)

        self.table._mapping_table.exists.return_value = False
        self.table.create_mapping_table()
        self.table._mapping_table.create.assert_called()

    @patch('database.database_table.insert')
    @patch('database.database_table.update')
    @patch('database.database_table.gen_source_table')
    @patch('database.database_table.select')
    def test_branch_set_source(self, mocked_select, mocked_gen_source_table,
                               mocked_update, mocked_insert):
        ''' Tests the branch of set_source when table_id = None '''
        self.table.get_definitions = lambda: {'data_source': gen_random_string(3, 10)}
        table = MagicMock(sqlalchemy.Table)
        mocked_gen_source_table.return_value = table
        self.engine.execute().fetchone.return_value = None
        self.table.set_source(bind=self.engine)
        mocked_select.assert_called()
        mocked_insert.assert_called()
        mocked_update.assert_not_called()

    @patch('database.database_table.insert')
    @patch('database.database_table.update')
    @patch('database.database_table.gen_source_table')
    @patch('database.database_table.select')
    def test_set_source(self, mocked_select, mocked_gen_source_table, mocked_update, mocked_insert):
        '''Tests the definition of the data source for a given table'''
        self.table.get_definitions = lambda: {'data_source': gen_random_string(3, 10)}
        table = MagicMock(sqlalchemy.Table)
        mocked_gen_source_table.return_value = table
        table.exists.return_value = True

        self.table.set_source(bind=self.engine)
        table.create.assert_not_called()

        table.exists.return_value = False
        self.table.set_source(bind=self.engine)
        table.create.assert_called_with(bind=self.engine)

        mocked_select.assert_called()

        self.engine.execute().fetchone.return_value = (1,)
        self.table.set_source(bind=self.engine)
        mocked_update.assert_called()
        mocked_insert.assert_not_called()

    def test_create(self):
        '''Tests the creation of the table in the database'''
        self.table.exists = MagicMock(self.table.exists)
        self.table.exists.return_value = True

        self.table.create()

        self.table.exists.return_value = False
        self.table.map_from_protocol = MagicMock(self.table.map_from_protocol)

        self.table.create()

    def test_drop(self):
        '''Tests the dropping of the table from the database'''
        # Must assert arguments
        self.table.exists = MagicMock(self.table.exists)
        self.table.exists.return_value = True

        self.table.drop()

        self.table.exists.return_value = False

        self.table.drop()

    def test_drop_column(self):
        '''Test dropping a given column'''
        # Must assert arguments
        self.table.drop_column(gen_random_string(4, 6))

        self.table._mapping_table.exists = MagicMock(self.table._mapping_table.exists)
        self.table._mapping_table.exists.return_value = True
        self.table.drop_column(gen_random_string(4, 6), gen_random_string(4, 6))

    @patch('database.database_table.Column')
    @patch('database.database_table.get_type')
    def test_add_column_not_mapped(self, mocked_get_type, mocked_column):
        '''Tests adding a column not mapped in the protocol'''
        self.table._mapping_table.exists = MagicMock(self.table._mapping_table.exists)
        self.table._mapping_table.exists.return_value = False

        field_name = gen_random_string(4, 10)
        field_type = gen_random_string(4, 10)
        self.table.add_column(field_name, field_type)

        mocked_get_type.assert_called_with(field_type)

        mocked_column.assert_called()
        self.engine.execute.assert_called()

    @patch('database.database_table.Column')
    @patch('database.database_table.get_type')
    @patch('database.database_table.insert')
    def test_add_column_mapped(self, mocked_insert, mocked_get_type, mocked_column):
        '''Tests adding a column mapped in the protocol'''
        self.table._mapping_table.exists = MagicMock(self.table._mapping_table.exists)
        self.table._mapping_table.exists.return_value = True

        field_name = gen_random_string(4, 10)
        field_type = gen_random_string(4, 10)
        field_target = gen_random_string(4, 10)
        self.table.add_column(field_name, field_type, field_target)

        mocked_get_type.assert_called_with(field_type)
        mocked_insert.assert_called()

        mocked_column.assert_called()
        self.engine.execute.assert_called()

    @patch('database.database_table.get_type')
    @patch('database.database_table.Column')
    @patch('database.database_table.update')
    def test_redefine_column(self, mocked_update, mocked_column, mocked_get_type):
        '''Tests the evocation of the redefine_column method'''
        self.table.add_column = MagicMock(self.table.add_column)
        self.table.drop_column = MagicMock(self.table.drop_column)

        original_name = gen_random_string(2, 5)
        new_name = gen_random_string(2, 5)
        new_type = gen_random_string(2, 5)

        with self.assertRaises(base.DatabaseColumnError):
            self.table.redefine_column(self.engine, original_name, new_name, new_type)

        self.table._mapping_table = MagicMock(sqlalchemy.Table)

        self.table.columns = MagicMock(self.table.columns)
        self.table.columns.keys.return_value = [original_name]

        self.table.redefine_column(self.engine, original_name, new_name, new_type)

        self.table.add_column.assert_called_once()
        self.table.drop_column.assert_called_once()
        self.table.add_column.assert_called_with(new_name, new_type, bind=self.engine)
        self.table.drop_column.assert_called_with(original_name, bind=self.engine)

        self.engine.execute.assert_called_once()
        mocked_update.assert_called_with(self.table._mapping_table)
        mocked_column.assert_called()
        mocked_get_type.assert_called_with(new_type)

    @patch('database.database_table.get_type')
    @patch('database.database_table.Table')
    @patch('database.database_table.Column')
    @patch('database.database_table.insert')
    @patch('database.database_table.select')
    def test_transfer_data(self, mocked_select, mocked_insert,
                           mocked_column, mocked_table, mocked_get_type):
        '''Tests data transfering when a column is changed'''
        self.table.transfer_data(self.engine, [])

        self.engine.execute.assert_not_called()

        self.table.primary_key = MagicMock(self.table.primary_key)
        primary_column = MagicMock(sqlalchemy.Column)
        self.table.primary_key.columns = [primary_column]

        self.table.redefine_column = MagicMock(self.table.redefine_column)
        transfer_list = []
        for _ in range(randint(2, 5)):
            name = gen_random_string(5, 10)
            new_name = gen_random_string(5, 10)
            new_type = gen_random_string(5, 10)

            transfer_list.append({
                'name': name,
                'new_name': new_name,
                'new_type': new_type
            })

        self.table.transfer_data(self.engine, transfer_list)

        mocked_select.assert_called()
        calls = []
        column_calls = []
        for transfer in transfer_list:
            calls.append(call(self.engine, transfer['name'], transfer['new_name'],
                              transfer['new_type']))
            column_calls.append(call(transfer['new_name'], mocked_get_type(transfer['new_type'])))
        self.table.redefine_column.assert_has_calls(calls, any_order=True)
        mocked_column.assert_has_calls(column_calls)

    def test_compare_mapping(self):
        '''Tests the compare_mapping method that compares mapping_table information with
           a mapping protocol'''
        with self.assertRaises(base.MissingProtocolError):
            self.table.compare_mapping()

        p = protocol.Protocol()
        self.table.load_protocol(p)

    def test_insert_from_temporary(self):
        '''Tests insertion in table from a previously created temporary table'''
        pass

    def test_update_from_temporary(self):
        '''Tests updating of given columns from a temporary table'''
        pass

if __name__ == '__main__':
    unittest.main()
