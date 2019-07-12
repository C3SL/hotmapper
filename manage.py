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

'''CLI for database module'''
from manager import Manager
import subprocess
import database.actions
from settings import SCRIPTS_FOLDER

manager = Manager()

@manager.command
def insert(csv_file, table, year, sep=';', null='',notifybackup=None):
    '''Inserts file in table using a year as index'''
    database.actions.insert(csv_file, table, year, delimiters=[sep, '\\n', '"'], null=null)
    if notifybackup:
        database.actions.generate_backup()
@manager.command
def create(table, ignore_definitions=False):
    '''Creates table using mapping protocols
    If ignore_definitions is set, it will ignore the columns from table definition if both, table_definitions and
    mapping_protocol, exists (though it will still get primary_key, foreign_key and source information)'''
    database.actions.create(table, ignore_definitions)

@manager.command
def drop(table):
    '''Drops a table'''
    database.actions.drop(table)

@manager.command
def remap(table, auto_confirmation=False, verify_definitions=False):
    '''Restructures a table to match the mapping protocol.
    If auto_confirmation is set it will not ask before doing any operation
    If verify_definitions is set it will ask any difference between mapping_protocol and table_definition'''
    database.actions.remap(table, auto_confirmation, verify_definitions)

@manager.command
def update_from_file(csv_file, table, year, columns=None, target_list=None, offset=2, sep=';',
                     null=''):
    if columns:
        columns = columns.split(',')
    if target_list:
        target_list = target_list.split(',')
    database.actions.update_from_file(csv_file, table, year, columns=columns,
                                      offset=offset,
                                      delimiters=[sep, '\\n', '"'], null=null)

@manager.command
def csv_from_tabbed(table_name, input_file, output_file, year, sep=';'):
    database.actions.csv_from_tabbed(table_name, input_file, output_file, year, sep=';')

@manager.command
def update_denormalized(table_name, year):
    database.actions.update_denormalized(table_name, year)

@manager.command
def run_aggregations(table_name, year):
    database.actions.run_aggregations(table_name, year)

@manager.command
def generate_backup():
    '''Create/Recriate file monitored by backup script in production'''
    database.actions.generate_backup()

@manager.command
def execute_sql_group(script_group, script_path=SCRIPTS_FOLDER, files=False):
    '''Execute a group of sql files from groups.py,
    if you want only specific files use --files and a "file1,file2,..." pattern'''
    database.actions.execute_sql_group(script_group, script_path, files)

@manager.command
def drop_group(script_group, files=False):
    '''Drop a group of tables from groups.py,
    if you want to drop only specif tables use --files and a "table1,table2,..." pattern'''
    database.actions.drop_group(script_group, files)

@manager.command
def rebuild_group(script_group, sql_path=SCRIPTS_FOLDER, files=False):
    database.actions.drop_group(script_group, files)
    database.actions.execute_sql_group(script_group, sql_path, files)

@manager.command
def run_script(script_name, args="", folder=SCRIPTS_FOLDER):
    '''Run a script from the scripts folder, the arguments of the script needs to be passed as a string'''
    run_list = args.split(",")
    run_list.insert(0, script_name)
    if script_name[-2:] == 'py':
        run_list.insert(0, 'python')
        subprocess.run(run_list, cwd=folder)
    elif script_name[-2:] == 'sh':
        run_list.insert(0, 'sh')
        subprocess.run(run_list, cwd=folder)
    elif script_name[-3:] == 'sql':
        database.actions.execute_sql_script(script_name)

if __name__ == "__main__":
    manager.main()
