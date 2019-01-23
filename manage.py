#!/usr/bin/env python3

"""
Copyright (C) 2018 Centro de Computacao Cientifica e Software Livre
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
along with simcaq-cdn.  If not, see <https://www.gnu.org/licenses/>.

"""



'''CLI for database module'''
from manager import Manager

import database.actions

manager = Manager()

@manager.command
def insert(csv_file, table, year, sep=';', null='',notifybackup=None):
    '''Inserts file in table using a year as index'''
    database.actions.insert(csv_file, table, year, delimiters=[sep, '\\n', '"'], null=null)
    if notifybackup:
        database.actions.generate_backup()
@manager.command
def create(table):
    '''Creates table using mapping protocols'''
    database.actions.create(table)

@manager.command
def drop(table):
    '''Drops a table'''
    database.actions.drop(table)

@manager.command
def remap(table):
    '''Restructures a table to match the mapping protocol.'''
    database.actions.remap(table)

@manager.command
def update_from_file(csv_file, table, year, columns=None, target_list=None, offset=2, sep=';',
                     null=''):
    if columns:
        columns = columns.split(',')
    if target_list:
        target_list = target_list.split(',')
    database.actions.update_from_file(csv_file, table, year, columns=columns,
                                      target_list=target_list, offset=offset,
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

if __name__ == "__main__":
    manager.main()
