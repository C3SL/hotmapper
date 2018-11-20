#!/usr/bin/env python3
'''CLI for database module'''
from manager import Manager

import database.actions


manager = Manager()

@manager.command
def insert(csv_file, table, year, sep=';', null=''):
    '''Inserts file in table using a year as index'''
    database.actions.insert(csv_file, table, year, sep=sep, null=null)

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
    '''TODO'''
    database.actions.remap(table)

@manager.command
def generate_pairing_report(output='csv'):
    '''In progress'''
    database.actions.generate_pairing_report(output)

@manager.command
def update_from_file(csv_file, table, year, columns=None, target_list=None, offset=2, sep=';',
                     null=''):
    if columns:
        columns = columns.split(',')
    if target_list:
        target_list = target_list.split(',')
    database.actions.update_from_file(csv_file, table, year, columns=columns,
                    target_list=target_list, offset=offset, sep=sep, null=null)

if __name__ == "__main__":
    manager.main()
