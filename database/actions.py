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

'''Database manipulation actions - these can be used as models for other modules.'''
import logging
from sqlalchemy import create_engine, MetaData, text
from os import chdir
from datetime import datetime
from database.base import MissingTableError
from database.database_table import gen_data_table, copy_tabbed_to_csv
import database.groups
import settings
from database.groups import DATA_GROUP, DATABASE_TABLE_NAME

ENGINE = create_engine(settings.DATABASE_URI, echo=settings.ECHO)
META = MetaData(bind=ENGINE)

logging.basicConfig(format = settings.LOGGING_FORMAT)
logger = logging.getLogger(__name__)

database_table_logger = logging.getLogger('database.database_table')
database_table_logger.setLevel(settings.LOGGING_LEVEL)
protocol_logger = logging.getLogger('database.protocol')
protocol_logger.setLevel(settings.LOGGING_LEVEL)
sqlalchemy_logger = logging.getLogger('sqlalchemy.engine')
sqlalchemy_logger.setLevel(settings.LOGGING_LEVEL)

def temporary_data(connection, file_name, table, year, offset=2,
                   delimiters=[';', '\\n', '"'], null=''):
    header = open(file_name, encoding="ISO-8859-9").readline().strip()
    header = header.split(delimiters[0])

    ttable = table.get_temporary(header, year)
    ttable.create(bind=connection)

    table.populate_temporary(ttable, file_name, header, year, delimiters, null, offset, bind=connection)
    table.apply_derivatives(ttable, ttable.columns.keys(), year, bind=connection)

    return ttable

def insert(file_name, table, year, offset=2, delimiters=[';', '\\n', '"'], null='', notifybackup=None):
    '''Inserts contents of csv in file_name in table using year as index for mapping'''
    table = gen_data_table(table, META)
    table.map_from_database()
    if not table.exists():
        raise MissingTableError(table.name)

    with ENGINE.connect() as connection:
        trans = connection.begin()

        ttable = temporary_data(connection, file_name, table, year, offset, delimiters, null)
        table.insert_from_temporary(ttable, bind=connection)

        trans.commit()

def create(table):
    '''Creates table from mapping_protocol metadata'''
    table = gen_data_table(table, META)

    with ENGINE.connect() as connection:
        trans = connection.begin()
        table.create(bind=connection)
        table.set_source(bind=connection)
        table.create_mapping_table(bind=connection)
        trans.commit()

def drop(table):
    '''Drops table'''
    table = gen_data_table(table, META)

    table.drop()

def remap(table):
    '''Applies change made in mapping protocols to database'''
    table = gen_data_table(table, META)
    table.map_from_database()

    table.remap()

def csv_from_tabbed(table_name, input_file, output_file, year, sep=';'):
    table = gen_data_table(table_name, META)

    protocol = table.get_protocol()
    column_names, column_mappings = protocol.get_tabbed_mapping(year)

    copy_tabbed_to_csv(input_file, column_mappings, settings.CHUNK_SIZE, output_file,
                       column_names=column_names, sep=sep)

def update_from_file(file_name, table, year, columns=None,
                     offset=2, delimiters=[';', '\\n', '"'], null=''):
    '''Updates table columns from an input csv file'''
    table = gen_data_table(table, META)
    table.map_from_database()
    if not table.exists():
        raise MissingTableError(table.name)

    if columns is None:
        columns = []

    with ENGINE.connect() as connection:
        trans = connection.begin()

        ttable = temporary_data(connection, file_name, table, year, offset, delimiters, null)
        table.update_from_temporary(ttable, columns, bind=connection)

        trans.commit()

def run_aggregations(table, year):
    '''
    Runs aggregation queries from protocol
    '''
    table = gen_data_table(table, META)
    table.map_from_database()

    with ENGINE.connect() as connection:
        trans = connection.begin()

        table.run_aggregations(year, bind=connection)

        trans.commit()

def generate_backup():
    '''Create/Recriate file monitored by backup script in production'''
    chdir(settings.BACKUP_FOLDER)
    f = open(settings.BACKUP_FILE,"w")
    f.write(str(datetime.now()))
    f.close()

def execute_sql_script(sql_scripts, sql_path=settings.SCRIPTS_FOLDER):
    if type(sql_scripts) == str:
        sql_scripts = [sql_scripts]
    with ENGINE.connect() as connection:
        trans = connection.begin()
        for script in sql_scripts:
            with open(sql_path + '/' + script) as sql:
                connection.execute(text(sql.read()))
        trans.commit()

def execute_sql_group(script_group, sql_path=settings.SCRIPTS_FOLDER, files=False):
    if not files:
        sql_script = [DATA_GROUP[group.upper()] for group in script_group.split(",")]
    else:
        sql_script = script_group.split(",")
    for sql in sql_script:
        execute_sql_script(sql, sql_path + '/')

def drop_group(script_group, files=False):
    script_group = script_group.split(",")
    selected_tables = []
    if not files:
        for group in script_group:
            selected_tables += DATA_GROUP[group.upper()]
    else:
        selected_tables = script_group

    for table in reversed(selected_tables):
        if table in DATABASE_TABLE_NAME:
            table_name = DATABASE_TABLE_NAME[table]
        else:
            table_name = table.replace('.sql', '')
        drop(table_name)
