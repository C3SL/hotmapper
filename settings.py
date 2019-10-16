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

'''Settings used by the database module'''
import logging

# SQL dialect used by sqlalchemy.
DATABASE_DIALECT = 'monetdb'

# Login credentials in database
DATABASE_USER = 'monetdb'
DATABASE_USER_PASSWORD = 'monetdb'

# Host to connect to. Bulk inserts won't work remotely unless you can specify an
# absolute path in the server
DATABASE_HOST = 'localhost'

# Database to connect to
DATABASE = 'hotmapper_demo'

# Column used to run aggregations and denormalizations
YEAR_COLUMN = 'ano_censo'

# URI structure. Standards to login:password model, but can be changed as needed.
DATABASE_URI = '{}://{}:{}@{}/{}'.format(DATABASE_DIALECT, DATABASE_USER,
                                         DATABASE_USER_PASSWORD, DATABASE_HOST, DATABASE)
# Folder and file where backup is created - absolute path
BACKUP_FOLDER = '/home/banco/dumps/monetdb/'
BACKUP_FILE = 'backupdadoseducacionais'

# Folder where mapping protocols can be found - relative to root
MAPPING_PROTOCOLS_FOLDER = 'mapping_protocols'

# Folder for table definitions files
TABLE_DEFINITIONS_FOLDER = 'table_definitions'

# Folder for scripts and sql tables
SCRIPTS_FOLDER = 'sql'

# Source table definitions
SOURCE_TABLE_NAME = 'fonte'
SOURCE_TABLE_COLUMNS = {
    'table_name': 'tabela',
    'source': 'fonte'
}

# If set to True, will display SQL queries sent to database
ECHO = False

# Logging
LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = "%(levelname)s - %(name)s: %(message)s"

# Info used on file format conversions
CHUNK_SIZE = 500
