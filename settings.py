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
DATABASE = 'simcaq_dev'

# URI structure. Standards to login:password model, but can be changed as needed.
DATABASE_URI = '{}://{}:{}@{}/{}'.format(DATABASE_DIALECT, DATABASE_USER,
                                         DATABASE_USER_PASSWORD, DATABASE_HOST, DATABASE)

# Folder where mapping protocols can be found - relative to root
MAPPING_PROTOCOLS_FOLDER = 'mapping_protocols'

# Folder for table definitions files
TABLE_DEFINITIONS_FOLDER = 'table_definitions'

# Pairing report paths
PAIRING_OUTPUT_FOLDER = './pairing'
XLS_OUTPUT_FILE_NAME = 'pairing.xlsx'
DICTIONARY_FOLDER = './dictionaries'
# Pairing variable list will eventually be moved to mapping protocol - kept because of old
# script used as base
PAIRING_VARIABLE_FILE = './variables.txt'

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
