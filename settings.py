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
