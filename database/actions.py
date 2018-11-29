'''Database manipulation actions - these can be used as models for other modules.'''
import logging
from sqlalchemy import create_engine, MetaData
from os import chdir
from datetime import datetime
from database.base import MissingTableError
from database.database_table import gen_data_table, copy_tabbed_to_csv
import settings

ENGINE = create_engine(settings.DATABASE_URI, echo=settings.ECHO)
META = MetaData(bind=ENGINE)

logging.basicConfig(format = settings.LOGGING_FORMAT)

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

def update_from_file(file_name, table, year, columns=None, target_list=None,
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
