'''Database manipulation actions - these can be used as models for other modules.'''
import logging
from sqlalchemy import create_engine, MetaData

from database.database_table import gen_data_table, gen_temporary, copy_to_temporary
from mapping_functions import generate_pairing_xlsx, generate_pairing_csv
import settings


ENGINE = create_engine(settings.DATABASE_URI, echo=settings.ECHO)
META = MetaData(bind=ENGINE)

logging.basicConfig(format = settings.LOGGING_FORMAT)

database_table_logger = logging.getLogger('database.database_table')
database_table_logger.setLevel(settings.LOGGING_LEVEL)
sqlalchemy_logger = logging.getLogger('sqlalchemy.engine')
sqlalchemy_logger.setLevel(settings.LOGGING_LEVEL)

def temporary_data(connection, file_name, table, year, offset=2, sep=';', null=''):
    header = open(file_name, encoding="ISO-8859-9").readline()
    header = header.split(sep)
    columns = table.mount_original_columns(header, year)

    ttable = gen_temporary('t_' + table.name, META, *columns)
    table.set_temporary_primary_keys(ttable, year)

    ttable.create(bind=connection)
    copy_to_temporary(connection, file_name, ttable, offset, sep, null)

    return ttable


def insert(file_name, table, year, offset=2, sep=';', null=''):
    '''Inserts contents of csv in file_name in table using year as index for mapping'''
    table = gen_data_table(table, META)

    with ENGINE.connect() as connection:
        trans = connection.begin()

        ttable = temporary_data(connection, file_name, table, year, offset, sep, null)
        table.insert_from_temporary(connection, ttable, year)

        trans.commit()

def create(table):
    '''Creates table from mapping_protocol metadata'''
    table = gen_data_table(table, META)

    table.create()

def drop(table):
    '''Drops table'''
    table = gen_data_table(table, META)

    table.drop()

def remap(table):
    '''Applies change made in mapping protocols to database'''
    table = gen_data_table(table, META)

    table.remap()

def generate_pairing_report(output='csv'):
    '''Generates the pairing report for a given table'''
    if output == 'csv':
        generate_pairing_csv(ENGINE)
    elif output == 'xlsx':
        generate_pairing_xlsx(ENGINE)
    else:
        print('Unsuported output type "{}"'.format(output))

def update_from_file(csv_file, table, year, columns=None, target_list=None,
                     offset=2, sep=';', null=''):
    '''Updates table columns from an input csv file'''
    table = gen_data_table(table, META)

    if columns is None:
        columns = []
    columns = columns + table.columns_from_targets(target_list)

    with ENGINE.connect() as connection:
        trans = connection.begin()

        ttable = temporary_data(connection, csv_file, table, year, offset, sep, null)
        table.update_from_temporary(connection, ttable, year, columns)

        trans.commit()
