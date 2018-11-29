'''Generates schema in mysql dialect. Useful for documentation'''
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from database.database_table import DatabaseTable
import settings


engine = create_engine(settings.DATABASE_URI, echo=settings.ECHO)
meta = MetaData(bind=engine)

insp = inspect(engine)

table_list = insp.get_table_names()
for table_name in table_list:
    if table_name in [t.name for t in meta.sorted_tables]:
        continue
    table = DatabaseTable(table_name, meta)
    table.map_from_database()

to_output = [table for table in meta.sorted_tables if not table.name.startswith('mapping_')]

for table in to_output:
    print(CreateTable(table).compile(dialect=mysql.dialect()).string, end=';')
