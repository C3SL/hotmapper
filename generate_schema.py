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
