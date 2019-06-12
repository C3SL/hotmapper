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

'''This module contains the definition of the DatabaseTable class and a constructor'''
import os
import time
import json
import re
import logging
from sqlalchemy import Table, Column, inspect, Integer, String, Boolean,\
                       PrimaryKeyConstraint, ForeignKeyConstraint, text
from sqlalchemy.sql import select, insert, update, delete, func
import pandas as pd

from database.base import DatabaseColumnError, MissingProtocolError, DatabaseMappingError,\
                          InvalidTargetError, MissingForeignKeyError, MissingTableError,\
                          CircularReferenceError
from database.protocol import Protocol
from database.types import get_type
import settings

# Disable no-member warnings to silence false positives from Table instances dinamically generated
# attributes
# pylint: disable=no-member

logger = logging.getLogger(__name__)

def gen_source_table(meta):
    '''Returns a source table object, so source entries can be added or updated'''
    logger.info("Acquiring source table")
    columns = settings.SOURCE_TABLE_COLUMNS
    source_table = Table(settings.SOURCE_TABLE_NAME, meta,
                         Column('id', Integer(), primary_key=True),
                         Column(columns['table_name'], String(63), key='table_name'),
                         Column(columns['source'], String(255), key='source'),
                         extend_existing=True)

    return source_table

def gen_mapping_table(table):
    '''Generates an object with the columns of a mapping table. Once the
    mapping table becomes a class by itself, this might be transformed into
    a constructor'''
    logger.info("Acquiring mapping table for %s", table.name)
    mapping_table = Table('mapping_' + table.name, table.metadata,
                          Column('id', Integer, nullable=False, primary_key=True),
                          Column('target_name', String(63)),
                          Column('name', String(63)),
                          Column('type', String(63)),
                          extend_existing = True)

    return mapping_table

def get_primary_keys(table):
    '''Returns a list of columns corresponding to the primary key of a given table instance'''
    return [c[1] for c in table.primary_key.columns.items()]

def tabbed_iterate(tabbed_file_name, column_mappings, chunk_size):
    '''
    Iterates over a tabbed file and yields chunks of chunk_size
    '''
    counter = 0
    chunk = []

    tabbed_file = open(tabbed_file_name)
    for line in tabbed_file:
        line = list(line)
        entry = [''.join(line[int(p0-1):int(p0+pt-1)]).strip().strip('.') for p0, pt in column_mappings]
        chunk.append(tuple(entry))
        counter += 1
        if counter == chunk_size:
            yield chunk
            chunk = []
            counter = 0
    if chunk:
        yield chunk
    tabbed_file.close()

def copy_tabbed_to_csv(tabbed_file_name, column_mappings, chunk_size, output_file_name,
                       column_names=None, sep=';'):
    '''
    Copies tabbed positional data into csv
    '''
    output_file = open(output_file_name, 'w')
    if column_names:
        if not len(column_names) == len(column_mappings):
            print('column_names != column_mappings')
            # Raise an exception here?
            return
        header = sep.join(column_names)
        output_file.write(header + '\n')

    for chunk in tabbed_iterate(tabbed_file_name, column_mappings, chunk_size):
        chunk = pd.DataFrame(chunk)

        chunk.to_csv(output_file, header=False, index=False, sep=sep)

def is_aggregation(in_string):
    '''
    Any function will be treated as aggregation. Might need
    some extra love.
    '''
    # Should be revisited
    if re.search(r'^~[a-zA-Z0-9_]+\(.+\)', in_string) is not None:
        return True
    else:
        return False

class DatabaseTable(Table):
    '''Database class for module operations, tweaked for Monetdb. Inherits from
    sqlalchemy Table type, sets table columns if table is available in database and
    can actively create or drop itself'''
    # Disable too-many-ancestors for Table inheritance - sqlalchemy.Table itself already has
    # more ancestors than what standard pylint finds reasonable.
    # W0223 stands for abstract method not being implemented: params and unique_params, from
    # the Table's ancestor Immutable, are not supposed to be implemented in the Table instance,
    # for it's defined for query objects and is used there.
    # pylint: disable=too-many-ancestors, W0223


    def __init__(self, *args, **kwargs):
        '''Instantiation works the same as sqlalchemy.Table, but a mapping table and
        mapping protocol are linked.'''
        super().__init__(*args, **kwargs)

        if not hasattr(self, '_mapping_table'):
            self._mapping_table = gen_mapping_table(self)
        if not hasattr(self, '_protocol'):
            self._protocol = None

        if 'protocol' in kwargs.keys():
            self.load_protocol(kwargs['protocol'])

    def get_temporary(self, header_columns=[], year=None):
        '''
        Returns a temporary table with identical structure to self. If a header_columns list
        is passed, will check protocol to ensure any of the columns is not mapped. Unmapped
        columns will be added with original name and type VARCHAR(255).

        If a header_columns list is provided, a year must be passed to allow mapping to originals.
        '''
        if header_columns and not year:
            raise Exception

        additional = header_columns.copy()
        if year:
            for column in header_columns:
                target = self._protocol.target_from_original(column, year)
                try:
                    if target and self._protocol.dbcolumn_from_target(target):
                        additional.remove(column)
                except InvalidTargetError:
                    pass

        timestamp = time.strftime('%Y%m%d%H%M%S')
        name = '_' + timestamp + '_' + self.name

        logger.info("Acquiring temporary table with name '%s'", name)
        logger.debug("Temporary table '%s' with list of extra columns %s", name, header_columns)
        ttable = Table(name, self.metadata, prefixes=['TEMPORARY'], schema='tmp')

        for target in self._protocol.get_targets():
            try:
                column_name, column_type = self._protocol.dbcolumn_from_target(target)
                ttable.append_column(Column(column_name, get_type(column_type)))
            except InvalidTargetError:
                pass

        pks = get_primary_keys(self)
        primary_key = []
        for pk in pks:
            primary_key.append(ttable.columns.get(pk.name))

        ttable.constraints.add(PrimaryKeyConstraint(*primary_key))

        for column in additional:
            ttable.append_column(Column(column, String(255)))

        return ttable

    def populate_temporary(self, ttable, in_file, header, year, delimiters=[';', '\\n', '"'],
                           null='', offset=2, bind=None):
        '''
        Visits a temporary table ttable and bulk inserts data from in_file in it. The header
        list of the original file must be supplied to ensure columns are correctly mapped.
        '''
        if bind is None:
            bind = self.metadata.bind

        columns = header.copy()
        for i, column in enumerate(columns):
            try:
                target = self._protocol.target_from_original(column, year)
                columns[i] = self._protocol.dbcolumn_from_target(target)[0] or column
            except InvalidTargetError:
                pass

        columns = ['"{}"'.format(c) for c in columns]
        delimiters = ["'{}'".format(d) for d in delimiters]
        delimiters = ', '.join(delimiters)
        query_columns = ', '.join(columns)
        query = 'COPY OFFSET {} INTO {}({}) '.format(offset, ttable.name, query_columns)
        query = query + "FROM '{}'({}) USING DELIMITERS {} ".format(in_file, query_columns,
                                                                    delimiters)
        query = query + "NULL AS '{}'".format(null)

        query = text(query)

        bind.execute(query)

        return query

    def create_temporary_mirror(self, year, bind=None):
        '''
        Creates a new temporary table where its data mirrors the original, taken directly from the database
        '''
        ttable = self.get_temporary(year=year)
        ttable.create(bind)
        if bind is None:
            bind = self.metadata.bind

        original_columns = list(self.columns)
        query = ttable.insert().from_select(original_columns, select(original_columns))
        bind.execute(query)

        return ttable

    def check_protocol(self):
        '''
        Raises MissingProtocolError if no protocol is loaded.
        '''
        if self._protocol is None:
            raise MissingProtocolError("You must first load a protocol")

    def map_from_database(self, bind=None):
        '''
        Inspects database and map columns to table object to match exsisting table.

        If table doesn't exist, nothing happens.
        Avoid using mirroring feature, since it doesn't handle well multicolumn pks

        This method can be deprecated once the foreign_key mapping from sqlalchemy-monetdb
        gets fixed on pypi (already fixed on github). In that case, instantiation can use
        reflect=True to map the table from database.
        '''
        if self.columns.keys():
            logger.warning("Table mapping already has columns. Nothing done.")
            return

        if bind is None:
            bind = self.metadata.bind

        logger.debug("Acquiring info about table %s", self.name)
        insp = inspect(bind)

        if self.exists(bind=bind):
            logger.info("Using existing table %s", self.name)
            for column in insp.get_columns(self.name):
                self.append_column(Column(column['name'], column['type']))
            pks = insp.get_pk_constraint(self.name)
            if 'constrained_columns' in pks.keys():
                pks = [self.columns.get(k) for k in pks['constrained_columns']]
                self.primary_key = PrimaryKeyConstraint(*pks)

            foreign_keys = insp.get_foreign_keys(self.name)

            for foreign_key in foreign_keys:
                keys = [self.columns.get(c) for c in foreign_key["constrained_columns"]]
                ref_table = DatabaseTable(foreign_key['referred_table'], self.metadata)
                ref_table.map_from_database()

                fkeys = [ref_table.columns.get(c) for c in foreign_key['referred_columns']]

                self.constraints.add(ForeignKeyConstraint(keys, fkeys))

        else:
            logger.debug("Table %s not present in database.", self.name)
            raise MissingTableError(self.name)

    def get_definitions(self):
        '''
        Returns a dictionary with definitions from a table definitions file
        '''
        definitions = self.name + '.json'
        logger.debug("Acquiring definitions from %s", definitions)
        definitions = os.path.join(settings.TABLE_DEFINITIONS_FOLDER, definitions)
        definitions = json.loads(open(definitions).read())
        logger.debug("Definitions loaded")

        return definitions

    def load_protocol(self, protocol):
        '''
        Takes a Protocol instance and loads it for further use
        '''
        if not isinstance(protocol, Protocol):
            raise TypeError(('The passed protocol must be an instance of the Protocol class, '
                             'got %s' % type(protocol)))
        self._protocol = protocol

    def get_protocol(self):
        '''
        Returns the linked protocol if it exists
        '''
        self.check_protocol()
        return self._protocol

    def create_mapping_table(self, bind=None):
        '''
        Creates the mapping table in the database
        '''
        self.check_protocol()
        if bind is None:
            bind = self.metadata.bind

        if not self._mapping_table.exists(bind=bind):
            logger.info("Creating mapping table %s", self._mapping_table.name)
            self._mapping_table.create(bind=bind)

        with bind.connect() as connection:
            logger.info("Populating mapping table")
            columns = [c[1] for c in self.columns.items()]
            for c in columns:
                column = {}
                column['target_name'] = self._protocol.target_from_dbcolumn(c.name)
                if not column['target_name']:
                    continue
                column['name'] = c.name
                column['type'] = str(c.type)
                logger.debug("Mapping column %s with target_name %s",
                             column['name'], column['target_name'])
                query = insert(self._mapping_table).values(**column)
                connection.execute(query)

    def set_source(self, bind=None):
        '''
        Inserts or updates table entry in the sources table
        '''
        if bind is None:
            bind = self.metadata.bind
        definitions = self.get_definitions()
        source_table = gen_source_table(self.metadata)

        # Create source table if doesnt exist
        if not source_table.exists(bind=bind):
            logger.debug("Source table not found. Creating...")
            source_table.create(bind=bind)
            logger.debug("Source table creation: no exceptions.")

        source = definitions['data_source']

        logger.debug("Checking for '%s' in source table", self.name)
        base_select = select([source_table.c.id]).where(source_table.c.table_name == self.name)
        table_id = bind.execute(base_select).fetchone()

        if table_id:
            logger.debug("Table found. Running update query")
            table_id = table_id[0]
            base_query = update(source_table).where(source_table.c.id == table_id)
        else:
            logger.debug("Table not found. Running insert query")
            base_query = insert(source_table)

        base_query = base_query.values(table_name=self.name, source=source)

        bind.execute(base_query)

    def map_from_protocol(self, create=False, bind=None):
        '''
        Uses information from a protocol to generate self columns. Table definitions must also
        be defined to allow primary key and foreign keys addition.
        Useful for table creation.
        '''
        self.check_protocol()
        if self.columns.keys():
            logger.warning("Table mapping already has columns. Nothing done.")
            return
        if bind is None:
            bind = self.metadata.bind

        definitions = self.get_definitions()

        for column in self._protocol.get_targets():
            try:
                column = self._protocol.dbcolumn_from_target(column)
            except InvalidTargetError:
                continue
            if column[0]:
                column[0] = column[0].strip()
            column = Column(column[0], get_type(column[1]))

            self.append_column(column)

        primary_key = [self.columns.get(c) for c in definitions['pk']]
        if primary_key:
            self.constraints.add(PrimaryKeyConstraint(*primary_key))

        for foreign_key in definitions["foreign_keys"]:
            keys = [self.columns.get(c) for c in foreign_key["keys"]]
            ref_table = DatabaseTable(foreign_key["reference_table"], self.metadata)

            protocol_path = os.path.join(settings.MAPPING_PROTOCOLS_FOLDER, ref_table.name + '.csv')
            protocol = Protocol()
            try:
                protocol.load_csv(protocol_path)
                ref_table.load_protocol(protocol)
                ref_table.map_from_protocol(create=create, bind=bind)
                if create:
                    ref_table.create(bind=bind)
            except FileNotFoundError:
                if ref_table.exists(bind=bind):
                    ref_table.map_from_database(bind=bind)
                else:
                    logger.critical(('Table %s could not be mapped. Create it or assemble a '
                                     'mapping protocol.'), ref_table.name)
                    raise DatabaseMappingError("Table {} can't be mapped".format(ref_table.name))
            fkeys = [ref_table.columns.get(c) for c in foreign_key["reference_columns"]]

            self.constraints.add(ForeignKeyConstraint(keys, fkeys))

    def create(self, bind=None, checkfirst=False):
        '''
        Overrides sqlalchemy's create method to use map_from_protocol before creating.
        '''
        if bind is None:
            bind = self.metadata.bind
        if self.exists(bind=bind):
            logger.error("Table %s already exists", self.name)
            return

        self.map_from_protocol(create=True, bind=bind)

        super().create(bind=bind, checkfirst=checkfirst)

    def drop(self, bind=None):
        '''
        Override sqlalchemy's drop method to drop mapping table along with original.
        '''
        logger.info("Dropping mapping table %s...", self.name)
        self._mapping_table.drop(bind=bind, checkfirst=True)
        if not self.exists(bind=bind):
            logger.error("Table %s doesn't exist", self.name)
            return
        super().drop(bind=bind)

    def drop_column(self, name, target=None, bind=None):
        '''
        Drops a column given by name using connection. If no transaction control is necessary,
        and engine can be passed instead of a connection
        '''
        if bind is None:
            bind = self.metadata.bind

        if target is not None and self._mapping_table.exists():
            logger.debug("Deleting target %s from %s", target, self._mapping_table.name)
            query = delete(self._mapping_table)
            query = query.where(self._mapping_table.c.target_name == target)
            bind.execute(query)
        elif target is None:
            logger.warning("Dropping column %s without mapping", name)
        else:
            logger.warning("Table %s has no mpaping", self.name)

        column = self.columns.get(name)
        if column is not None:
            logger.debug("Dropping column %s from %s", name, self.name)
            query = "alter table {} drop column {}".format(self.name, name)
            bind.execute(query)

    def add_column(self, name, field_type, target=None, bind=None):
        '''
        Adds a column with name and type using connection. If no transaction control is
        necessary, and engine can be passed instead of a connection
        '''
        if bind is None:
            bind = self.metadata.bind

        field_type = get_type(field_type)


        if target is not None and self._mapping_table.exists():
            entry = {
                'target_name': target,
                'name': name,
                'type': str(field_type)
            }
            logger.debug("Mapping column %s with type %s. Target: %s", name, str(field_type), target)
            query = insert(self._mapping_table).values(**entry)
            bind.execute(query)

        column = self.columns.get(name)
        if column is None:
            logger.debug("Adding column %s with type %s", name, str(field_type))
            column = Column(name, field_type)
            self.append_column(column)

            query = "alter table {} add column {} {}".format(self.name, name, str(field_type))
            bind.execute(query)
        else:
            logger.warning("Column %s already exists. Won't attempt to create.", name)

    def redefine_column(self, connection, original_name, new_name=None, new_type=None):
        '''
        Redefines a column to match a new name and a new type. Can be used to change names,
        types or both for a column.
        '''
        if original_name not in self.columns.keys():
            logger.error("Column %s doesn't exist in table %s", original_name, self.name)
            raise DatabaseColumnError(original_name)
        if not (new_name or new_type):
            return
        if not new_type:
            new_type = str(self.c.get(original_name).type)
        if not new_name:
            new_name = original_name

        self.drop_column(original_name, bind=connection)
        self.add_column(new_name, new_type, bind=connection)

        field_type = get_type(new_type.lower())

        self.columns.replace(Column(original_name, field_type, key=new_name))
        column = self.columns.get(new_name)
        column.name = new_name
        column.type = field_type

        query = update(self._mapping_table).values(name=new_name).\
                    where(self._mapping_table.c.name == original_name)
        connection.execute(query)

    def transfer_data(self, connection, transfer_list):
        '''
        Receives a list of columns to be transfered. Transfered columns are backed up,
        removed, added with new parameters and then repopulated.
        transfer_list must be a list of dictionaries with the following fields:
        name - the name of the original column;
        new_name - name for the new column. If None is passed, original name is used;
        new_type - type for the new column. If None is passed, original type is used.
        '''
        if not transfer_list:
            return
        pk_columns = list(self.primary_key.columns)
        if not pk_columns:
            logger.error("Cant transfer data for table that has no Primary Key.")
            return

        temp_pk_columns = [c.copy() for c in pk_columns]

        original_columns = [a['name'] for a in transfer_list]
        original_columns = [self.columns.get(name) for name in original_columns]
        temp_columns = [Column(c['new_name'], get_type(c['new_type'])) for c in transfer_list]

        trans = connection.begin()

        ttable = Table('t_' + self.name, self.metadata, *(temp_pk_columns + temp_columns),
                       schema="tmp", prefixes=["TEMPORARY"])
        ttable.create(bind=connection)

        ttable.schema = None

        base_select = select(self.primary_key.columns + original_columns)
        connection.execute(insert(ttable).from_select((temp_pk_columns + temp_columns),
                                                      base_select))

        for transfer in transfer_list:
            self.redefine_column(connection, transfer['name'],
                                 transfer['new_name'], transfer['new_type'])

        values = {}
        for item in transfer_list:
            values[item['new_name']] = ttable.columns.get(item['new_name'])
        base_update = update(self).values(**values)
        for original_pk, temp_pk in zip(list(self.primary_key.columns), temp_pk_columns):
            base_update = base_update.where(original_pk == temp_pk)

        connection.execute(base_update)

        trans.commit()

    def compare_mapping(self):
        '''
        Compares contents of mapping table to protocol and returns tuple with differences in
        the following format:
        new_columns, to_drop_columns, update_columns

        The elements of the tuple are lists containing:
            new_columns - columns that are in the protocol, but not in the mapping_table;
            to_drop_columns - columns that are in the mapping_table but not in the protocol;
            update_columns - columns that are in both places, but with type or name differences.

        The method uses target_names as the criteria to decide if columns are the same or not.
        '''
        self.check_protocol()

        protocol_target_list = self._protocol.get_targets()

        query = self._mapping_table.select()
        results = self.metadata.bind.execute(query).fetchall()
        db_target_list = [t[1] for t in results]

        new_columns = [c for c in protocol_target_list if c not in db_target_list and c != '']
        to_drop_columns = [c for c in db_target_list if c not in protocol_target_list]

        update_columns = []
        for target in protocol_target_list:
            query = select([self._mapping_table.c.name, self._mapping_table.c.type])\
                   .where(self._mapping_table.c.target_name == target)
            result = self.metadata.bind.execute(query).fetchone()
            if not result:
                continue
            name, field_type = result
            try:
                new_name, new_type = self._protocol.dbcolumn_from_target(target)
            except InvalidTargetError:
                to_drop_columns.append(target)
                continue
            new_name = new_name.strip()
            new_type = str(get_type(new_type))
            if name == new_name and field_type == new_type:
                continue
            update_columns.append({
                "name": name,
                "new_name": new_name,
                "new_type": new_type
            })

        return new_columns, to_drop_columns, update_columns

    def remap(self, auto_confirmation=True):
        '''
        Checks mapping protocol for differences in table structure - then
        attempts to apply differences according to what is recorded in the
        mapping table
        '''
        if not self.exists():
            print("Table {} doesn't exist".format(self.name))
            return

        self.check_protocol()

        mtable = self._mapping_table

        if not mtable.exists():
            print("Mapping table for {} not found.".format(self.name))
            print("Creating mapping table...")
            self.create_mapping_table()
            print("Done!")

        self.set_source()

        new_columns, to_drop_columns, update_columns = self.compare_mapping()

        accept_new_columns, accept_drop_columns, accept_update_columns = [True for _ in range(3)]
        if not auto_confirmation:
            if new_columns:
                print('The following columns will be CREATED:', ', '.join(new_columns))
                prompt = input('Is it right (yes or no)? ')
                accept_new_columns = prompt == 'yes' or prompt == 'y' or prompt == 1
            if to_drop_columns:
                print('The following columns will be DROPPED:', ', '.join(to_drop_columns))
                prompt = input('Is it right (yes or no)? ')
                accept_drop_columns = prompt == 'yes' or prompt == 'y' or prompt == 1
            if update_columns:
                update_list = [update_dict['name'] + ' -new name: ' + update_dict['new_name']
                               + ' -new type: ' + update_dict['new_type'] for update_dict in update_columns]
                print('The following columns will be UPDATED:', ', '.join(update_list))
                prompt = input('Is it right (yes or no)? ')
                accept_update_columns = prompt == 'yes' or prompt == 'y' or prompt == 1

        with self.metadata.bind.connect() as connection:
            # Create new columns
            if accept_new_columns:
                for column in new_columns:
                    try:
                        dbcolumn = self._protocol.dbcolumn_from_target(column)
                    except InvalidTargetError:
                        continue

                    self.add_column(dbcolumn[0], dbcolumn[1], column, bind=connection)

            # Drop columns
            if accept_drop_columns:
                for column in to_drop_columns:
                    column_name = select([mtable.c.name]).where(mtable.c.target_name == column)
                    column_name = connection.execute(column_name).fetchone()[0]
                    if not column_name:
                        continue

                    self.drop_column(column_name, column, bind=connection)

            # Update existing columns
            if accept_update_columns:
                self.transfer_data(connection, update_columns)

    def _get_variable_target(self, original, year):
        '''
        Searches the protocol for a target for original. It will first check if the argument is
        a dbcolumn, and later if it is an original column. If none of them verifies, it will
        check if the argument is an actual target itself.
        '''
        try:
            original = original.strip()
        except AttributeError:
            original = ''
        # Verifies if original is a dbcolumn or an original data column - or neither
        target = self._protocol.target_from_dbcolumn(original) or \
                 self._protocol.target_from_original(original, year)
        # Verifies if original is actually a column target
        if target is None:
            if original in self._protocol.get_targets():
                target = original

        return target

    def _derivative_recursion(self, original, year, recursion_list=[]):
        '''
        Verifies if a string is a derivative, and splits it to verify if its parts are other
        derivatives themselves.

        Results of the recursion are stored in self._derivatives. The 'level' key will show
        the variable level of dependency. 0 will be natural variables, 1 and higher will be
        derivatives. Variables of a certain level should only be updated after all lower levels
        have been resolved and updated, to ensure dependencies will not be ignored.
        '''
        if self._protocol is None:
            return {'original': original, 'dbcolumn': original, 'new': original, 'level': 0}
        target = self._get_variable_target(original, year)


        if target in self._derivatives:
            # This variable has been evaluated already, just return
            return self._derivatives[target]

        if target is not None and target in recursion_list:
            # This is a circular reference. Don't be like that.
            print(target)
            raise CircularReferenceError

        original = self._protocol.original_from_target(target, year) or original
        try:
            dbcolumn = self._protocol.dbcolumn_from_target(target)
        except InvalidTargetError:
            dbcolumn = None

        if is_aggregation(original):
            # Aggregation not integrated
            derivative = {'original': original, 'dbcolumn': dbcolumn, 'new': original, 'level': -1}
            self._derivatives[target] = derivative
            return derivative

        denorm_match = re.match(r'~?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', original)
        if denorm_match is not None:
            table, column = denorm_match.groups()
            table = gen_data_table(table, self.metadata)

            if table is self:
                return self._derivative_recursion(column, year, recursion_list)
            derivative = table._resolv_derivative(column, year)
            self._derivatives[target] = {'original': original, 'dbcolumn': dbcolumn, 'level': 0,
                                         'new': '.'.join([table.name, derivative['dbcolumn'][0]])}
            return self._derivatives[target]

        if not original.startswith('~'):
            # Possibly keyword, definitely not a variable. Shouldn't change the level.
            return {'original': original, 'processed': original, 'dbcolumn': dbcolumn, 'level': 0}

        # Well, looks like we actually got a derivative here
        original = original.strip('~ ')
        str_list = re.findall(r'("[\w]+"|[\w]+)', original)
        level = 0
        substitutions = []
        recursion_list.append(target)
        for substring in str_list:
            derivative = self._derivative_recursion(substring.strip('"'), year,
                                                   recursion_list=recursion_list)
            if derivative['dbcolumn']:
                substitutions.append({'original': substring, 'new': derivative['dbcolumn'][0]})
            if derivative['level'] >= level:
                level = derivative['level'] + 1

        processed = original
        for substitution in substitutions:
            processed = re.sub(substitution['original'], substitution['new'], processed)
        self._derivatives[target] = {'original': original, 'dbcolumn': dbcolumn, 'level': level,
                                     'processed': processed}
        return self._derivatives[target]

    def _resolv_derivative(self, original, year):
        '''
        Populates self._derivatives with all necessary derivatives to satisfy original in a given
        year.
        '''
        if not hasattr(self, '_derivatives'):
            self._derivatives = {}
        return self._derivative_recursion(original, year)

    def _get_denormalizations(self, ttable, originals, year):
        '''
        Searches protocol for denormalizations and yields the necessary update queries.
        '''
        exp = r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'
        external = {}
        for dst, original in originals:
            original = original.strip(' ~\n\t')
            for match in re.finditer(exp, original):
                table, column = match.groups()
                if table not in external:
                    external[table] = []
                external[table].append([dst, text(original)])

        for table in external:
            query = update(ttable)
            for dst, src in external[table]:
                query = query.values(**{dst[0]: src})
            for fk_column, fkey in self.get_relations(table):
                fk_column = ttable.columns.get(fk_column.name)
                query = query.where(fk_column == fkey)
            if year:
                query = query.where(ttable.c.ano_censo == year)
            yield query

    def apply_derivatives(self, ttable, columns, year, bind=None):
        '''
        Given a list of columns, searches for derivatives and denormalizations and applies them
        in the appropriate order. Dependencies will be updated regardless of being or not in the
        columns list.
        '''
        if bind is None:
            bind = self.metadata.bind

        self._derivatives = {}
        for original in columns:
            self._resolv_derivative(original, year)

        originals = [(self._derivatives[d]['dbcolumn'], self._derivatives[d]['original'])\
                      for d in self._derivatives if self._derivatives[d]['level'] == 0]

        t_schema = ttable.schema
        ttable.schema = None
        for query in self._get_denormalizations(ttable, originals, year):
            bind.execute(query)

        ttable.schema = t_schema
        if len(self._derivatives) > 0:
            max_level = max([self._derivatives[d]['level'] for d in self._derivatives])
            derivative_levels = []
            for i in range(max_level):
                i = i+1
                query = {}
                level = [self._derivatives[d] for d in self._derivatives if\
                         self._derivatives[d]['level'] == i]
                for derivative in level:
                    query[derivative['dbcolumn'][0]] = text(derivative['processed'])

                query = update(ttable).values(**query)
                print(query)
                bind.execute(query)

        return self._derivatives

    def _get_aggregations(self, year):
        '''
        Will iterate over all targets and return column and query for all aggregations.
        '''
        self.check_protocol()

        for target in self._protocol.get_targets():
            original = self._protocol.original_from_target(target, year)
            if is_aggregation(original):
                column, _ = self._protocol.dbcolumn_from_target(target)
                if isinstance(column, str):
                    column = self.columns.get(column)
                    yield column, original.strip('~ ')

    def _aggregate(self, column, aggregation, source_column, year=None):
        '''
        Given a column and an aggregation, will return the appropriate query to be executed.
        '''
        referred_table = gen_data_table(source_column.table.name, self.metadata)
        referred_table.map_from_database()
        selecter = select([getattr(func, aggregation)(source_column)])

        try:
            fk_tuples = [(fk_column, fkey) for fk_column, fkey in referred_table.get_relations(self)]
        except MissingForeignKeyError:
            fk_tuples = [(fk_column, fkey) for fk_column, fkey in self.get_relations(referred_table)]

        for fk_column, fkey in fk_tuples:
            selecter = selecter.where(fk_column == fkey)
        if year:
            selecter = selecter.where(self.c.ano_censo == year)

        query = update(self).values(**{column.name: selecter})

        return query

    def run_aggregations(self, year, bind=None):
        '''
        Searches protocol for all aggregations for a given year and executes them.
        '''
        self.check_protocol()
        if not bind:
            bind = self.metadata.bind

        exp = r'\(.+\)'
        for column, aggregation in self._get_aggregations(year):
            func = re.sub(exp, '', aggregation)
            source_column = re.search(exp, aggregation).group()
            source_column = source_column.strip('()')
            source_table, source_column = source_column.split('.')
            source_table = gen_data_table(source_table, self.metadata)
            source_table.map_from_database()
            source_column = source_table.columns.get(source_column)
            if source_column is not None:
                query = self._aggregate(column, func, source_column, year)
                bind.execute(query)

        # Run derivatives
        ttable = self.create_temporary_mirror(year, bind)
        self.apply_derivatives(ttable, ttable.columns.keys(), year, bind)
        self.update_from_temporary(ttable, ttable.columns.keys(), bind)

    def get_relations(self, table):
        '''
        Yields relations between two tables in format
        [foreign_key, referred_key]
        '''
        if isinstance(table, str):
            table = DatabaseTable(table, self.metadata)
        foreign_key = None
        for fk in self.foreign_key_constraints:
            if fk.referred_table is not table:
                continue
            foreign_key = fk
            break
        if not foreign_key:
            raise MissingForeignKeyError(table)
        for _, fk_column in foreign_key.columns.items():
            fkey = list(fk_column.foreign_keys)[0]
            fkey = fkey.column.name
            fkey = table.columns.get(fkey)

            yield fk_column, fkey

    def insert_from_temporary(self, ttable, bind=None):
        '''
        Transfer data entries from a temporary table to self.
        '''
        if bind is None:
            bind = self.metadata.bind

        temp_schema = ttable.schema
        ttable.schema = None

        query_dst = []
        query_src = []
        for column in self.columns.items():
            temporary_column = ttable.columns.get(column[0])
            if temporary_column is not None:
                query_src.append(temporary_column)
                query_dst.append(column[1])

        query_src = select(query_src)
        query = insert(self).from_select(query_dst, query_src)

        bind.execute(query)

        ttable.schema = temp_schema

    def update_from_temporary(self, ttable, columns, bind=None):
        '''
        Update data in columns from self from a given temporary table.
        '''
        if bind is None:
            bind = self.metadata.bind

        temp_schema = ttable.schema
        ttable.schema = None

        query = {}
        for column in columns:
            temporary_column = ttable.columns.get(column)
            if temporary_column is not None:
                query[column] = temporary_column

        query = update(self).values(**query)
        pk = ttable.primary_key.columns.items()
        for column_name, temp_column in pk:
            column = self.columns.get(column_name)
            query = query.where(column == temp_column)
        bind.execute(query)

        ttable.schema = temp_schema

def gen_data_table(table, meta):
    '''Returns a DatabaseTable instance with associated mapping protocol'''
    table = DatabaseTable(table, meta)

    protocol_path = os.path.join(settings.MAPPING_PROTOCOLS_FOLDER, table.name + '.csv')
    if os.path.isfile(protocol_path) and table._protocol is None:
        protocol = Protocol()
        protocol.load_csv(protocol_path)

        table.load_protocol(protocol)

    return table
