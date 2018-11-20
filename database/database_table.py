'''This module contains the definition of the DatabaseTable class and a constructor'''
import os
import json
import logging
from sqlalchemy import Table, Column, inspect, Integer, String, Boolean,\
                       PrimaryKeyConstraint, ForeignKeyConstraint, text
from sqlalchemy.sql import select, insert, update, delete

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
                          Column('nullable', Boolean),
                          Column('autoincrement', Boolean),
                          Column('default', String(63)),
                          extend_existing = True)

    return mapping_table

def get_primary_keys(table):
    '''Returns a list of columns corresponding to the primary key of a given table instance'''
    return [c[1] for c in table.primary_key.columns.items()]


def gen_temporary(name, meta, *dbcolumns):
    '''Returns a temporary table mapped by meta, with columns and column types given by dbcolumns'''
    logger.info("Acquiring temporary table with name '%s'", name)
    logger.debug("Temporary table '%s' with list of columns %s", name, dbcolumns)
    ttable = Table(name, meta, prefixes=['TEMPORARY'], schema='tmp')
    for field_name, field_type in dbcolumns:
        column = Column(field_name, get_type(field_type))
        ttable.append_column(column)

    return ttable

def copy_to_temporary(connection, csv_file, ttable, offset=2, sep=';', null=''):
    '''Loads a CSV file into a temporary table for futher use. Must receive an
    active connection. To avoid the issuing of autocommits, start a transaction
    before calling the function.
    Example:

    with engine.connect() as connection:
        trans = connection.begin()
        # Stuff...
        ttable, hdict = database_table.insert_temporary(connection, file_name, '2013', sep='|')
        # Do stuff using the temporary table
        # The commit will close the transaction, freeing the memory occupied by the ttable in
        # the database server
        tarns.commit()

    Returns: A tupple with the temporary table object and the dictionary with information
    about the file header.'''

    logger.info("Copying %s into temporary table %s", csv_file, ttable.name)
    query = "COPY OFFSET {} INTO \"{}\" FROM '{}' USING DELIMITERS '{}','\\n' NULL AS '{}'"\
            .format(offset, ttable.name, csv_file, sep, null)
    connection.execute(query)

    # Workaround TODO error raised by monetdb server when column level >= 3 is used
    ttable.schema = None

    # Due to some reason, select * queries from ttable might result on an error in
    # the pymonetdb module.


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
        super().__init__(*args, **kwargs)
        if self.columns.keys():
            return

        logger.debug("Acquiring info about table %s", self.name)
        insp = inspect(self.metadata.bind)

        if self.exists():
            logger.info("Using existing table %s", self.name)
            for column in insp.get_columns(self.name):
                self.append_column(Column(column['name'], column['type']))
            pks = insp.get_pk_constraint(self.name)
            if 'constrained_columns' in pks.keys():
                pks = [self.columns.get(k) for k in pks['constrained_columns']]
                self.primary_key = PrimaryKeyConstraint(*pks)
        else:
            logger.debug("Table %s not present in database.", self.name)

        self._mapping_table = gen_mapping_table(self)
        self._protocol = None

        if 'protocol' in kwargs.keys():
            self.load_protocol(kwargs['protocol'])

    def get_definitions(self):
        '''Returns a dictionary with definitions from a table definitions file'''
        definitions = self.name + '.json'
        logger.debug("Acquiring definitions from %s", definitions)
        definitions = os.path.join(settings.TABLE_DEFINITIONS_FOLDER, definitions)
        definitions = json.loads(open(definitions).read())
        logger.debug("Definitions loaded")

        return definitions

    def load_protocol(self, protocol):
        '''Takes a Protocol instance and loads it for further use'''
        if not isinstance(protocol, Protocol):
            raise Exception
        self._protocol = protocol

    def get_target(self, name):
        '''Returns a target from a dbcolumn name'''
        return self._protocol.target_from_dbcolumn(name)

    def translate_header(self, header, year):
        '''Receives a list of headers from an original source and translates it to a
        dictionary which maps source columns to destiny columns'''
        output = {}
        for title in header:
            target = self._protocol.target_from_original(title, year)
            if target:
                column_name, column_type, _, _ = \
                    self._protocol.dbcolumn_from_target(target)

            if not (target and column_name.strip() and column_type.strip()):
                column_name = title
                column_type = 'VARCHAR(255)'

            output[title] = {
                "column_name": column_name,
                "column_type": column_type,
            }
        return output

    def create_mapping_table(self):
        '''Creates the mapping table in the database'''
        logger.info("Creating mapping table %s", self._mapping_table.name)
        self._mapping_table.create()

        insp = inspect(self.metadata.bind)
        with self.metadata.bind.connect() as connection:
            logger.info("Populating mapping table")
            for column in insp.get_columns(self.name):
                for key in column:
                    column[key] = str(column[key])
                column['target_name'] = self.get_target(column['name'])
                logger.debug("Mapping column %s with target_name %s",
                             column['name'], column['target_name'])
                query = insert(self._mapping_table).values(**column)
                connection.execute(query)

    def set_source(self):
        '''Inserts or updates table entry in the sources table'''
        definitions = self.get_definitions()
        source_table = gen_source_table(self.metadata)

        # Create source table if doesnt exist
        if not source_table.exists():
            logger.debug("Source table not found. Creating...")
            source_table.create()
            logger.debug("Source table creation: no exceptions.")

        source = definitions['data_source']

        logger.debug("Checking for '%s' in source table", self.name)
        base_select = select([source_table.c.id]).where(source_table.c.table_name == self.name)
        table_id = self.metadata.bind.execute(base_select).fetchone()

        if table_id:
            logger.debug("Table found. Running update query")
            table_id = table_id[0]
            base_query = update(source_table).where(source_table.c.id == table_id)
        else:
            logger.debug("Table not found. Running insert query")
            base_query = insert(source_table)

        base_query = base_query.values(table_name=self.name, source=source)

        self.metadata.bind.execute(base_query)

    def create(self, bind=None, checkfirst=False):
        if self.exists():
            logger.error("Table %s already exists", self.name)
            return
        definitions = self.get_definitions()

        for column in self._protocol.get_targets():
            column = self._protocol.dbcolumn_from_target(column)
            if column[0]:
                column[0] = column[0].strip()
            if not column[0]:
                continue
            column = Column(column[0], get_type(column[1]))
            self.append_column(column)

        primary_key = [self.columns.get(c) for c in definitions['pk']]
        self.constraints.add(PrimaryKeyConstraint(*primary_key))

        for foreign_key in definitions["foreign_keys"]:
            keys = [self.columns.get(c) for c in foreign_key["keys"]]
            ref_table = DatabaseTable(foreign_key["reference_table"], self.metadata)
            fkeys = [ref_table.columns.get(c) for c in foreign_key["reference_columns"]]

            self.constraints.add(ForeignKeyConstraint(keys, fkeys))

        super().create(bind=bind, checkfirst=checkfirst)

        self.set_source()

        self.create_mapping_table()


    def drop(self, bind=None, checkfirst=False):
        if self._mapping_table.exists():
            logger.info("Dropping mapping table %s...", self.name)
            self.metadata.drop_all(tables=[self._mapping_table])
        if not self.exists():
            logger.error("Table %s doesn't exist", self.name)
            return
        super().drop(bind=bind, checkfirst=checkfirst)

    def drop_column(self, connection, name, target=None):
        '''Drops a column given by name using connection. If no transaction control is necessary,
        and engine can be passed instead of a connection'''

        if target is not None and self._mapping_table.exists():
            logger.debug("Deleting target %s from %s", target, self._mapping_table.name)
            query = delete(self._mapping_table)
            query = query.where(self._mapping_table.c.target_name == target)
            connection.execute(query)
        elif target is None:
            logger.warning("Dropping column %d without mapping", name)
        else:
            logger.warning("Table %s has no mpaping", self.name)

        column = self.columns.get(name)
        if column is not None:
            logger.debug("Dropping column %s from %s", name, self.name)
            query = "alter table {} drop column {}".format(self.name, name)
            connection.execute(query)

    def add_column(self, connection, name, field_type, target=None):
        '''Adds a column with name and type using connection. If no transaction control is
        necessary, and engine can be passed instead of a connection'''
        field_type = get_type(field_type)


        if target is not None and self._mapping_table.exists():
            entry = {
                'target_name': target,
                'name': name,
                'type': str(field_type)
            }
            logger.debug("Mapping column %s with type %s. Target: %s", name, str(field_type), target)
            query = insert(self._mapping_table).values(**entry)
            self.metadata.bind.execute(query)

        column = self.columns.get(name)
        if column is None:
            logger.debug("Adding column %s with type %s", name, str(field_type))
            column = Column(name, field_type)
            self.append_column(column)

            query = "alter table {} add column {} {}".format(self.name, name, str(field_type))
            connection.execute(query)
        else:
            logger.warning("Column %s already exists. Won't attempt to create.", name)

    def redefine_column(self, connection, original_name, new_name=None, new_type=None):
        '''Redefines a column to match a new name and a new type. Can be used to change names,
        types or both for a column.'''
        if original_name not in self.columns.keys():
            raise Exception("Column %s doesn't exist in table %s" % (original_name, self.name))
        if not (new_name or new_type):
            return
        if not new_type:
            new_type = str(self.c.get(original_name).type)
        if not new_name:
            new_name = original_name

        self.drop_column(connection, original_name)
        self.add_column(connection, new_name, new_type)

        field_type = get_type(new_type.lower())

        self.columns.replace(Column(original_name, field_type, key=new_name))
        column = self.columns.get(new_name)
        column.name = new_name
        column.type = field_type

        query = update(self._mapping_table).values(name=new_name).\
                    where(self._mapping_table.c.name == original_name)
        connection.execute(query)

    def transfer_data(self, transfer_list):
        '''Receives a list of columns to be transfered. Transfered columns are backed up,
        removed, added with new parameters and then repopulated.
        transfer_list must be a list of dictionaries with the following fields:
        name - the name of the original column;
        new_name - name for the new column. If None is passed, original name is used;
        new_type - type for the new column. If None is passed, original type is used.'''
        if not transfer_list:
            return
        temp_pk_columns = [c.copy() for c in list(self.primary_key.columns)]

        original_columns = [a['name'] for a in transfer_list]
        original_columns = [self.columns.get(name) for name in original_columns]
        temp_columns = [Column(c['new_name'], get_type(c['new_type'])) for c in transfer_list]

        with self.metadata.bind.connect() as connection:
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
        protocol_target_list = self._protocol.get_targets()

        query = self._mapping_table.select()
        results = self.metadata.bind.execute(query).fetchall()
        db_target_list = [t[1] for t in results]

        new_columns = [c for c in protocol_target_list if c not in db_target_list]
        to_drop_columns = [c for c in db_target_list if c not in protocol_target_list]

        update_columns = []
        for target in protocol_target_list:
            query = select([self._mapping_table.c.name, self._mapping_table.c.type])\
                   .where(self._mapping_table.c.target_name == target)
            result = self.metadata.bind.execute(query).fetchone()
            if not result:
                continue
            name, field_type = result
            new_name, new_type, _, _ = self._protocol.dbcolumn_from_target(target)
            new_type = str(get_type(new_type))
            if name == new_name and field_type == new_type:
                continue
            update_columns.append({
                "name": name,
                "new_name": new_name,
                "new_type": new_type
            })

        return new_columns, to_drop_columns, update_columns

    def remap(self):
        '''Checks mapping protocol for differences in table structure - then
        attempts to apply differences according to what is recorded in the
        mapping table'''
        if not self.exists():
            print("Table {} doesn't exist".format(self.name))
            return
        mtable = self._mapping_table

        if not mtable.exists():
            print("Mapping table for {} not found.".format(self.name))
            print("Creating mapping table...")
            self.create_mapping_table()
            print("Done!")

        self.set_source()

        new_columns, to_drop_columns, update_columns = self.compare_mapping()

        # Create new columns
        for column in new_columns:
            dbcolumn = self._protocol.dbcolumn_from_target(column)
            if not dbcolumn[0]:
                continue

            self.add_column(self.metadata.bind.connect(), dbcolumn[0], dbcolumn[1], column)

        # Drop columns
        for column in to_drop_columns:
            column_name = select([mtable.c.name]).where(mtable.c.target_name == column)
            column_name = self.metadata.bind.execute(column_name).fetchone()[0]
            if not column_name:
                continue

            self.drop_column(self.metadata.bind.connect(), column_name, column)

        # Update existing columns

        self.transfer_data(update_columns)

    def treat_derivative(self, target, year):
        '''Receives a dictionary of headers and includes the derivative expressions from the
        mapping protocol'''
        original = self._protocol.original_from_target(target, year)
        try:
            original = original.strip()
        except AttributeError:
            original = ''
        if not original or not original[0] == '~':
            return None
        original = original.strip('~ ')

        logger.debug("Found derivative for target %s: %s", target, original)

        return text(original)

    def set_temporary_primary_keys(self, ttable, year=None):
        '''Matches temporary table primary key with self'''
        pks = get_primary_keys(self)
        tpks = []
        if year is None:
            for primary_key in pks:
                tpks.append(ttable.columns.get(primary_key.name))
        else:
            for primary_key in pks:
                target = self._protocol.target_from_dbcolumn(primary_key.name)
                name = self._protocol.original_from_target(target, year)
                tpk = ttable.columns.get(name)
                tpks.append(tpk)

        ttable.constraints.add(PrimaryKeyConstraint(*tpks))

    def mount_original_columns(self, header, year):
        '''Receives a list of header names and returns list of touples with name, type for
        each field'''
        header_dict = self.translate_header(header, year)

        columns = []
        for entry in header:
            columns.append((entry.strip(' \n\t'), header_dict[entry]['column_type'].strip(' \n\t')))

        return columns

    def insert_from_temporary(self, connection, ttable, year):
        '''Inserts contents from the temporary table into self'''
        query_dst = []
        query_src = []

        logger.debug("Checking temporary table %s to start insertion into %s",
                     ttable.name, self.name)
        logger.debug("Analysing columns with mapping protocol...")
        for column in ttable.columns.items():
            column = column[1]
            logger.debug("Checking column %s", column)
            target = self._protocol.target_from_original(column.name, year)
            if target is None:
                logger.debug("Column has no related target")
                continue
            logger.debug("Found target %s", target)
            dst, _, _, _ = self._protocol.dbcolumn_from_target(target)
            try:
                dst = dst.strip()
            except AttributeError:
                dst = ''
            if not dst:
                logger.debug("No dbcolumn related to target %s", target)
                continue

            logger.debug("Temporary column %s mapped to column %s", column, dst)
            query_dst.append(self.columns.get(dst))
            query_src.append(column)

        # Derivative fields
        logger.debug("Checking protocol for derivative fields")
        for target in self._protocol.get_targets():
            derivative = self.treat_derivative(target, year)
            if derivative is not None:
                dst, _, _, _ = self._protocol.dbcolumn_from_target(target)
                logger.debug("Mapped derivative to field %s", dst)
                query_dst.append(dst)
                query_src.append(derivative)

        logger.info("Inserting data into %s from temporary table %s", self.name, ttable.name)

        query_src = select(query_src)
        query = insert(self).from_select(query_dst, query_src)

        connection.execute(query)

    def columns_from_targets(self, target_list):
        '''Receives a list of targets and returns a list of column names associated with them'''
        if target_list is None:
            target_list = []
        columns = []
        for target in target_list:
            column_name, _, _, standard = self._protocol.dbcolumn_from_target(target)
            if not standard:
                raise Exception
            columns.append(column_name)
        return columns

    def update_from_temporary(self, connection, ttable, year, columns=None):
        '''Updates a table with information from a given file. This function should be used
        whenever source files are updated from corrections or unused columns from source files
        start being needed.
        It's important that the database is up to date with the mapping protocols before this
        function is called. To ensure so, run a self.remap call before.
        Input:
            - csv_file: file name to be used as source. Must be absolute path for most database
                servers;
            - year: string with the temporal variable used;
            - columns: a list with names for dbcolumns to be updated. Use this parameter when you
                to use the names existing in the database.
            - target_list: a list with target names (like 'CEBTU049N0'). Use this parameter to use
                target names instead of dbcolumn names.
            - offset, sep and null are supplied to the insert_temporary call issued during this
                method execution.

        Important note: if neither columns or target_list are supplied, the function will return
        without issuing changes to the database.'''
        if not columns:
            logger.error("You must provide a column list")
            return

        logger.debug("Analysing temporary table %s to update table %s", ttable.name, self.name)

        query = {}
        for dbcolumn in columns:
            logger.debug("Consulting column %s in mapping protocol", dbcolumn)
            target = self._protocol.target_from_dbcolumn(dbcolumn)
            if not target:
                logger.error(("Unknown column %s. Verify your typing or "
                       "check the relevant mapping protocol."), dbcolumn)
                return
            column = self._protocol.original_from_target(target, year)
            column = ttable.columns.get(column)
            if column is None:
                column = self.treat_derivative(target, year)
            if column is None:
                logger.warning("Column %s no mapped for these parameters. Skipping", dbcolumn)
                continue

            query[dbcolumn] = column

        pks = get_primary_keys(self)
        pkst = get_primary_keys(ttable)

        conditions = [opk == tpk for opk, tpk in zip(pks, pkst)]
        query = update(self).values(**query)
        for condition in conditions:
            query = query.where(condition)
        query = query.where("ano_censo={}".format(year))
        logger.debug("Updating table")
        connection.execute(query)


def gen_data_table(table, meta):
    '''Returns a DatabaseTable instance with associated mapping protocol'''
    protocol_path = os.path.join(settings.MAPPING_PROTOCOLS_FOLDER, table + '.csv')
    protocol = Protocol()
    protocol.load_csv(protocol_path)

    table = DatabaseTable(table, meta)
    table.load_protocol(protocol)

    return table
