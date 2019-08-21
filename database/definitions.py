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

import logging
import os
import json
import jsbeautifier
import settings
from database.base import InvalidTargetError

logger = logging.getLogger(__name__)

standard_keys = {
    'source': 'data_source',
    'description': 'pairing_description',
    'pkcolumns': 'pk',
    'fkcolumns': 'foreign_keys',
    'columns': 'columns'
}

class Definitions(object):
    '''
    Class created from the Table definitions, contains primary key, foreign key, descriptions, source
    and columns
    '''
    def __init__(self, t_name, keys=None):
        self.source = None
        self.description = None
        self.columns = None
        self.pkcolumns = None
        self.fkcolumns = None
        self._name = t_name
        self.load_json(keys)

    def load_json(self, keys=None):
        ''' Read the table definition json into the correct Definitions variables '''
        definitions = self._name + '.json'
        logger.debug("Acquiring definitions from %s", definitions)
        definitions = os.path.join(settings.TABLE_DEFINITIONS_FOLDER, definitions)
        definitions = json.loads(open(definitions).read())
        self.load_from_dict(definitions, keys)

    def update_columns(self, columns):
        ''' Update Table definition json with a new columns dict '''
        definitions_json = self._name + '.json'
        logger.debug("Updating table definitions from %s", definitions_json)
        definitions_json = os.path.join(settings.TABLE_DEFINITIONS_FOLDER, definitions_json)

        self.columns = columns
        new_definitions = self.to_dict()
        new_definitions = jsbeautifier.beautify(json.dumps(new_definitions, ensure_ascii=False))
        with open(definitions_json, "w") as def_json:
            def_json.write(new_definitions)

        logger.debug("Definitions Updated")

    def load_from_dict(self, definitions, keys=None):
        ''' Takes a definitions dictionary and load the object Definitions variables '''
        if not keys:
            keys = standard_keys

        self.source = definitions[keys['source']]
        self.description = definitions[keys['description']]
        self.pkcolumns = definitions[keys['pkcolumns']]
        self.fkcolumns = definitions[keys['fkcolumns']]

        try:
            self.columns = definitions[keys['columns']]
        except KeyError:
            self.columns = None
        logger.debug("Definitions loaded")

    def to_dict(self, keys=None):
        ''' Transforms a Definition object into a dictionary for writing in a json file '''
        if not keys:
            keys = standard_keys

        definitions = {
            keys['description']: self.description,
            keys['source']: self.source,
            keys['pkcolumns']: self.pkcolumns,
            keys['fkcolumns']: self.fkcolumns,
            keys['columns']: self.columns
        }
        return definitions

    def get_targets(self):
        ''' Returns a list containing all columns targets '''
        targets = []
        for column_name, parameter_list in self.columns.items():
            targets.append(parameter_list[1])

        return targets

    def get_dbcolumn_from_target(self, target):
        ''' Gets a database column from a target column name. Ouput is a list
        with the column name and type contents.
        :return: ['column_name','column_type'] '''
        found = False
        for column_name, parameter_list in self.columns.items():
            if parameter_list[1] == target:
                found = True
                return [column_name, parameter_list[0]]

        if not found:
            raise InvalidTargetError(target)
