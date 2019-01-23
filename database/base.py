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

'''Module containing base declarations'''


class DatabaseError(Exception):
    '''Base class for errors in database manipulation'''
    pass

class DatabaseColumnError(DatabaseError):
    '''This exception should be raised if the program tries to access a columns
       that doesn't belong to a table object'''
    def __init__(self, column_name):
        self.column_name = self.message = column_name
        super().__init__(column_name)

class DatabaseMappingError(DatabaseError):
    '''This exception should be raised if some table mapping can't be done'''
    pass

class MissingProtocolError(DatabaseError):
    '''This exception should be raised if the program tries to use methods that
       requires a protocol while there is none loaded'''
    pass

class MissingForeignKeyError(DatabaseError):
    '''This exception should be raised if an expected foreign key is not found.'''
    def __init__(self, referred_table=None):
        self.referred_table = referred_table
        super().__init__(referred_table)

class MissingTableError(DatabaseError):
    '''This exception should be raised if an expected table doesn't exist.'''
    def __init__(self, table=None):
        self.table = table
        super().__init__(table)

class ProtocolError(Exception):
    '''Base class for errors in protocols'''
    pass

class InvalidTargetError(ProtocolError):
    '''This exception should be raised if calls to a protocol require an invalid
       target - either wrong syntax or non existing target'''
    def __init__(self, target_name):
        self.target = self.message = target_name
        super().__init__(target_name)

class DuplicateColumnNameError(ProtocolError):
    '''This exception should be raised if a column name is repeated throughout
       the protocol'''
    def __init__(self, column_name):
        self.name = self.message = column_name
        super().__init__(column_name)

class CircularReferenceError(ProtocolError):
    '''
    This exception should be raised if a derivative variable or group of variables
    imply on a circular dependency tree.
    '''
    def __init__(self, target_name):
        self.target_name = target_name
        super().__init__(target_name)
