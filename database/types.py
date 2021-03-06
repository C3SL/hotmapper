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

import re
from sqlalchemy_monetdb.monetdb_types import MONETDB_TYPE_MAP, TINYINT, DOUBLE_PRECISION
from sqlalchemy.ext.compiler import compiles


TYPE_RE = re.compile('[a-z]+')
ARGS_RE = re.compile('\\( *[0-9,.]+ *\\)')

MONETDB_TYPE_MAP['integer'] = MONETDB_TYPE_MAP['int']

@compiles(TINYINT)
def compile_tinyint(element, compiler, **kwargs):
    '''Translation for tinyint - not sure if implemented in sqlalchemy_monetdb
    by default'''
    return 'TINYINT'

@compiles(DOUBLE_PRECISION)
def compile_double(element, compiler, **kwargs):
    '''Translation for double - not sure if implemented in sqlalchemy_monetdb
    by default'''
    return 'DOUBLE'

@compiles(TINYINT, 'mysql')
def compile_tinyint(element, compiler, **kwargs):
    return 'SMALLINT'

@compiles(DOUBLE_PRECISION, 'mysql')
def compile_double(element, compiler, **kwargs):
    '''Translation for double - not sure if implemented in sqlalchemy_monetdb
    by default'''
    return 'FLOAT'

def get_type(in_string):
    '''Returns a remapped type object for a given type string'''
    in_string = in_string.lower()
    in_string = re.sub(' +', ' ', in_string)
    field_type = re.search(TYPE_RE, in_string).group()
    field_type = MONETDB_TYPE_MAP[field_type]
    targs = re.search(ARGS_RE, in_string)
    if targs:
        targs = targs.group()
        targs = targs.strip('()')
        targs = targs.split(',')
        targs = [a.strip() for a in targs]
        args = []
        for arg in targs:
            try:
                arg = int(arg)
            except ValueError:
                pass
            finally:
                args.append(arg)
        field_type = field_type(*args)
    else:
        field_type = field_type()
    return field_type
