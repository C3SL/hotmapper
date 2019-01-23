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

'''Defines functions to build the pairing reports, without concern for specific type output'''
import os
import numpy as np
import pandas as pd

from database.protocol import Protocol
import settings


VALUES = 'valores'
VALUES_DESCRIPTION = 'descrição'
VARIABLE_DESCRIPTION = 'Descrição'

def get_from_dict_file(file, variable):
    '''Gets dictionary information about a variable'''
    dict_file = pd.read_excel(file)
    dict_file = dict_file.fillna('')
    columns = [(dict_file[c][9] or dict_file[c][8]) for c in dict_file]
    dict_file.columns = columns
    dict_file = dict_file.drop(range(10))
    dict_file = dict_file.reset_index(drop=True)

    variables = [c for c in columns if 'variavel' in c.lower() or 'variável' in c.lower()]
    lde_name = [c for c in variables if 'lde' in c.lower()][0]

    found = False
    for line, row in dict_file.iterrows():
        if row[lde_name].strip() == variable:
            start_line = line
            found = True
            break
    if not found:
        return None
    line_count = 1
    while (start_line + line_count) < len(dict_file) and\
          (not dict_file.iloc[start_line + line_count][lde_name]) and\
          dict_file.iloc[start_line + line_count][VALUES]:
        line_count += 1
    line_range = [l+start_line for l in range(line_count)]
    return dict_file.iloc[line_range]

def get_from_attch_file(table_name,year_list,variable,protocol,attachments):
    '''Gets attachments information about a variable in attachments according to attachment year and year_list'''
    attach_dict = {
        'turma': 'Tabela de Turma',
        'matricula': 'Tabela de Matrícula',
        'escola': 'Tabela de Escola',
        'docente': 'Tabela de Docente',
    }
    attachs = pd.DataFrame()
    for attachment in attachments:
            attach_file_location = os.path.join(settings.DICTIONARY_FOLDER, attachment)
            year = attachment[6:10]
            skip = 0
            if int(year) in year_list:
                original_cod = protocol.original_from_target(variable,year)
                attach_file = pd.read_excel(attach_file_location,None)
                for a in attach_file[attach_dict[table_name]].iterrows():
                    skip=skip+1
                    if a[0] == 'N':
                        break
                attach_table = pd.read_excel(attach_file_location,skiprows=skip,sheetname=attach_dict[table_name])
                found = False
                for line, row in attach_table[attach_table['Nome da Variável'] == original_cod].iterrows():
                    start_line = line
                    found = True
                    break
                if not found:
                    attach = ''
                    continue
                attach = attach_table['N']
                line_count = 1
                while attach.iloc[start_line + line_count] is np.nan:
                    line_count += 1
                line_range = [l+start_line for l in range(line_count)]
                attach = attach_table.iloc[line_range]['Categoria'].reset_index(drop=True)
                if pd.isnull(attach).any() or attach.empty or len(attach)==0:
                    attach = ''
                elif attach[0].find('\n') != -1:
                     attach = attach[0].split('\n')
                     attach = pd.DataFrame(attach)
                     attach.index = np.arange(1,len(attach)+1)
                     attach.columns = ['Descricao_'+year]
                     attachs = pd.concat([attachs,attach],axis=1)
                else:
                     attach = pd.DataFrame(attach)
                     attach.index = np.arange(1,len(attach)+1)
                     attach.columns = ['Descricao_'+year]
                     attachs = pd.concat([attachs,attach],axis=1)
    return attachs

def get_year_list(table_name, engine):
    '''Builds the year list from a table using the given engine'''
    response = engine.execute('select distinct ano_censo from {} order by ano_censo'.\
                              format(table_name))
    return [r[0] for r in response.fetchall()]

def get_variable_values(table_name, variable_name, year, engine):
    '''Builds a list with all possible values for a variable from a table using a given engine,
    using only results for the given year. Values are ordered in the database query'''
    response = engine.execute('select distinct {0} from {1} where ano_censo={2} order by {0}'.\
        format(variable_name, table_name, year))
    return [r[0] for r in response.fetchall() if not (r[0]== '' or r[0] is None)]

def handle_table_field(table_name, field, engine, year_list):
    '''Checks tables for pairing information'''
    variable_content = pd.DataFrame([])
    for i in range(len(year_list)):
        year = year_list[i]
        value1 = get_variable_values(table_name, field, year, engine)
        content1 = pd.DataFrame(value1, columns=[year],index=value1)
        variable_content = pd.concat([variable_content,content1], axis=1)
    return variable_content

def assemble_variable_content(table_name, protocol, variable, engine, year_list,attachments):
    '''Builds the variable contents to populate the pairing report for a given variable from
    a given table using the given engine'''
    field_name, data_type, comment, _ = protocol.dbcolumn_from_target(variable)
    if not field_name:
        return None
    field_name = field_name.strip()
    if not field_name:
        return None
    variable_content = handle_table_field(table_name, field_name, engine, year_list)

    dict_file_location = os.path.join(settings.DICTIONARY_FOLDER, table_name + '.xls')

    variable_dict = get_from_dict_file(dict_file_location, variable)
    if variable_dict is not None:
        variable_attachments = get_from_attch_file(table_name,year_list,variable,protocol,attachments)
        variable_dict.index = variable_dict[VALUES]
        variable_content = pd.concat([variable_dict[[VALUES, VALUES_DESCRIPTION]],
                                      variable_content], axis=1)
    else:
        variable_attachments = pd.DataFrame([])
    variable_content = variable_content.reset_index(drop=True)
    original_names_line = ['', '']
    for year in iter(variable_content.columns[2:]):
        original_names_line.append(protocol.original_from_target(variable, str(year)))
    original_names_line = pd.DataFrame(original_names_line, index=variable_content.columns)
    original_names_line = original_names_line.transpose()
    variable_content = pd.concat([original_names_line, variable_content])
    variable_content = variable_content.reset_index(drop=True)
    contents = [variable, field_name, comment, data_type]
    contents = pd.DataFrame(contents,index=['Variável','Nome no Banco','Comentário','Tipo'])
    contents = contents.transpose()
    contents = pd.concat([contents, variable_content,variable_attachments], axis=1)
    return contents

def output_per_variable(table_name, variables, engine, attachments):
    '''Yields the contents for a given variable in a pandas DataFrame. Can be used to iterate a
    variable list (variables) and get formated output for report'''
    print(table_name)
    protocol_file = os.path.join(settings.MAPPING_PROTOCOLS_FOLDER, table_name + '.csv')
    protocol = Protocol()
    protocol.load_csv(protocol_file)
    year_list = get_year_list(table_name, engine)
    for variable in variables:
        contents = assemble_variable_content(table_name, protocol, variable, engine, year_list, attachments)
        if contents is not None:
            yield contents

def walk_tables(engine):
    '''Uses given engine to search for tables that have both a mapping protocol and a dicionary
    in the folders listed in settings.py, then iterates over them to output master DataFrames
    for each of those tables'''
    variables = open(settings.PAIRING_VARIABLE_FILE).read()
    variables = [v for v in variables.split('\n') if v]
    protocols = [f for f in os.listdir(settings.MAPPING_PROTOCOLS_FOLDER) if
                 f.lower().endswith('.csv')]
    dictionaries = [f for f in os.listdir(settings.DICTIONARY_FOLDER) if f.lower().endswith('.xls')]
    attachments = [f for f in os.listdir(settings.DICTIONARY_FOLDER) if f.lower().endswith('.xlsx')]
    for protocol in protocols:
        table_name = os.path.splitext(protocol)[0]
        dictionary = table_name + '.xls'
        if dictionary in dictionaries:
            output_table = pd.DataFrame()
            variable_sizes = []
            for variable_table in output_per_variable(table_name, variables, engine, attachments):
                output_table = pd.concat([output_table, variable_table])
                variable_sizes.append(len(variable_table))
            yield [table_name, output_table, variable_sizes]
