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


import sys
import pandas as pd


protocol = pd.read_csv('mapping_protocols/pnad.csv')

dic = pd.read_excel(sys.argv[1])
year = sys.argv[2]

protocol[year] = protocol['2015']
protocol['p0' + year] = ['' for _ in range(len(protocol[year]))]
protocol['pf' + year] = ['' for _ in range(len(protocol[year]))]

dic.columns = list(dic.loc[0])
dic = dic.loc[4:]
dic = dic.fillna('')
dic = dic[dic['Tamanho'] != '']
res_dic = dic[list(dic.columns)[0:3]]
res_dic.index = res_dic['Código de variável']
# print(res_dic)
col_list = list(res_dic.index)

for i, row in protocol.iterrows():
    if row[year] in col_list:
        protocol.loc[i, 'p0' + year] = res_dic.loc[row[year]]['Posição Inicial']
        protocol.loc[i, 'pf' + year] = res_dic.loc[row[year]]['Tamanho']
        # print(res_dic.loc[row[year]]['Posição Inicial'])
        # print(res_dic.loc[row[year]]['Tamanho'])

protocol.to_csv(sys.stdout)
