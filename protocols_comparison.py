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

import pandas as pd
import os

from database.protocol import Protocol


full_frame = pd.DataFrame([])

file_list = [f for f in os.listdir('./mapping_protocols') if os.path.isfile(os.path.join('./mapping_protocols', f))]

for f in file_list:
    if not f.endswith('.csv'):
        continue
    p = Protocol()
    p.load_csv(os.path.join('./mapping_protocols', f))

    df = p._dataframe

    df = df[df['2015'] != '']
    df = df.set_index(df['2015'])
    df = df['Var.Lab']
    df.name = f
    df = df.to_frame()
    full_frame = pd.concat([full_frame, df], axis=1)

full_frame['unique'] = pd.Series(index=full_frame.index)

for i, row in full_frame.iterrows():
    uniques = len(row.dropna().unique())
    if uniques > 1:
        full_frame['unique'][i] = False
    else:
        full_frame['unique'][i] = True

full_frame = full_frame[full_frame['unique'] == False]
open('output.csv', 'w').write(full_frame.to_csv())
