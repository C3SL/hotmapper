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
