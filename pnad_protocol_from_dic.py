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
