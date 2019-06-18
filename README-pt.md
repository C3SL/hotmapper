# HOTMapper #

Este repositório contem a ferramenta HOTMapper, o qual permite o usuário gerenciar seus dados históricos usando protocolos de mapeamento. 

## Dados ##

Os dados abertos extraídos e processados pela ferramenta podem ser encontrados no link [INEP](http://portal.inep.gov.br/web/guest/microdados) na seção "Censo Escolar" e " Censo da Educação Superior".

Para facilitar a execução da ferramente, nós baixamos todos os dados de "Local Oferta" no diretório `open_data`. Desta forma não é necessário procurar os dados originais.

**NOTA**: É importante verificar se existem uma coluna identificando o ano do conjunto de dados

## Requisitos ##

* Python 3 (É recomendado o uso de um ambiente virtual, como o virtualenv)
* MonetDB (Nós temos planos de expandir o suporte de bancos de dados que o HOTMapper suporta no futuro)

## Installação ##

----
**IMPORTANTE:**
Nós assumimos queo Python 3.X está instalado na máquina que executará o HOTMapper e que todos os comandos a seguir que utilizam Python serão executados com o Python 3.x.
----


1) Instale o virtualenv

1a) No Linux/macOS

```bash
$ sudo -H pip install virtualenv
```

1b) No Windows (with administrator privilleges)

```cmd
$ pip install virtualenv
```


2) Clone este repositório
```bash
$ git clone git@gitlab.c3sl.ufpr.br:tools/hotmapper.git
```

ou

```bash
$ git clone https://github.com/C3SL/hotmapper.git
```

3) Acesse o repositório

```bash
$ cd hotmapper
```

4) Crie um ambiente virtual
 
```bash
$ virtualenv env
```

5) Inicie o ambiente virtual

5a) No Linux/macOS

```bash
$ source env/bin/activate
```

5b) No Windows (com privilégios de administrador)

```cmd
$ .\env\Scripts/activate
```

6) Instale as dependências
 
```bash
$ pip install -r requirements.txt
```

## Interface de Linha de Comando (CLI) ##


A interface de linha de comando (CLI) permite a ações fornecidas pelo manage.py. Para utilizar a CLI utiliza o seguinte formato padrão:

```bash
$ python manage.py [COMANDO] [ARGUMENTOS POSICIONAIS] [ARGUMENTOS OPCIONAIS]
```

Onde comando pode ser:

* create: Cria uma tabela usando o protocolo de mapeamento.

```bash
$ python manage.py create <nome_da_tabela>
```

**IMPORTANTE:** O HOTMapper usará o nome do protocolo como o nome da tabela


* insert: Insere um arquivo CSV em uma tabela existente.

```bash
$ python manage.py insert <caminho/completo/para/o/arquivo> <nome_da_tabela> <ano> [--sep separador] [--null valor_null]
```

```
<caminho/completo/para/o/arquivo> : O caminho absoluto para o arquivo

<nome_da_tabela>: O nome da tabela onde o arquivo será inserido

<ano>: A coluna do protocolo de mapeamento que o HOTMapper deve usar para inserir os dados

[--sep separador]: O separador personalizado do CSV. Para mudar, você deve substituir 'separador' com o separador que seu arquivo usa.

[--null valor_null]: Define o que substituirá o valor nulo. Substitua 'valor_nulo' com o que quiser que seja o valor nulo

```

* drop: Apaga uma tabela do banco de dados

```bash
$ python manage.py drop <nome_da_tabela>
```

**IMPORTANTE:** O comando não gerencia chaves estrangeiras que apontam para a tabela que está sendo excluída.

* remap: sincroniza a tabela com os mapeamentos

```bash
$ python manage.py remap <nome_da_tabela>
```
Este comando deve ser executado toda vez que a definição dos mapeamentos são atualizadas.

O rema permite a criação de novas colunas, a exclusão de colunas existentes, a renomeação de columnas e a modificação de tipo das colunas. Preste atenção que quanto maior a tabela sendo atualizada, maior o uso de memória RAM.

* update_from_file: Atualiza os dados em um tabela

```bash
$ python manage.py update_from_file <arquivo_csv> <nome_da_tabela> <ano> [--columns="column_name1","column_name2"] [--sep=separador]
```

* generate_pairing_report: gera relatórios para comparar os dados de diferentes anos.

```bash
$ python manage.py generate_pairing_report [--output xlsx|csv]
```

Os relatórios serão criados no diretório "pairing"


* generate_backup: Cria/Atualiza um arquivo de backup da base de dados.

```bash
$ python manage.py generate_backup
```

## Cenários demonstrativos ##

Nesta Seção nós explicaremos como executar os cenários demonstrativos que foram enviados para a conferência EDBT 2019. No cenário 1 será utilizado o conjunto de dados de "local oferta", o qual está incluído no diretório `open_data`. O cenário 2 utiliza o conjunto de dados "matrícula", o qual pode ser baixado do [Link do INEP](http://portal.inep.gov.br/web/guest/microdados) na seção "Censo Escolar".

Em ambos os cenários nós assumimos que você iniciou o ambiente virtual como explicado na Seção `Instalação - 5`;

### Cenário 1 ###

Esta Seção contem os comandos usados no cenário 1, os quais criam uma tabela e adicionam os dados correspondentes.


1) Primeiro nós precisamos criar a tabela no banco de dados. Para fazer isso execute o seguinte comando:
```bash
$ ./manage.py create localoferta_ens_superior
```

2) Agora, que nós já temos o protocolo de mapeamento, nós precisamos inserir os dados abertos no banco de dados. Para fazer isso nós precisamos executar os seguintes comandos:

**IMPORTANTE:** CAMINHO_DO_ARQUIVO é o **_caminho completo_** para o diretório que o dado aberto está localizado, por exemplo (em um ambiente Linux): `/home/c3sl/HOTMapper/open_data/DM_LOCAL_OFERTA_2010.CSV`


a) Para inserir 2010:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/DM_LOCAL_OFERTA_2010.CSV localoferta_ens_superior 2010 --sep="|" 
```

b) Para inserir 2011:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/DM_LOCAL_OFERTA_2011.CSV localoferta_ens_superior 2011 --sep="|" 
```

c) Para inserir 2012:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/DM_LOCAL_OFERTA_2012.CSV localoferta_ens_superior 2012 --sep="|" 
```

d) Para inserir 2013:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/DM_LOCAL_OFERTA_2013.CSV localoferta_ens_superior 2013 --sep="|" 
```

e) Para inserir 2014:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/DM_LOCAL_OFERTA_2014.CSV localoferta_ens_superior 2014 --sep="|" 
```

f) Para inserir 2015:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/DM_LOCAL_OFERTA_2015.CSV localoferta_ens_superior 2015 --sep="|" 
```

g) Para inserir 2016:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/DM_LOCAL_OFERTA_2016.CSV localoferta_ens_superior 2016 --sep="|" 
```

### Cenário 2 ###

Esta Seção contem os comandos usados no cenário 2, os quais são uma atualização de uma tabela.


1) Primeiro nós precisamos criar a tabela no banco de dados. Para fazer isso execute o seguinte comando:
```bash
$ ./manage.py create matricula
```

2) Agora, que nós já temos o protocolo de mapeamento, nós precisamos inserir os dados abertos no banco de dados. Para fazer isso nós precisamos executar os seguintes comandos:

**IMPORTANTE:** CAMINHO_DO_ARQUIVO é o **_caminho completo_** para o diretório que o dado aberto está localizado, por exemplo (em um ambiente Linux): `/home/c3sl/HOTMapper/open_data/MATRICULA_2013.CSV`

a) Para inserir 2013:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/MATRICULA_2013.CSV matricula 2013 --sep="|" 
```

b) Para inserir 2014:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/MATRICULA_2014.CSV matricula 2014 --sep="|" 
```

c) Para inserir 2015:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/MATRICULA_2015.CSV matricula 2015 --sep="|" 
```

d) Para inserir 2016:
```bash
$ ./manage.py insert CAMINHO_DO_ARQUIVO/MATRICULA_2016.CSV matricula 2016 --sep="|" 
```

3) Mude o protocolo de mapeamento de matrícula. Você pode usar o protocolo `matricula_remap.csv` ( Para fazer isso, renomeie o atual `matricula.csv` para qualquer outra coisa e o `matricula_remap.csv` para `matricula.csv`). Neste caso, a única coluna que mudará é a "profissionalizante", porque agora, ao invés de `ELSE returns 0` ela retorna `9`. 

4) Rode o comando remap

```bash
$ ./manage.py remap matricula
```

O comando acima atualizará a tabela `Fonte` e o esquema da tabela `matricula`

5) Atualize a tabela 

```bash
$ ./manage.py update_from_file CAMINHO_DO_ARQUIVO/MATRICULA_2013.CSV matricula 2013 --columns="profissionalizante" --sep="|"
```

O comando acima atualizará os dados na tabela `matricula`.