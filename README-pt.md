# Administrador de base de dados SimCAQ/SMPPIR #

Esse repositório implementa a classe DatabaseTable e funções para verificar pareamento entre
diferentes anos inseridos no banco de dados. A ferramenta é desenvolvida em Python 3, e usa
como base arquivos de mapeamento em formato CSV.

Para a utilização a partir da linha de comando, a CLI manage.py pode ser utilizada sem
que se invoque manualmente as funções a partir da linha de comando Python.

## Requisitos ##

O utilitário foi desenvolvido em Python 3 usando a biblioteca SQLAlchemy com vistas ao banco
de dados MonetDB. Versões futuras podem ter modificações visando a compatibilidade com outros
bancos de dados, aproveitando as capacidades da biblioteca base.

Para a instalação dos requisitos conforme usados durante o desenvolvimento, o arquivo
requirements.txt pode ser usado como base (Recomenda-se o uso de um ambiente virtual).

```bash
(env) $ pip install -r requirements.txt
```

A CLI depende do módulo manage.py. Demais dependências serão listadas a seguir.

### Requisitos para a interface com a base de dados ###

* pymonetdb
* SQLAlchemy
* sqlalchemy-monetdb

### Requisitos para geração de pareamentos ###

* numpy
* pandas
* xlrd
* XlsxWriter

## Interface de linha de comando ##

A invocação da CLI utiliza o padrão do pacote manage.py, que é:

```bash
$ python manage.py [commando] [argumentos posicionais] [argumentos opcionais com valor]
```

Os comandos já implementados são:

* create: Cria a tabela conforme definido no protocolo de mapeamento.

```bash
$ python manage.py create <nome da tabela>
```

O único argumento usado é o nome da tabela. O script procurará por um protocolo de
mapeamento com o mesmo nome para a busca do esquema das colunas.

* insert: insere um arquivo de dados em formato CSV ou similar em uma tabela existente.

```bash
$ python manage.py insert <caminho para o arquivo> <nome da tabela> <ano> [--sep separador] [--null valor_nulo]
```

O caminho para o arquivo deve ser absoluto. A tabela utilizada deve existir e estar
sincronizada com o protocolo de mapeamento correspondente. O separador padrão utilizado
é ponto e vírgula (';'); caso outros separadores sejam utilizados pelo arquivo fonte,
devem ser especificados com --sep (por exemplo --sep \\| para pipe). O valor nulo padrão
é string vazia. Caso outro valor seja usado, deve ser especificado com --null.

* drop: derruba uma tabela do banco de dados.

```bash
$ python manage.py drop <nome da tabela>
```

O comando não contorna chaves estrangeiras que apontem para a tabela, e o banco de dados
pode retornar um erro caso exista alguma.

* remap: sincroniza uma tabela com o protocolo de mapeamento.

```bash
$ python manage.py remap <nome da tabela>
```

Esse comando deve ser utilizado sempre que um protocolo de mapeamento for atualizado.

O remapeamento permite a criação de novas colunas, derrubada de colunas existentes,
renomeamento de colunas e mudança de tipo. Dependendo do tamanho da tabela, o uso de
memória primária pode ser intenso.

* generate_pairing_report: gera relatórios de pareamento para comparação de dados ano
a ano.

```bash
$ python manage.py generate_pairing_report [--output xlsx|csv]
```

Os relatórios são criados na pasta pairing. Caso o formato não seja especificado,
csv será utilizado (um arquivo será criado para cada tabela). Caso xlsx seja o formato
utilizado, um arquivo será criado com todas as tabelas separadas em diferentes planilhas.

* generate_backup: Cria/Atualiza o arquivo monitorado para o backup.

```bash
$ python manage.py generate_backup
```

O arquivo é criado ou atualizado na máquina onde o banco de dados da produção está,
o procedimento de backup da equipe de infraestrutura o monitora para realizar o procedimento.