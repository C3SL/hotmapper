#!/bin/bash

# ---------------------------------------------------------------------------------------#
# Esse script tem como objetivo facilitar a criação do banco de dados do projeto SIMCAQ,
# conforme a necessidade dos desenvolvedores. O código é livre para modificações contanto
# que os que utilizam o script sejam notificados das mudanças decorrentes.
# ---------------------------------------------------------------------------------------#


# ---------------------------------------------------------------------------------------#
# Função para criar as tabelas que são consideradas bases para o banco de dados
# ---------------------------------------------------------------------------------------#
fBase ()
{
    mclient -d $1 base/regiao.sql
    mclient -d $1 base/estado.sql
    mclient -d $1 base/municipio.sql
    mclient -d $1 base/siope_uf.sql
    mclient -d $1 base/siope_mun.sql
    mclient -d $1 base/siope_mun_seed.sql
    mclient -d $1 base/instituicao_superior.sql
    mclient -d $1 base/formacao_superior.sql
    mclient -d $1 base/formacao_superior_seed.sql
    mclient -d $1 base/ibge_pib.sql
}
# ---------------------------------------------------------------------------------------#

# ---------------------------------------------------------------------------------------#
# Função para criar as tabelas a partir dos protocolos de mapeamento
# ---------------------------------------------------------------------------------------#
fCreate ()
{
    ./manage.py create escola
    ./manage.py create turma
    ./manage.py create matricula
    ./manage.py create docente
}
# ---------------------------------------------------------------------------------------#

# ---------------------------------------------------------------------------------------#
# Função para inserir dados nas tabelas criadas a partir dos protocolos de mapeamento
# ---------------------------------------------------------------------------------------#
fInsert()
{
    local alpha="$2"

    while [ "$alpha" -le "$3" ]; do
        ./manage.py insert $1${alpha}_ESCOLAS.CSV escola $alpha --sep=\|
        ./manage.py insert $1${alpha}_TURMAS.CSV turma $alpha --sep=\|
        ./manage.py insert $1${alpha}_DOCENTES_CO.CSV docente $alpha --sep=\|
        ./manage.py insert $1${alpha}_DOCENTES_NORTE.CSV docente $alpha --sep=\|
        ./manage.py insert $1${alpha}_DOCENTES_NORDESTE.CSV docente $alpha --sep=\|
        ./manage.py insert $1${alpha}_DOCENTES_SUDESTE.CSV docente $alpha --sep=\|
        ./manage.py insert $1${alpha}_DOCENTES_SUL.CSV docente $alpha --sep=\|
        ./manage.py insert $1${alpha}_MATRICULA_CO.CSV matricula $alpha --sep=\|
        ./manage.py insert $1${alpha}_MATRICULA_NORTE.CSV matricula $alpha --sep=\|
        ./manage.py insert $1${alpha}_MATRICULA_NORDESTE.CSV matricula $alpha --sep=\|
        ./manage.py insert $1${alpha}_MATRICULA_SUDESTE.CSV matricula $alpha --sep=\|
        ./manage.py insert $1${alpha}_MATRICULA_SUL.CSV matricula $alpha --sep=\|
        alpha=$(($alpha + 1))
    done
}
# ---------------------------------------------------------------------------------------#

# ---------------------------------------------------------------------------------------#
# Retorna uma ajuda caso não haja parâmetros de entrada
# ---------------------------------------------------------------------------------------#
if [ ! $1 ]; then
    printf "\n# WARNING: Don't forget to check the settings file for the database name.\n"
    printf "\n# This script has 4 commands:\n"
    printf "# 1. all: execute all commands to create the database and insert data.\n"
    printf "# 2. base: execute the commands to create de base tables.\n"
    printf "# 3. create: execute the commands to create the tables.\n"
    printf "# 4. insert: execute the commands to insert data to tables.\n\n"
    printf "# Estructure of commands:\n"
    printf "# 1. ./auto.sh all [database_name] [path_to_files] [initial_year]"
    printf " [final_year]\n"
    printf "# 2. ./auto.sh base [database_name]\n"
    printf "# 3. ./auto.sh create\n"
    printf "# 4. ./auto.sh insert [path_to_files] [initial_year] [final_year]\n\n"
    exit 0;
fi
# ---------------------------------------------------------------------------------------#

# ---------------------------------------------------------------------------------------#
# Execução do script conforme os comandos passados
# ---------------------------------------------------------------------------------------#
source ./env/bin/activate
if [ $? = 0 ]; then
    printf "\n# Environment activated!\n"
    if [ "$1" = 'all' ]; then
        if [ $2 ] && [ $3 ] && [ $4 ] && [ $5 ]; then
            printf "\n# Initializing the creation of base tables (may need database"
            printf " password)...\n"
            sleep 1
            fBase "$2"
            printf "\n# Initializing the creation of mapping tables...\n"
            sleep 1
            fCreate
            printf "\n# Initializing the insertion of data, this may take a while...\n"
            sleep 2
            fInsert "$3" "$4" "$5"
            sleep 1
        else
            printf "# ERROR: Missing parameters!\n"
            exit -1;
        fi
    elif [ "$1" = 'base' ]; then
        if [ $2 ]; then
            printf "\n# Initializing the creation of base tables (may need database"
            printf " password)...\n"
            sleep 1
            fBase "$2"
            sleep 1
        else
            printf "# ERROR: Missing parameters!\n"
            exit -1;
        fi
    elif [ "$1" = 'create' ]; then
        printf "\n# Initializing the creation of tables...\n"
        sleep 1
        fCreate
        sleep 1
    elif [ "$1" = 'insert' ]; then
        if [ $2 ] && [ $3 ] && [ $4 ]; then
            printf "\n# Initializing the insertion of data, this may take a while...\n"
            sleep 2
            fInsert "$2" "$3" "$4"
            sleep 1
        else
            printf "# ERROR: Missing parameters!\n"
            exit -1;
        fi
    else
        printf "\n# ERROR: Missing parameters!\n"
        deactivate
        printf "\n# Environment deactivated!\n"
        printf "# Terminating...\n"
        sleep 1
        exit -1;
    fi
    deactivate
    printf "\n# Environment deactivated!\n"
    printf "\n# All done! Terminating...\n"
    sleep 1
    exit 0;
else
    printf "# ERROR: can't find the directory for environment!\n"
    exit -1;
fi
