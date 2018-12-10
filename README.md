# HOTMapper #

This respository contains the HOTMapper tool, a tool that allows the user to manage his historical data using a mapping protocol. This specific version is freezed for demonstration purposes for the EDBT 2019 conference. 

## Table of content ##

 [HOTMapper](#hotmapper)
 - [Data](#data)
 - [Requirements](#requirements)
 - [Installation](#installation)
 - [Command Line Interface](#command-line-interface)
 - [Demo scenarios](#demo-scenarios)
     - [Demo scenario 1](#demo-scenario-1)
     - [Demo scenario 2](#demo-scenario-2)

## Data ##

The Open Data sources extracted and processed by the tool can be found at the link: [INEP](http://portal.inep.gov.br/web/guest/microdados) in the section "Censo Escolar" and "Censo da Educação Superior".

To make it easier to execute the tool, we have dowloaded all data from "Local Oferta" is in the directory open_data. This way it is not necessary to search for the original sources.

**NOTE**: It's important to verify if there is a column identifying the year of the dataset;

## Requirements ##

* Python 3 (It's recommended to use a virtual environment, such as virtualenv)
* MonetDB (We plan to make other databases to work with HOTMapper in the future)

## Installation ##

----
**NOTICE:**
We assume thatPython 3.x is installed in the local computer and that all the following commands that use Python will use Python 3.x.
--

1) Install virtualenv

1a) On Linux/macOS

```bash
$ sudo -H pip install virtualenv
```

1b) On Windows (with administrator privilleges)

```cmd
$ pip install virtualenv
```


2) Clone this repository
```bash
$ git clone git@gitlab.c3sl.ufpr.br:tools/hotmapper.git
```

3) Go to the repository

```bash
$ cd hotmapper
```

4) Create a virtual environment
 
```bash
$ virtualenv env
```

5) Start the virtual environment

5a) On Linux/macOS

```bash
$ source env/bin/activate
```

5b) On Windows (with administrator privilleges)

```cmd
$ .\env\Scripts/activate
```

6) Install dependencies
 
```bash
$ pip install -r requirements.txt
```

## Command Line Interface ##

The CLI (Command Line Interface) uses the standard actions provided by manage.py, which means that to invoke a command it follows the following patterns:

```bash
$ python manage.py [COMMAND] [POSITIONAL ARGUMENTS] [OPTIONAL ARGUMENTS]
```

Where COMMAND can be:

* create: Creates a table using the mapping protocol.

```bash
$ python manage.py create <table_name>
```

**Notice** that the HOTMapper will use the name of the protocol as the name of the table.


* insert: Inserts a CSV file in an existing table.

```bash
$ python manage.py insert <full/path/for/the/file> <table_name> <year> [--sep separator] [--null null_value]
```

```
<full/path/for/the/file> : The absolute file path

<table_name>: The name of the table where the file will be inserted

<year>: The column of the mapping protocol that the HOTMapper should use to insert data

[--sep separator]: The custom separtor of the CSV. To change it you should just replace 'separator' with the token your file uses

[--null null_value]: Define what will replace the null value. Replace the 'null_value' with what you wish to do.

```



* drop: Delete a table from the database

```bash
$ python manage.py drop <table_name>
```

**NOTICE:** The command does not handle foreign keys that points to the table that are being deleted.

* remap: syncronizes a table with the mapping definition.

```bash
$ python manage.py remap <table_name>
```
This command should be run everytime a mapping definition is updated.

The remap allows the creation of new columns, the exclusion of existing columns, the renaming of columns and the modification of the type of columns. Be aware that the bigger the table the bigger the useage of RAM memory.

* update_from_file: Updates the data in the table

```bash
$ python manage.py update_from_file <csv_file> <table_name> <year> [--columns="column_name1","column_name2"] [--sep=separator]
```

* generate_pairing_report: generates reports to compare data from diferent years.

```bash
$ python manage.py generate_pairing_report [--output xlsx|csv]
```

The reports will be created in the folder "pairing" 


* generate_backup: Create/Update a file to backup the database.

```bash
$ python manage.py generate_backup
```
## Demo scenarios ##

In this Section we will explain how to execute the demo scenarios that were submitted to EDBT 2019. Demo scenario 1 uses the dataset "local oferta", which is included in the directory open_data. Demo scenario 2 uses the dataset "matricula" which can be downloaded from the [INEP's Link ](http://portal.inep.gov.br/web/guest/microdados) in the section "Censo Escolar".

In both scnearios, we assume that you started the virtual environment as explained in Section `Installation - 5`

### Demo scenario 1 ###

This section contains the commands used in the scenario 1, which is the creation of a new data source and the inclusion of the corresponding data.


1) First we need to create the database, to do so we execute the following command:
```bash
$ ./manage.py create localoferta_ens_superior
```

2) Now, as we already have the mapping definition, we need to insert the open data in the data base. To do it we must execute the following commands:

**NOTE:** FILEPATH is the **_full path_** for the directory where the open data table is, for example (in a Linux environment): `/home/c3sl/HOTMapper/open_data/DM_LOCAL_OFERTA_2010`


a) To insert 2010:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2010.CSV localoferta_ens_superior 2010 --sep="|" 
```

b) To insert 2011:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2011.CSV localoferta_ens_superior 2011 --sep="|" 
```

c) To insert 2012:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2012.CSV localoferta_ens_superior 2012 --sep="|" 
```

d) To insert 2013:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2013.CSV localoferta_ens_superior 2013 --sep="|" 
```

e) To insert 2014:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2014.CSV localoferta_ens_superior 2014 --sep="|" 
```

f) To insert 2015:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2015.CSV localoferta_ens_superior 2015 --sep="|" 
```

g) To insert 2016:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2016.CSV localoferta_ens_superior 2016 --sep="|" 
```

### Demo scenario 2 ###

This section contains the commands used in the scenario 2, which is an update of an existing data source.


1) First we need to create the database, to do so execute the following command:
```bash
$ ./manage.py create localoferta_ens_superior
```

2) Now, as we already have the mapping protocol, we need to insert the open data in the data base. To do it we must execute the following commands:

**NOTE:** FILEPATH is the **_full path_** for the directory where the open data table is, for example (in a Linux environment): `/home/c3sl/HOTMapper/open_data/DM_LOCAL_OFERTA_2010`

a) To insert 2013:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2013.CSV localoferta_ens_superior 2013 --sep="|" 
```

b) To insert 2014:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2014.CSV localoferta_ens_superior 2014 --sep="|" 
```

c) To insert 2015:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2015.CSV localoferta_ens_superior 2015 --sep="|" 
```

d) To insert 2016:
```bash
$ ./manage.py insert FILEPATH/DM_LOCAL_OFERTA_2016.CSV localoferta_ens_superior 2016 --sep="|" 
```

3) Change the matricula's mapping protocol. You can use the `matricula_remap.csv` (To do so, rename the current `matricula.csv` to something else and the `matricula_remap.csv` to `matricula.csv`). In that case, the only column that will change is the "profissionalizante", because now, instead of the ELSE returns 0 it returns 9. 

4) Run the remap command

```bash
$ ./manage.py remap matricula
```
The above command will update the table `Fonte` and the schema from the table matricula

5) Update the table

```bash
$ ./manage.py update_from_file /FILEPATH/2013_MATRICULA.csv matricula 2013 --columns="profissionalizante" --sep="|"
```

The above command will update the data in the table matricula.
