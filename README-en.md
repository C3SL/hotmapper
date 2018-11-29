# HOTMapper #

This respository was created in order to make available the HOTMapper, a tool that allows the user to manage his historical data using a mapping protocol for demonstration purposes for the EDBT 2019. 

## Data ##

The dataset "Matrícula" can be found at the link: [INEP](http://portal.inep.gov.br/web/guest/microdados) in the section "Censo Escolar".

The dataset "Local Oferta" can be found in the same link, but at the section "Censo da Educação Superior". Additionaly for increase the convenience, all data from "Local Oferta" is in the directory open_data.

**NOTE**: It's important that you ta

## Requirements ##

* Python 3 (It's recommended that you use a virtual environment, such as virtualenv)
* MonetDB (We plan to make other databases to work with HOTMapper in the future)

## Installation ##

----
**NOTICE:**
We suppose that you already have Python 3.x installed in you computer and that all the following commands that use Python will use the Python 3.x
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

## Interface de linha de comando ##

The CLI (Command Line Interface) uses the standart of the manage.py package, which means that to invoke a command you should use the following pattern:

```bash
$ python manage.py [COMMAND] [POSITIONAL ARGUMENTS] [OPTIONAL ARGUMENTS]
```

Where COMMAND can be:

* create: Create a table using the mapping protocol.

```bash
$ python manage.py create <table_name>
```

**Notice** that the HOTMapper will use the name of the protocol as the name of the table.


* insert: Insert a CSV file in an existing table.

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

**NOTICE:** The command does not take care of foreign keys that points to the table that are being deleted. Therefore, the database can produce errors.

* remap: syncronize a table with the mapping protocol.

```bash
$ python manage.py remap <table_name>
```
You should use this command everytime a mapping protocol is updated.

The remap allows the creation of new columns, the drop of existent columns, the renaming of columns and the change of type of columns. Be aware that the bigger the table the bigger the usegae of RAM memory.

* generate_pairing_report: generate reports to compare data from diferent years.

```bash
$ python manage.py generate_pairing_report [--output xlsx|csv]
```

The reports will be created in the folder "pairing" 


* generate_backup: Create/Update a file to backup the database.

```bash
$ python manage.py generate_backup
```