# Changelog

All changes between versions will be documented in this file.

## 1.1.0 - 2019-10-15
### New Features
* Added integration of sql, python and bash scripts with the command `run_script`.
* Added group execution of sql scripts with the commands `execute_sql_group`, `drop_group` and `rebuild_group`. 
The groups are defined in the configuration file hotmapper/database/`groups.py`.
* The run_aggregations command now also run all denormalizations (It executes any special condition `~` that doesn't need 
the data CSV.).
* All columns information will also be contained in the `table_definition JSON`, it'll be updated automatically when a
remap is executed. This permits easier access to all the table information.
* You can now create a table only with it's table_definition. (It needs the columns parameter in the JSON, 
you still won't be able to insert data without a mapping_protocol CSV.)
* You can now, create, delete and rename columns by editing directly the `table_definition JSON`. (The mapping_protocol
CSV won't be automatically updated and the insertion of data still requires the presence of the columns there.)
* Added a confirmation prompt when doing a remap.

### Fixes
* Fixed insertion error when the header of the data csv contains `"` (quotes).
* Fixed being unable to run denormalizations when the table didn't contain an `ano_censo` column. Now it will try to get
the column `YEAR_COLUMN` defined in the `settings.py` as condition a of the denormalization/aggregation query.
* Fixed insertion of data when two columns of the mapping_protocol where mapped directly to the same header from the data CSV.
* Fixed foreign key error with sqlalchemy-monetdb (dependency updated).

### Code changes
* Updated all dependencies, adjusted the code accordingly with possible changes.
* Added new functional tests. Can be run executing `python -m tests.database_test test_all`
* Refactored table_definition into a class, similar to protocol.