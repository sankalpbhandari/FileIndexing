##Rudimentary-database-engine- DavisBase

- The implementation operate entirely from the command line and API calls 
- Like MySQL's InnoDB data engine (SDL), program use file-per-table approach to physical storage. 
- Each database table is physcially stored as a separate file. 
- Each table file gets subdivided into logical sections of fixed equal size call pages. 
- Therefore, each table file size is exact increments of the global page_size attribute, i.e. all data files share the same page_size attribute. 
- You may make page_size be a configurable attribute, but we support a page size of 512 Bytes. 
- The programming example used is JAVA.

Read Me:

Default page size: 512 Degree of B tree: 4

Leaf header: 1 00X00 Page type 1 00X01 No of cells 2	00X02 Start of cell pointer 4 00X04 Right pointer 2 * n 00X08 Record pointer

Interior page header: 1 00X00	Page type 4 00X01	Left pointer 4 00X05	Index 4 00X09	Right pointer 4 00X0D	Right most pointer 4 00X11	Parent pointer

Supported Commands: All commands below are case insensitive
Create new database.
```
CREATE database database_name; 
```
Switches to new database.
```
USE database_name;
```
Create new table under respective database.
Supported DataTypes : int,smallint,tinyint, datatime, double
```
CREATE table table_name(id int, name text);
```
Display all tables under respective database.

```
SHOW tables;
```
Display all the databases present in the system.
```
SHOW databases;
```
Display all records in the table.
```
SELECT * FROM table_name;
```
Display columns in column_list records in the table.
```
SELECT (column_list) FROM table_name;
```
Display all records in the table whose rowid is .
```
SELECT * FROM table_name WHERE rowid = value;
```
Display columns in column_list records in the table whose rowid is <value\>.
```
SELECT (column_list) FROM table_name WHERE rowid = value;
```
Inserts into the table the values which corresponds to the columns
```
INSERT INTO table_name (column_list) values(value1, value2)
```
Inserts into the table the values in order in which the columns are there in the table.
```
INSERT INTO table_name values(value1, value2)
```
Delete records whose rowid is <value\>.
```
DELETE FROM table_name WHERE rowid = value;
```
Remove table data and its schema.
```
DROP TABLE table_name;
```
Remove database and its table.
```
DROP Database database_name;
```
Show this help information.
```
HELP;
```
Exit the program.
```
EXIT;/QUIT;
```
To run the program:
Compile and Run the Main.Java file