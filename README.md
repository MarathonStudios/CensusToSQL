# CensusToSQL
CensusToSQL is a Python script that allows direct importing of Census Bureau American Community Survey (ACS) data from the Census API to a MySQL database.

<hr>

### What do I need to run this program?
- A running mySQL server
- python 3
- python 3 dependencies
	* json
	* pymysql
	* urllib
	* sys

### System Requirements
- Ubuntu 16.04 LTS
(This program will probably work on any OS, but it has only been tested for Ubuntu so far)


### What does this program do?

This program connects to the census data api (https://census.gov/data/developers/data-sets.html)
and stores all needed from this api into a mysql schema (assuming it's all available). As well as creating tables this program also stores information for each column of data a table called column_lookup. There the column name, label, concept, and group info is stored for each key

Note: As of this version only American Community Survey 1-Year Data (2011-2016), and (American Community Survey 5-Year Data (2009-2016)) can be collected using this tool


### How do I used this program?

<b>Note: If you are unsure on how to use the census developer api to retrive data please read up on how its used before moving forward. Here is a good place to start (https://census.gov/developers/)</b>

Step One:
- Create a schema in mysql for storing the census api data (Don't add anything to this schema! or else things might break)
- Enter your database connection information in the config.json file (the schema name attribute is the name of the schema you created in the last step)
- Enter your census.gov developer api key in the config.json file (don't have an api key? get one at https://api.census.gov/data/key_signup.html)

Step Two:
- Run the build.py file, open up a terminal and run "python3 build.py" in this directory
(This command gets the project ready, you only ever need to run this once)

Step Three:
- Open the execution_tables.json file and enter the details for the data you want collected

- Example format of the execution_tables.json file:

```
{
	"tables" : {
		"table_name_goes_here" : {
			"columns" : "B25090_002E,B25090_003E",
			"include_margin_of_error": true,
			"geography" : "state",
			"range_of_years": "2013,2016",
			"term" : "1"
		}
	}
}
```
- columns
	- These columns must correspond with a column key (look in keys/keys_1_year.json or keys/keys_5_year.json to see all valid keys)
	- These columns must be separated by commas!

- include_margin_of_error field:
	- When this field is true the program will attempt to find an associated margin of error column for each data estimate column you requested

	- When this field is false the program will not include the margin of error columns in the table

- geography
	- As of this version only state level and county level data can be collected using this tool
	- the geography field can only be "state" or "county" else the program will throw an error

- range_of_years
	- This is the range of years the data will be collected from
	- If this field is equal to "2013, 2016", the program will gather all data for every year in this range
	- If this field is equal to "2016, 2016", the program will gather all data for 2016

- term
	- The census collection agency publishes data for 1 year estimates and 5 year estimates
	- term can only be "1" or "5"

<b>Note: You can add as many tables to this list as you need, each one will be created independently</b> <br><br>
<b>Note: Table names cannot be used twice in a schema, make sure all table names you define are unique. Else the program will tell you that the table already exists</b>

Step Four:
- run "python3 collector.py" in this directory
- If all goes well you will see your tables added to the database, else an error message will tell you where you went wrong

<b> To figure out what data is stored to a specific column reefer to the column_lookup table in the database (this table is added to the schema and is updated automatically with every successful run)</b>

### Example program walk through

- Copy this json object into execution_tables.json

```
{
	"tables" : {
		"test_table_one" : {
			"columns" : "B25090_002E,B25090_003E, B25090_001E",
			"include_margin_of_error": true,
			"geography" : "state",
			"range_of_years": "2013,2016",
			"term" : "1"
		},
		"test_table_two" : {
			"columns" : "B25090_002E,B25090_003E, B25090_001E",
			"include_margin_of_error": false,
			"geography" : "county",
			"range_of_years": "2011,2016",
			"term" : "5"
		}
	}
}
```


- Run "python3 collector.py" in this directory

- If all goes well two tables will be added to your schema

### Q and A

Q: What does the margin of error column mean in the column_lookup table?<br>
A: If that column is marked 0 then it is a data estimate, if that column is marked 1 then it is a data estimate margin of error


