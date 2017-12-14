import sys
import json
import urllib.request
import pymysql

########################################################################################
# This program is used to generate the key file contents by loading them from the census
# website and saving them to the 1 and 5 year key files. This program also checks the
# sql connection and creates the column loopup table in the specified database
########################################################################################

#storing link to keys data
one_year_variables_link = "https://api.census.gov/data/2016/acs/acs1/variables.json"
five_year_variables_link = "https://api.census.gov/data/2015/acs/acs5/variables.json"

#store key file paths
keys_one_year_path = "keys/keys_1_year.json"
keys_five_year_path = "keys/keys_5_year.json"

print("Loading one year census data keys .........")

try:
	#load one year keys from server to memory
	urllib.request.urlretrieve(one_year_variables_link, keys_one_year_path)
except:
	print("\nError: Cannot read '/keys/keys_1_year.json'! Make sure file exists and that this program has permissions to modify it!")
	sys.exit(1)

print("Loading five year census data keys .........")

try:
	#load one year keys from server to memory
	urllib.request.urlretrieve(five_year_variables_link, keys_five_year_path)
except:
	print("\nError: Cannot read '/keys/keys_5_year.json'! Make sure file exists and that this program has permissions to modify it!")
	sys.exit(1)

#store dbconnection info
#save the config vars in memory
with open('config.json', encoding='utf-8') as config_file:
	config_data = json.load(config_file)

	db_username = config_data['db_connection_vars']['username']
	db_password = config_data['db_connection_vars']['password']
	db_port = config_data['db_connection_vars']['port']
	db_host = config_data['db_connection_vars']['host']
	db_schema = config_data['db_connection_vars']['dataset_schema']
	
# if the config.json has invalid connection settings this will fail
try:

	# init the database connection
	conn = pymysql.connect(host=db_host, user=db_username, passwd=db_password, db=db_schema)

	# open the connection
	with conn.cursor() as cursor:

		#create the column_lookup table in the database
		sql_command = """
		CREATE TABLE `column_lookup` (
  			`index_key` int(11) NOT NULL AUTO_INCREMENT,
  			`column_key` varchar(15) DEFAULT NULL,
  			`label` varchar(300) DEFAULT NULL,
  			`concept` varchar(300) DEFAULT NULL,
  			`predicate_type` varchar(10) DEFAULT NULL,
  			`group` varchar(8) DEFAULT NULL,
  			`margin_of_error` int(1) DEFAULT NULL,
  			PRIMARY KEY (`index_key`)
		) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4;
		"""


		cursor.execute(sql_command)
		conn.commit()

# the config.json has invalid connection settings or the mysql sever isn't running
except:

	print("\nError: Could not connect to the mysql database...\nPlease recheck connection settings in the config.json file and make sure the mysql server is running on your machine!")
	sys.exit(1)

print("Success! This project is ready to be used")




