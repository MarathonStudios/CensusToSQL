import json
import pymysql
import requests
import urllib
import sys

########################################################################################
# This class is used to verify that the given table names are not already used.
# After this check the functions in this oject generate a listing in the column lookup
# table for each column added to a table, then creates a table for each table defined in
# the execution tables.json file, gets the data from the api, and fills the table with the
# data for each table defined in the exectution_tables.json file
########################################################################################
class table_generator:

	#creates the object, inits paths to key files
	def __init__(self, keys_one_year_path, keys_five_year_path, execution_file_path, config_file_path):

		#store paths to project files
		self.keys_one_year_path = keys_one_year_path
		self.keys_five_year_path = keys_five_year_path
		self.execution_file_path = execution_file_path
		self.config_file_path = config_file_path

		#store dbconnection info
		#save the config vars in memory
		with open('config.json', encoding='utf-8') as config_file:

			config_data = json.load(config_file)

			self.db_username = config_data['db_connection_vars']['username']
			self.db_password = config_data['db_connection_vars']['password']
			self.db_port = config_data['db_connection_vars']['port']
			self.db_host = config_data['db_connection_vars']['host']
			self.db_schema = config_data['db_connection_vars']['dataset_schema']
			self.api_key = config_data['census_api_key']

	# connects the the database to ensure a connection can be made
	def check_database_connection(self):
		conn = pymysql.connect(host=self.db_host, user=self.db_username, passwd=self.db_password, db=self.db_schema)
		
		try:
			with conn.cursor() as cursor:
				return "Verified"
		except:
				return "\nError: could not connect to database, check the connection attributes in the " + self.config_file_path + " file.... Aborting"

	# This function checks to see if the table names used in the execution tables file 
	# are already used in the databse. If they are an error message is returned
	def check_for_existing_tables(self):

		#open file
		with open(self.execution_file_path, encoding='utf-8') as execution_file:
			try:
				execution_file_data = json.load(execution_file)
			except json.decoder.JSONDecodeError:
				return "\nError: execution_tables.json file not in json formate... Aborting" 

		#get list of new table names
		names_for_new_tables = []
		for index, key in enumerate(execution_file_data['tables']):
			if(execution_file_data['tables'][key] not in names_for_new_tables):
				names_for_new_tables.append(key)

		#create database connection
		conn = pymysql.connect(host=self.db_host, user=self.db_username, passwd=self.db_password, db=self.db_schema)
		with conn.cursor() as cursor:

			#loops through each table index from the previously made list of table names
			for index in range(len(names_for_new_tables)):

				#make a select statemnt for table
				sql_command = "SELECT * FROM " + names_for_new_tables[index] + ";"
				try:
					# if the command doesn't throw an error it means the table exists already
					conn.execute(sql_command)
					conn.commit()

					# table already exists
					return "Error: table " + names_for_new_tables[index] + " already exists... Aborting"
				except:

					#if an error is not thrown then this table doensn't exist yet
					#this line of code is not nessessary for logic, but something must be here for this to run correctly
					sql_command = "SELECT * FROM " + names_for_new_tables[index] + ";"

		#if all these checks pass.....
		return "Verified"

	#this fuction inserts the collected data from the api into the pre created table
	def fill_table(self, table_data, table_name, column_names, geography):

		# create a database connection
		conn = pymysql.connect(host=self.db_host, user=self.db_username, passwd=self.db_password, db=self.db_schema)
		with conn.cursor() as cursor:

			if(geography == 'county'):
				sql = "INSERT INTO `" + table_name + "` (`fips_key`, `collection_year`,`state_name`,`county_name`,"
			elif(geography == 'state'):
				sql = "INSERT INTO `" + table_name + "` (`state_fips_key`, `collection_year`,`state_name`,"

			# add the column names to the insert statement
			for column in column_names:
				sql += "`" + column + "`,"

			# all values part of the insert statement, these values are added to the string at execution
			if(geography == 'county'):
				sql = sql[:-1] + ") VALUES (%s,%s,%s,%s,"
				additional_cols = 4
			elif(geography == 'state'):
				sql = sql[:-1] + ") VALUES (%s,%s,%s,"
				additional_cols = 3

			# add a %s for each column
			for column in column_names:
				sql = sql + "%s,"

			#end insert statement
			sql = sql[:-1] + ")"

			#loop through each row of data
			for row in range(len(table_data)):

				#create empty tuple
				tup = ()

				#for each piece of data create a new tuple (because you can not directly add to them)
				for i in range(len(column_names) + additional_cols):
					tup = tup + (table_data[row][i],)

				# once data is in tuple execute the tuple along with the insert statement
				# the values in the tuple replace the %s added previously
				cursor.execute(sql, tup)

				#save changes to the database
				conn.commit()

	# this function takes in multiple lists of data with dfferent columns but same amount of rows
	# It returns a list with all tables merged together
	def merge_datasets(self, datasets, year, geography):

		all_data = []

		#loop through each row
		for row_index in range(1, len(datasets[0])):
			one_row = []

			#the first table will have extra items added to the front
			first_table = True

			# loop through each inner table
			for data_table_index in range(len(datasets)):

				if(first_table == True):

					if(geography == 'county'):

						# save list with state name and county name
						place = datasets[data_table_index][row_index][0].split(',')
						county_code =  datasets[data_table_index][row_index][len(datasets[data_table_index][row_index]) - 1]
						state_code =  datasets[data_table_index][row_index][len(datasets[data_table_index][row_index]) - 2]
						fips_code = int(state_code + county_code)

						# add to current row
						one_row += [fips_code, year, place[1], place[0]] + datasets[data_table_index][row_index][1:-2]

					elif(geography == 'state'):

						#save parts to be added to front
						state = datasets[data_table_index][row_index][0]
						state_code =  datasets[data_table_index][row_index][len(datasets[data_table_index][row_index]) - 1]
						one_row += [state_code, year, state] + datasets[data_table_index][row_index][1:-1]

					#once the first table is done this is set to false
					first_table = False
				else:

					#add data to current row, skip the name and ending indexes
					if(geography == 'county'):
						one_row += datasets[data_table_index][row_index][1:-2]
					elif(geography == 'state'):
						one_row += datasets[data_table_index][row_index][1:-1]
			
			#append row to complete list
			all_data.append(one_row)

		return all_data

	# This function adds the column keys to the database (with the keys label, concept, predicate type, group, and whether or not its a margin of error column or not)
	# This functions returns a full list of columns (the init list of columns does not include margin of error columns, if requested they are added here)
	# This function also returns a list of column comments, these comments are from the label attribute for that specific key
	def add_col_keys_to_db_and_get_full_col_names(self, column_names, include_margin_of_error, key_file_path):

		# used to store key column names
		cols = []

		# used to store key labels (refered to as comments)
		col_comments = []

		# save execution file data
		with open (key_file_path, encoding='utf-8') as execution_file:

			execution_file_data = json.load(execution_file)

		# create a mysql connection
		conn = pymysql.connect(host=self.db_host, user=self.db_username, passwd=self.db_password, db=self.db_schema)

		with conn.cursor() as cursor:

			# loop through each key (column) name
			for column_index in range(len(column_names)):

				# store the data for that key
				current_column_key_object = execution_file_data['variables'][column_names[column_index]]

				# add this column to the full list
				cols.append(column_names[column_index])

				# check for and save each attribute from the key oject that is availible
				if('label' in current_column_key_object):
					label = current_column_key_object['label']
				else:
					label = ""

				# add the comment in order of the corresponding key/column name (they must be kept in order!) 
				col_comments.append(label)

				if('concept' in current_column_key_object):
					concept = current_column_key_object['concept']
				else:
					concept = ""

				if('predicateType' in current_column_key_object):
					predicate_type = current_column_key_object['predicateType']
				else:
					predicate_type = ""

				if('group' in current_column_key_object):
					group = current_column_key_object['group']
				else:
					group = ""

				# check to see if an entry with that key name already exists
				sql_command = "SELECT * FROM column_lookup WHERE column_key = '" + column_names[column_index] + "';"

				cursor.execute(sql_command)

				results = cursor.fetchall()

				# if it doesn't exist add the key to the table
				if(len(results) == 0):

					sql_statement = "INSERT INTO `column_lookup` (`column_key`, `label`, `concept`, `predicate_type`, `group`, `margin_of_error`)"
					sql_statement += "VALUES (%s, %s, %s, %s, %s, %s)"

					# A zero at the end is used to indicate that this is an estimate column and not a margin of error
					insert_data = (column_names[column_index], label, concept, predicate_type, group, "0")

					cursor.execute(sql_statement, insert_data)

					conn.commit()

				# if include margin of error is set and a margin of error column is found then add this column to the list
				if('attributes' in current_column_key_object and include_margin_of_error):

					# making sure there is at least one column in this field
					if(len(current_column_key_object['attributes'].split(',')) >= 1):

						# Add this margin of error column to the list
						cols.append(current_column_key_object['attributes'].split(',')[0])

						# Add this columns label in order
						col_comments.append(label + " -- Margin of Error")

						# checking if this column already exists in table
						sql_command = "SELECT * FROM column_lookup WHERE column_key = '" + current_column_key_object['attributes'].split(',')[0] + "';"

						cursor.execute(sql_command)

						results = cursor.fetchall()

						# if it doesn't exist add the key to the table
						if(len(results) == 0):

							sql_statement = "INSERT INTO `column_lookup` (`column_key`, `label`, `concept`, `predicate_type`, `group`, `margin_of_error`)"

							sql_statement += "VALUES (%s, %s, %s, %s, %s, %s)"

							# A 1 is added at the end to indicate that this is a margin of error column
							insert_data = (current_column_key_object['attributes'].split(',')[0], label, concept, predicate_type, group, "1")

							cursor.execute(sql_statement, insert_data)

							conn.commit()

		# return the list of columns to be fetched and a comment for each one (these lists should be the save length)
		return cols, col_comments

	# This function servers as the controller for creating tables and adding data to the database
	# It loops through each table defined in the execution tables file and adds each to the database (and checks each for errors independent of other tables)
	def create_tables(self):

		# save the execution file data
		with open (self.execution_file_path, encoding='utf-8') as execution_file:

			execution_file_data = json.load(execution_file)

		# loop through each table
		for table_index, table_name in enumerate(execution_file_data['tables']):

			#store the attributes for each table
			column_names = execution_file_data['tables'][table_name]['columns'].split(',')
			geography =  execution_file_data['tables'][table_name]['geography']
			start_year = int(execution_file_data['tables'][table_name]['range_of_years'].split(',')[0])
			end_year = int(execution_file_data['tables'][table_name]['range_of_years'].split(',')[1])
			term = int(execution_file_data['tables'][table_name]['term'])
			include_margin_of_error = execution_file_data['tables'][table_name]['include_margin_of_error']

			# set the key file path for this table
			if(term == 1):
				key_file_path = self.keys_one_year_path
			elif(term == 5):
				key_file_path = self.keys_five_year_path

			# get a list of columns to be fetched and store comments describing each
			column_names, column_comments = self.add_col_keys_to_db_and_get_full_col_names(column_names, include_margin_of_error, key_file_path)

			# attempt to create the table in the database, if "Verified" is not returned then an error message was returned instead
			table_status = self.init_table_in_database(column_names, column_comments, table_name, geography, term)

			# print the error message if an error occured (note that an error doesn't stop execution, it just skips that table)
			if(table_status != "Success"):
				print(table_status)
				continue

			# get the data to be inserted into the table in the formate of a [][] list
			table_data = self.get_table_data(table_name, column_names, geography, term, start_year, end_year)

			# add the data to the previously created table
			self.fill_table(table_data, table_name, column_names, geography)

			# move onto the next table to be added ..............

	# This function uses the api to get data that will be loaded into the table in the form of a [][] list
	def get_table_data(self, table_name, column_names, geography, term, start_year, end_year):

		all_table_data = []

		# add pieces of the dataset to all_table_data by year
		for year in range(start_year, end_year + 1):

			# create a list for pieces of the dataset in the formate of [dataset][row][column]
			# this needs to be stored this way to it can be converted to a list of [row][column] 
			# this is only needed when the user needs more than 50 columns from the api (50 columns is the request limit)
			table_data_pieces_for_current_year = []

			# set the api limit
			cenus_column_length_limit = 48

			# used to track index of current column
			current_column = 0

			# used to track index of start range
			start_range = 0

			# used to track index of end range
			end_range = 0

			# used to store the total number of columns
			max_range = len(column_names)

			# loop until current column index is equal to total columns
			while max_range != current_column:

				api_request_url_def_cols = ""

				start_range = current_column

				# if this is true then the user requested more than 50 columns
				if(max_range - start_range < cenus_column_length_limit):
					end_range = max_range

				# this will run when there are 50 or less columns left to grab
				else:
					end_range = start_range + cenus_column_length_limit

				# make a list of columns to be requested during this iteration
				for column_index in range(start_range, end_range):

					api_request_url_def_cols = api_request_url_def_cols + str(column_names[column_index]) + ','
          		
          		# create the api request url
				api_request_url = "https://api.census.gov/data/" + str(year) + "/acs/acs" + str(term) + "?get=NAME," + api_request_url_def_cols[:-1] + "&for=" + geography + ":*&key=" + self.api_key

				# try to save the data
				try:
					with urllib.request.urlopen(api_request_url) as census_data:

						# store requested data
						api_data = json.loads(census_data.read().decode())

						# add this set of data to the list
						table_data_pieces_for_current_year.append(api_data)

				# if this fails then the requested data is not avalible
				except:
					print("\nNote: Data for " + str(table_name) + " at term " + str(term) + " for year " + str(year) + " is not available from the api.... skipping")

					break

				# set the current column to the last one requested
				current_column = end_range

			# if not data was retrived dont do anything, else merge the datasets into one list
			if(table_data_pieces_for_current_year != []):

				# merge previous dataset for a specific year into one list, then add the rows to the master list
				all_table_data += self.merge_datasets(table_data_pieces_for_current_year, year, geography)

		# go on and collect the next years data ......

		return all_table_data


	# This function creates tables in the database
	def init_table_in_database(self, column_names, column_comments, table_name, geography, term):

		# tables with 500 + columns are both completely insane and are also not supported
		if(len(column_names) > 500):
			return "\nNote: " + table_name + " is set to have over 500 columns.... skipping this table, it won't be added to the database!"

		# make a databse connection
		conn = pymysql.connect(host=self.db_host, user=self.db_username, passwd=self.db_password, db=self.db_schema)

		with conn.cursor() as cursor:

			table_create_statement = ""

			# create the first part of the create statement for either state or county
			if(geography == "county"):
				table_create_statement = "  CREATE TABLE `" + table_name + "` (\n`fips_key` int NOT NULL, \n`collection_year` INT(4), \n`state_name` VARCHAR(22), \n`county_name` VARCHAR(150), \n"
			elif(geography == "state"):
				table_create_statement = "  CREATE TABLE `" + table_name + "` (\n`state_fips_key` int NOT NULL, \n`collection_year` INT(4), \n`state_name` VARCHAR(22), \n"

			# add the column names and the respective column comments to the create statement
			for column_index in range(len(column_names)):

				table_create_statement += " `" + column_names[column_index] + "` BIGINT COMMENT '" + column_comments[column_index] + "', \n"

			# create the last part of the create statement for either state or county
			if(geography == "county"):
				table_create_statement += "PRIMARY KEY (`fips_key`, `collection_year`, `state_name`, `county_name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
			elif(geography == "state"):
				table_create_statement += "PRIMARY KEY (`state_fips_key`, `collection_year`, `state_name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

			# try to add this table to the database
			try:
				cursor.execute(table_create_statement)
				conn.commit()

			# if the table could not be added return an error
			except:
				return "\nError: " + table_name + " could not be created.... skipping this table, it won't be added to the database!\nMake sure a table with this name doesn't already exist!"

			# Else return success
			return "Success"












