import json

########################################################################################
# This class is used to verify the formate of config.json
# ,variable key files, and execution file.
########################################################################################
class project_error_checker:

	#creates the object, inits paths to core files
	def __init__(self, keys_one_year_path, keys_five_year_path, execution_file_path, config_file_path):

		#saves paths to core files
		self.keys_one_year_path = keys_one_year_path
		self.keys_five_year_path = keys_five_year_path
		self.execution_file_path = execution_file_path
		self.config_file_path = config_file_path

		#set min and max for both 1 and 5 year ranges
		self.one_year_range_min = 2011
		self.one_year_range_max = 2016
		self.five_year_range_min = 2009
		self.five_year_range_max = 2016


	# Reads the config file and checks for proper json formate, 
	# and that all the fields both exist and are initalized
	def check_config(self):
		
		with open(self.config_file_path, encoding='utf-8') as config_file:

			#check integrity of config.json json formate
			try:
				config_json_data = json.load(config_file)
			except json.decoder.JSONDecodeError:
				return "Error: " + self.config_file_path + " file not in valid json formate... Aborting"
			
			#check if database connection property exists
			if ('db_connection_vars' not in config_json_data):
				return "Error: " + self.config_file_path + " file does not have 'db_connection_vars' attribute"

			#checks for username
			if('username' not in config_json_data['db_connection_vars']):
				return "Error: " + self.config_file_path + " does not have a ['db_connection_vars']['username'] attribute"
			elif(config_json_data['db_connection_vars']['username'] == ""):
				return "Error: " + self.config_file_path + " file has empty ['db_connection_vars']['username'] attribute"

			#checks for password (Note: password can be blank)
			if('password' not in config_json_data['db_connection_vars']):
				return "Error: " + self.config_file_path + " does not have a ['db_connection_vars']['password'] attribute"
			
			#checks for host name
			if('host' not in config_json_data['db_connection_vars']):
				return "Error: " + self.config_file_path + " does not have a ['db_connection_vars']['host'] attribute"
			elif(config_json_data['db_connection_vars']['host'] == ""):
				return "Error: " + self.config_file_path + " file has empty ['db_connection_vars']['host'] attribute"

			#checks for port number
			if('port' not in config_json_data['db_connection_vars']):
				return "Error: " + self.config_file_path + " does not have a ['db_connection_vars']['port'] attribute"
			elif(config_json_data['db_connection_vars']['port'] == ""):
				return "Error: " + self.config_file_path + " file has empty ['db_connection_vars']['port'] attribute"

			#checks for schema name to use
			if('dataset_schema' not in config_json_data['db_connection_vars']):
				return "Error: " + self.config_file_path + " does not have a ['db_connection_vars']['dataset_schema'] attribute"
			elif(config_json_data['db_connection_vars']['dataset_schema'] == ""):
				return "Error: " + self.config_file_path + " file has empty ['db_connection_vars']['dataset_schema'] attribute"

			#check if api key exists
			if ('census_api_key' not in config_json_data):
				return "Error: " + self.config_file_path + " file does not have 'census_api_key' attribute"
			elif (config_json_data['census_api_key'] == ""):
				return "Error: No api key set in config.json"

			#if all these checks pass.....
			return "Verified"

	# Checks for proper json formate on the key files
	def check_key_files(self):
		with open(self.keys_one_year_path, encoding='utf-8') as _one_year_key_data:

			#check integrity of one_year_key file json formate
			try:
				one_year_key_data = json.load(_one_year_key_data)
			except json.decoder.JSONDecodeError:
				return "Error: " + self.keys_one_year_path + " file not in valid json formate... Aborting"

		with open(self.keys_five_year_path, encoding='utf-8') as _five_year_key_data:	

			#check integrity of five_year_key file json formate
			try:
				config_json_data = json.load(_five_year_key_data)
			except json.decoder.JSONDecodeError:
				return "Error: " + self.keys_five_year_path + " file not in valid json formate... Aborting"

			#if all these checks pass.....
			return "Verified"

	# Checks for proper json formate on the execution_tables file
	# Then checks to ensure all required fields exist and are valid inputs
	def check_execution_file(self):

		with open(self.execution_file_path, encoding='utf-8') as execution_file:

			#check integrity of execution file json formate
			try:
				execution_file_data = json.load(execution_file)
			except json.decoder.JSONDecodeError:
				return "Error: execution_tables.json file not in valid json formate... Aborting"

			#ensure the file has tables to be created
			if(len(execution_file_data['tables']) == 0):
				return "Error: " + self.execution_file_path + " file doesn't have any tables defined.... Aborting \nPlease see readme.md for instructions on defining tables"

			#loop through each entry in execution file
			for index, key in enumerate(execution_file_data['tables']):

				#check the 'columns' attribute
				if("columns" not in execution_file_data['tables'][key]):
					return "Error: execution_file_data['tables'][" + key + "]['columns'] does not exist in execution file .... Aborting"
				elif(execution_file_data['tables'][key]['columns'] == ""):
					return "Error: execution_file_data['tables'][" + key + "]['columns'] attribute is empty in execution file .... Aborting"

				#check the 'geography' attribute
				if("geography" not in execution_file_data['tables'][key]):
					return "Error: execution_file_data['tables'][" + key + "]['geography'] does not exist in execution file .... Aborting"
				elif(execution_file_data['tables'][key]['geography'] != "state" and execution_file_data['tables'][key]['geography'] != "county"):
					return "Error: execution_file_data['tables'][" + key + "]['geography'] attribute must be 'state' or 'county' .... Aborting"

				#check the 'term' attribute
				if("term" not in execution_file_data['tables'][key]):
					return "Error: execution_file_data['tables'][" + key + "]['term'] does not exist in execution file .... Aborting"
				if(execution_file_data['tables'][key]["term"] != "1" and execution_file_data['tables'][key]["term"] != "5"):
					return "Error: execution_file_data['tables'][" + key + "]['term'] attribute must be '1' or '5' .... Aborting"

				#check the 'range_of_years' attribute
				if("range_of_years" not in execution_file_data['tables'][key]):
					return "Error: execution_file_data['tables'][" + key + "]['range_of_years'] does not exist in execution file .... Aborting"

				#check if two numbers seperated by commas were entered
				elif(len(execution_file_data['tables'][key]["range_of_years"].split(',')) != 2):
					return "Error: execution_file_data['tables'][" + key + "]['range_of_years'] is in wrong formate .... Aborting \nPlease see readme.md for instructions on setting the range"

				#check one year range low bound
				elif(execution_file_data['tables'][key]["term"] == "1" and int(execution_file_data['tables'][key]['range_of_years'].split(',')[0]) not in range(self.one_year_range_min, self.one_year_range_max + 1)):
					return "Error: execution_file_data['tables'][" + key + "]['range_of_years'] both parts must be less than or equal to " + str(self.five_year_range_max) + " and greater than of equal to " + str(self.five_year_range_min) + " if the term is one years .... Aborting"

				#check one year range higher bound
				elif(execution_file_data['tables'][key]["term"] == "1" and int(execution_file_data['tables'][key]['range_of_years'].split(',')[1]) not in range(self.one_year_range_min, self.one_year_range_max + 1)):
					return "Error: execution_file_data['tables'][" + key + "]['range_of_years'] both parts must be less than or equal to " + str(self.five_year_range_max) + " and greater than of equal to " + str(self.five_year_range_min) + " if the term is one years .... Aborting"

				#check five year range lower bound
				elif(execution_file_data['tables'][key]["term"] == "5" and int(execution_file_data['tables'][key]['range_of_years'].split(',')[0]) not in range(self.five_year_range_min, self.five_year_range_max + 1)):
					return "Error: execution_file_data['tables'][" + key + "]['range_of_years'] both parts must be less than or equal to " + str(self.five_year_range_max) + " and greater than of equal to " + str(self.five_year_range_min) + " if the term is five years .... Aborting"

				#check five year range high bound
				elif(execution_file_data['tables'][key]["term"] == "5" and int(execution_file_data['tables'][key]['range_of_years'].split(',')[1]) not in range(self.five_year_range_min, self.five_year_range_max + 1)):
					return "Error: execution_file_data['tables'][" + key + "]['range_of_years'] both parts must be less than or equal to " + str(self.five_year_range_max) + " and greater than of equal to " + str(self.five_year_range_min) + " if the term is five years .... Aborting"

				#ensure low bound is lower than higher bound
				elif(int(execution_file_data['tables'][key]['range_of_years'].split(',')[0]) > int(execution_file_data['tables'][key]['range_of_years'].split(',')[1])):
					return "Error: execution_file_data['tables'][" + key + "]['range_of_years'] haw a lower bound greater than the higher bound .... Aborting"

				#if all these checks pass.....
				return "Verified"