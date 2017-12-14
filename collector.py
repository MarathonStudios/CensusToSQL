import sys

import project_error_checker
import table_generator

#store key file paths
keys_one_year_path = "keys/keys_1_year.json"
keys_five_year_path = "keys/keys_5_year.json"

#store execution file path
execution_file_path = "execution_tables.json"

#store config file path
config_file_path = "config.json"

# create an instance of project_error_checker
checker = project_error_checker.project_error_checker(keys_one_year_path, keys_five_year_path, execution_file_path, config_file_path)

# creating an instance of table_generator
table_gen = table_generator.table_generator(keys_one_year_path, keys_five_year_path, execution_file_path, config_file_path)

print("Checking project files for errors......")

# These functions are used to find errors in the project files before execution
# if any errors are found then an message is shown with the specific error, and execution stops

#check config file for errors
config_status = checker.check_config()
if(config_status != "Verified"):
	print(config_status)
	sys.exit(1)

#check key files for errors
config_status = checker.check_key_files()
if(config_status != "Verified"):
	print(config_status)
	sys.exit(1)


#check execution file for errors
config_status = checker.check_execution_file()
if(config_status != "Verified"):
	print(config_status)
	sys.exit(1)


print("Done checking project files, no errors found")

print("Checking database connection....")

project_status = table_gen.check_database_connection()
if(config_status != "Verified"):
	print(config_status)
	sys.exit(0)

print("Database connection succeeded!")

#check to ensure the table names specified in the execution file are not already used for existing tables
# if so an error message is shown and execution stops
project_status = table_gen.check_for_existing_tables()
if(config_status != "Verified"):
	print(config_status)
	sys.exit(1)

# Once all the checks are passed this function is called to generate the tables
table_gen.create_tables()


