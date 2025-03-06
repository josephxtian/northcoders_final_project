import re
from src.connection import connect_to_db, close_db_connection
from src.get_currency_name import get_currency_details
from ingestion_to_processed_bucket.dim_date_function import extract_date_info_from_dim_date
from pg8000.native import Connection
from dotenv import load_dotenv

# BEFORE RUNNING, SET UP A LOCAL POSTGRESQL DATABASE
load_dotenv()

def create_connection():
    return connect_to_db()


    # This function will check the input is in the correct format
# Dictionary containing list of dictionaries (multiple allowed)
def check_formatting_of_input(*input_data):
    # check input is in correct format
    for input_dict in input_data:
        if isinstance(input_dict,dict):
            for item in input_dict:
                if isinstance(input_dict[item],list):
                    for inner_dict in input_dict[item]:
                        if isinstance(inner_dict,dict):
                            return True
                        else:
                            raise TypeError(f'Input is wrong type. Must be a dictionary, containing a list of dictionaries. {inner_dict} is {type(inner_dict)}')
                else: raise TypeError(f'Input is wrong type. Must be a dictionary, containing a list of dictionaries. {item} is {type(item)}')
        else:
            raise TypeError(f'Input is wrong type. Must be a dictionary, containing a list of dictionaries. {input_dict} is {type(input_dict)}')


# This function will import incoming information into temporary tables
# Sourcing table headers and table name from input
# It will return a list of lists containing column headers
def make_temporary_tables(database_connection,*input_data):
    # create each table
    column_headers_list = []
    for data in input_data:
        # get table name
        table_names = [*data]
        for table in table_names:
            # get column names
            column_names = data[table][0].keys()
            # add datatypes to column names
            column_names_with_types = []
            for name in column_names:
                if name.find('_id')>=0:
                    column_names_with_types.append(name + " int")
                else:
                    column_names_with_types.append(name + " text")
            database_connection.run(f'''
                CREATE TEMPORARY TABLE {table} {str(tuple(column_names_with_types)).replace("'","")};
                ''')
            
            # populate table with data
            for row in data[table]:
                database_connection.run(f'''
                INSERT INTO {table}
                VALUES {tuple(row.values())};
                ''')
            # run a query to get headers        
            database_connection.run(f"SELECT * FROM {table}")
            # list column headers
            column_headers = [c['name'] for c in database_connection.columns]
            column_headers_list.append(column_headers)
    # return a list of column header lists
    return table_names,column_headers_list


