
from src.dim_date_function import extract_date_info_from_dim_date
from src.get_currency_name import get_currency_details
import pg8000.native
from s3_read_function.s3_read_function import read_files_from_s3


# This function will set up all the required empty dimensions tables
def set_up_dims_table(database_connection,table_names):
    # delete all existing dim tables
    for dim_table in dimensions_tables_creation:
        database_connection.run(f"DROP TABLE IF EXISTS {dim_table};")
    # create list of tables to be created from 
    dim_tables_created = []
    # check dependent tables exist for each dim table
    for dim_table in dimensions_tables_creation:
      if all(dependent in table_names for dependent in list(dimensions_tables_creation[dim_table][1:])):
        dim_tables_created.append(dim_table)
    if not dim_tables_created:
      raise Exception("Not enough data to create any dim tables, please ensure adequate data has been provided")
    # create dim tables that can be created
    for table in dim_tables_created:
        database_connection.run(f"CREATE TEMPORARY TABLE \"{table}\" ({dimensions_tables_creation[table][0]});")
    # return the names of dim tables created
    return dim_tables_created




def put_info_into_dims_schema(database_connection, dim_tables_created, date_id=None, currency_id=None):
    dimension_value_rows = {}
    print(f"dim_tables_created: {dim_tables_created}")

    # Ensure date_id is provided for dim_date processing
    if 'dim_date' in dim_tables_created and date_id is None:
        raise ValueError("date_id must be provided for dim_date processing")

    # Ensure currency_id is provided for dim_currency processing
    if 'dim_currency' in dim_tables_created and currency_id is None:
        raise ValueError("currency_id must be provided for dim_currency processing")

    for table in dim_tables_created:
        if table == "dim_date":
            print(f"Processing table: {table} with date_id: {date_id}")
            date_info = extract_date_info_from_dim_date(date_id)
            print(f"Extracted Date Info for {table}: {date_info}")
        elif table == "dim_currency":
            print(f"Processing table: {table} with currency_id: {currency_id}")
            currency_info = get_currency_details(currency_id)
            print(f"Currency details for {table}: {currency_info}")
        elif table not in ["dim_staff", "dim_location", "dim_design", "dim_counterparty"]:
            raise Exception("Dimension table names requested are not valid")


        # Ensure the table is inserted correctly into the database
        dimension_value_rows[table] = database_connection.run(f'''
            INSERT INTO {table}
            SELECT {dimensions_insertion_queries[table]}
            RETURNING *;
        ''')

        # Raise error if no rows were inserted
        if not dimension_value_rows[table]:
            raise Exception(f"No paired keys to perform JOIN on for table {table}")

    # Raise error if no dimension rows were inserted
    if not dimension_value_rows:
        raise Exception("No rows outputted from any dimension table")

    return dimension_value_rows




# this function will populate the dimension tables
# def put_info_into_dims_schema(database_connection,dim_tables_created, date_id=None, currency_id=None):

#     dimension_value_rows = {}
#     print(f"dim_tables_created: {dim_tables_created}")

#     for table in dim_tables_created:
#         if table == "dim_date":
#             print(f"Processing table: {table} with date_id: {date_id}")
#             date_info = extract_date_info_from_dim_date(date_id)
#             print(f"Extracted Date Info for {table}: {date_info}")
#         elif table == "dim_currency":
#             print(f"Processing table: {table} with currency_id: {currency_id}")
#             get_currency = get_currency_details(currency_id)
#             print(f"Currency details for {table}: {currency_id}")
#         elif table not in ["dim_staff","dim_location","dim_design","dim_counterparty"]:
#            raise Exception("Dimension table names requested are not valid")
#         dimension_value_rows[table] =  database_connection.run(f'''
#         INSERT INTO {table}
#         SELECT {dimensions_insertion_queries[table]}
#         RETURNING *;
#         ''')
#         if dimension_value_rows[table] == []:
#           raise Exception("No paired keys to perform JOIN on")
#     # raise error if dimension_value_rows remains empty
#     if dimension_value_rows == {}:
#        raise Exception("No rows outputted")

#     # return as variable
#     return dimension_value_rows

# This is a dictionary of lists
# key = dimensions table names
# value[0] = headers
# value[1:] = dependencies
# for use in python, but written in SQL.


dimensions_tables_creation = {
  "dim_date":['''
  "date_id" date PRIMARY KEY NOT NULL,
  "year" int NOT NULL,
  "month" int NOT NULL,
  "day" int NOT NULL,
  "day_of_week" int NOT NULL,
  "day_name" varchar NOT NULL,
  "month_name" varchar NOT NULL,
  "quarter" int NOT NULL
''',"sales_order"],
  
"dim_staff":['''
  "staff_id" int PRIMARY KEY NOT NULL,
  "first_name" varchar NOT NULL,
  "last_name" varchar NOT NULL,
  "department_name" varchar NOT NULL,
  "location" varchar NOT NULL,
  "email_address" varchar NOT NULL
''',"staff","department"],

"dim_location":['''
  "location_id" int PRIMARY KEY NOT NULL,
  "address_line_1" varchar NOT NULL,
  "address_line_2" varchar,
  "district" varchar,
  "city" varchar NOT NULL,
  "postal_code" varchar NOT NULL,
  "country" varchar NOT NULL,
  "phone" varchar NOT NULL
''',"address"],

"dim_currency":['''
  "currency_id" int PRIMARY KEY NOT NULL,
  "currency_code" varchar NOT NULL,
  "currency_name" varchar NOT NULL
''',"currency"],

"dim_design":['''
  "design_id" int PRIMARY KEY NOT NULL,
  "design_name" varchar NOT NULL,
  "file_location" varchar NOT NULL,
  "file_name" varchar NOT NULL
''',"design"],

"dim_counterparty":['''
  "counterparty_id" int PRIMARY KEY NOT NULL,
  "counterparty_legal_name" varchar NOT NULL,
  "counterparty_legal_address_line_1" varchar NOT NULL,
  "counterparty_legal_address_line_2" varchar,
  "counterparty_legal_district" varchar,
  "counterparty_legal_city" varchar NOT NULL,
  "counterparty_legal_postal_code" varchar NOT NULL,
  "counterparty_legal_country" varchar NOT NULL,
  "counterparty_legal_phone_number" varchar NOT NULL
''',"counterparty","address"]
}

# This is a dictionary of lists
# key = dimensions table names
# value = SQL SELECT query
# for use in python, but written in SQL.
dimensions_insertion_queries = {
"dim_date":'''
created_at,last_updated,agreed_delivery_date,agreed_payment_date
FROM sales_order
''',
"dim_staff":'''
staff_id,first_name,last_name, department.department_name,department.location,email_address

FROM staff
JOIN department ON staff.department_id = department.department_id
''',
"dim_location":'''

address_id,address_line_1, address_line_2,district,city,postal_code,country,phone
FROM address
''',
"dim_currency":'''
currency_id, currency_code
FROM currency
''',
"dim_design":'''
design_id,design_name,file_location,file_name
FROM design
''',
"dim_counterparty":'''

counterparty_id,
counterparty_legal_name,
address.address_line_1,
address.address_line_2,
address.district,
address.city,
address.postal_code,
address.country,
address.phone
FROM counterparty
JOIN address
ON counterparty.legal_address_id = address.address_id
'''
}