import pg8000.native
from s3_read_function.s3_read_function import read_file_from_s3

# This function will set up the dimensions tables
def set_up_dims_table(database_connection):
    for table in dimensions_tables_creation:
        database_connection.run(f'''
        DROP TABLE IF EXISTS {table};
        {dimensions_tables_creation[table]};
        ''')


def put_information_into_dims_schema(database_connection,*input_data):

    # use select statement to choose information required for star schema
    for table in dimensions_tables_creation:
        database_connection.run(f'''
        INSERT INTO {table} ({dim_tables_column_headers[table]})
        VALUES (
        SELECT
        created_at,
        last_updated,
        agreed_delivery_date,
        agreed_payment_date
        FROM purchase_order
        )
        ''')
    # return as variable

        '''INSERT INTO dim_date (date_id,year,month,day,day_of_week,day_name,month_name,quarter)
        VALUES (
        SELECT
        created_at,
        last_updated,
        agreed_delivery_date,
        agreed_payment_date
        FROM purchase_order
        )
        '''




#     # take the data as variable
#     # for each table loop:
#     for dim_table in dimensions_tables_creation:
#     # delete previous tables
#     # create tables for the dimensions tables
#         dims_db.run(f'''
#                 DROP TABLE IF EXISTS {dim_table};   
#                 {dim_tables_column_headers[dim_table]};
#                 ''')
        
#         # for item in 
#         dims_db.execute(f'''''')
                

#         # for dim_staff table
#         dims_db.execute(f'''
#         INSERT INTO dim_staff (
#             staff_id,
#             first_name,
#             last_name,
#             department_name,
#             location,
#             email_address
#         )
#         VALUES
#         (SELECT staff_id, first_name, last_name, department_name, location, email_address)
#         ''')


# #   sort it into the correct dim tables using pg8000
# # return each dimension table as a variable


# # dimension_table_names = ["dim_date", "dim_staff", "dim_location","dim_currency","dim_design","dim_counterparty"]
# # dimension_tables = [read_file_from_s3(bucket_name, dim_tab_name) for dim_tab_name in dimension_table_names]


# #"dim_date"
# # "dim_staff", "dim_location","dim_currency","dim_design","dim_counterparty"


# This is a dictionary of dimensions tables 
# for use in python, but written in SQL.
# It uses block strings to hold the key-value pairs.
# Below it are lists of column headers

dimensions_tables_creation = {
  "dim_date":
'''CREATE TABLE "dim_date" (
  "date_id" date PRIMARY KEY NOT NULL,
  "year" int NOT NULL,
  "month" int NOT NULL,
  "day" int NOT NULL,
  "day_of_week" int NOT NULL,
  "day_name" varchar NOT NULL,
  "month_name" varchar NOT NULL,
  "quarter" int NOT NULL
);''',

"dim_staff":
'''CREATE TABLE "dim_staff" (
  "staff_id" int PRIMARY KEY NOT NULL,
  "first_name" varchar NOT NULL,
  "last_name" varchar NOT NULL,
  "department_name" varchar NOT NULL,
  "location" varchar NOT NULL,
  "email_address" varchar NOT NULL
);''',

"dim_location":
'''CREATE TABLE "dim_location" (
  "location_id" int PRIMARY KEY NOT NULL,
  "address_line_1" varchar NOT NULL,
  "address_line_2" varchar,
  "district" varchar,
  "city" varchar NOT NULL,
  "postal_code" varchar NOT NULL,
  "country" varchar NOT NULL,
  "phone" varchar NOT NULL
);''',

"dim_currency":
'''CREATE TABLE "dim_currency" (
  "currency_id" int PRIMARY KEY NOT NULL,
  "currency_code" varchar NOT NULL,
  "currency_name" varchar NOT NULL
);''',

"dim_design":
'''CREATE TABLE "dim_design" (
  "design_id" int PRIMARY KEY NOT NULL,
  "design_name" varchar NOT NULL,
  "file_location" varchar NOT NULL,
  "file_name" varchar NOT NULL
);''',

"dim_counterparty":
'''CREATE TABLE "dim_counterparty" (
  "counterparty_id" int PRIMARY KEY NOT NULL,
  "counterparty_legal_name" varchar NOT NULL,
  "counterparty_legal_address_line_1" varchar NOT NULL,
  "counterparty_legal_address_line_2" varchar,
  "counterparty_legal_district" varchar,
  "counterparty_legal_city" varchar NOT NULL,
  "counterparty_legal_postal_code" varchar NOT NULL,
  "counterparty_legal_country" varchar NOT NULL,
  "counterparty_legal_phone_number" varchar NOT NULL
);'''
}


dim_tables_column_headers = [
  ["dim_date",
  "date_id",
  "year",
  "month",
  "day",
  "day_of_week",
  "day_name",
  "month_name",
  "quarter"],

  ["dim_staff",
  "staff_id",
  "first_name",
  "last_name" ,
  "department_name",
  "location",
  "email_address"],

  ["dim_location",
  "location_id",
  "address_line_1",
  "address_line_2",
  "district",
  "city",
  "postal_code",
  "country",
  "phone"],

  ["dim_currency",
  "currency_id",
  "currency_code",
  "currency_name"],


  ["dim_design",
  "design_id",
  "design_name",
  "file_location",
  "file_name"],

  ["dim_counterparty",
  "counterparty_id",
  "counterparty_legal_name",
  "counterparty_legal_address_line_1",
  "counterparty_legal_address_line_2",
  "counterparty_legal_district",
  "counterparty_legal_city",
  "counterparty_legal_postal_code",
  "counterparty_legal_country",
  "counterparty_legal_phone_number"]
]