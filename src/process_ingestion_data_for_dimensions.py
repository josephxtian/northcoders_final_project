"""
This function will remodel code taken from the s3 ingestion bucket.
It will remodel it into the following dimension tables:
    * dim_staff
    * dim_location
    * dim_design
    * dim_date
    * dim_currency
    * dim_counterparty

(Note that data types in some columns may have to be changed to conform to the warehouse data model.)
"""

# FUNCTIONS REQUIRED FOR MAKING - ADD TO TO-DO LIST?
# get bucket name
# get 

import pg8000.native
from s3_read_function.s3_read_function import read_file_from_s3
from ingestion_to_processed_bucket.dimensions_tables import dimensions_tables_creation

bucket_name = "mock-bucket-name"

# FUNCTION

# table names

# take the data as variable
# for each table loop:

#   create tables for the dimensions tables using pg8000
#   sort it into the correct dim tables using pg8000
# return each dimension table as a variable


# dimension_table_names = ["dim_date", "dim_staff", "dim_location","dim_currency","dim_design","dim_counterparty"]
# dimension_tables = [read_file_from_s3(bucket_name, dim_tab_name) for dim_tab_name in dimension_table_names]


#"dim_date"
# "dim_staff", "dim_location","dim_currency","dim_design","dim_counterparty"
dim_currency = 

INSERT INTO dim_currency ()