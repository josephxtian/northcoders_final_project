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
# DEPENDENCIES
# use get_request_function to access files in the s3 bucket
# pg8000

# FUNCTION
# take the data as variable
# for each table loop:
#   create tables for the dimensions tables using pg8000
#   sort it into the correct dim tables using pg8000
# return each dimension table as a variable

# MAYBE OUT OF SCOPE
# turn back into a file
# upload it to the processed_bucket