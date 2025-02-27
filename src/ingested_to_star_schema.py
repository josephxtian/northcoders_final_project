"""
this function will take code from the s3 ingestion bucket and 
remodel it into the following dimension tables:
    * dim_staff
    * dim_location
    * dim_design
    * dim_date
    * dim_currency
    * dim_counterparty

(Note that data types in some columns may have to be changed to conform to the warehouse data model.)
"""
# INITIAL SETUP
# create tables for the dimensions tables


# get request to database file in the s3 bucket
# take the data and sort it into the correct dim tables
# turn back into a file
# upload it to the processed_bucket
