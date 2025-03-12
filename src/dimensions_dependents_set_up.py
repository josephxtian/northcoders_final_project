import pandas as pd
import json
from pprint import pprint
from src.dim_date_function import extract_date_info_from_dim_date

# This file contains code using pandas to create dataframes from the raw ingestion bucket.
# It manipulates the data into dimension table format.

# Create tables using the raw data from the ingestion bucket
# Returns a dictionary with keys of <table_name>
# Note for testing this currently retrieves information from local files.
def create_pandas_raw_tables():
    info_df_dict = {}
    file_list = ["address", "counterparty", "currency","department", "design","payment", "payment_type", "purchase_order","sales_order", "staff", "transaction"]
    for file in file_list:
        with open(f'json_data/json-{file}.json','r') as f:
            import_dict = json.load(f)
        
        info_df = pd.DataFrame(import_dict[file])
        info_df.set_index(list(info_df)[0],inplace=True)

        info_df_dict[file] = info_df
    return info_df_dict

# Creates empty dim tables with data types set.
# The index column is set to the ID value
# Returns a dictionary with keys of dim_<table_name>
def create_pandas_empty_dim_tables():
    dim_date_df = pd.DataFrame({
        'date_id': pd.Series(dtype='datetime64[ns]'),
        'year': pd.Series(dtype='int'),
        'month': pd.Series(dtype='int'),
        'day': pd.Series(dtype='int'),
        'day_of_week': pd.Series(dtype='int'),
        'day_name': pd.Series(dtype='str'),
        'month_name': pd.Series(dtype='str'),
        'quarter': pd.Series(dtype='int')
        })
    dim_date_df.set_index('date_id',inplace=True)
            
    dim_staff_df = pd.DataFrame({
        'staff_id': pd.Series(dtype='int'),
        'first_name': pd.Series(dtype='str'),
        'last_name': pd.Series(dtype='str'),
        'department_name': pd.Series(dtype='str'),
        'location': pd.Series(dtype='str'),
        'email_address': pd.Series(dtype='str')
        })
    dim_staff_df.set_index('staff_id',inplace=True)
            
    dim_location_df = pd.DataFrame({
        'location_id': pd.Series(dtype='int'),
        'address_line_1': pd.Series(dtype='str'),
        'address_line_2': pd.Series(dtype='str'),
        'district': pd.Series(dtype='str'),
        'city': pd.Series(dtype='str'),
        'postal_code': pd.Series(dtype='str'),
        'country': pd.Series(dtype='str'),
        'phone': pd.Series(dtype='str') 
        })
    dim_location_df.set_index('location_id',inplace=True)
            
    dim_currency_df = pd.DataFrame({
        'currency_id': pd.Series(dtype='int'),
        'currency_code': pd.Series(dtype='str'),
        'currency_name': pd.Series(dtype='str') 
        })
    dim_currency_df.set_index('currency_id',inplace=True)
            
    dim_design_df = pd.DataFrame({
        'design_id': pd.Series(dtype='int'),
        'design_name': pd.Series(dtype='str'),
        'file_location': pd.Series(dtype='str'),
        'file_name': pd.Series(dtype='str') 
        })
    dim_design_df.set_index('design_id',inplace=True)
            
    dim_counterparty_df = pd.DataFrame({
        'counterparty_id': pd.Series(dtype='int'),
        'counterparty_legal_name': pd.Series(dtype='str'),
        'counterparty_legal_address_line_1': pd.Series(dtype='str'),
        'counterparty_legal_address_line_2': pd.Series(dtype='str'),
        'counterparty_legal_district': pd.Series(dtype='str'),
        'counterparty_legal_city': pd.Series(dtype='str'),
        'counterparty_legal_postal_code': pd.Series(dtype='str'),
        'counterparty_legal_country': pd.Series(dtype='str'),
        'counterparty_legal_phone_number': pd.Series(dtype='str') 
        })
    dim_counterparty_df.set_index('counterparty_id',inplace=True)

    df_dim_dictionary = {
        'dim_date':dim_date_df,
        'dim_staff':dim_staff_df,
        'dim_location':dim_location_df,
        'dim_currency':dim_currency_df,
        'dim_design':dim_design_df,
        'dim_counterparty':dim_counterparty_df
    }

    print(df_dim_dictionary)
    return df_dim_dictionary

# Populates dim_tables
# Takes input of raw ingestion bucket 
def populate_dim_dfs(input_info_df,df_dim_dictionary):
    print(input_info_df,"<<<<<input_info_df")
    for table in df_dim_dictionary:
        print("TABLE>>>>>",table)
        if table == 'dim_date':
            dates = input_info_df["sales_order"][['created_at','last_updated','agreed_delivery_date','agreed_payment_date']]
            for date in dates:
                print("DATE>>>>",dates[date])
                for indv_date in dates[date]:
                    formatted_date = extract_date_info_from_dim_date(indv_date)
                    print(formatted_date)

if __name__ == "__main__":
    info_df_dict = create_pandas_raw_tables()
    empty_dim_tables = create_pandas_empty_dim_tables()
    populate_dim_dfs(info_df_dict,empty_dim_tables)