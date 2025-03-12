import pandas as pd
import json
from src.dim_date_function import extract_date_info_from_dim_date
from pprint import pprint
from src.dimensions_dependents_set_up import load_local_files_for_testing,create_pandas_raw_tables,create_pandas_empty_dim_tables

# This file manipulates the data into dimension table format.

# NOT YET FINISHED - DO NOT TEST YET
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


        # df.drop_duplicates(inplace = True)

if __name__ == "__main__":
    import_files = load_local_files_for_testing()
    info_df_dict = create_pandas_raw_tables(import_files)
    empty_dim_tables = create_pandas_empty_dim_tables()
    populate_dim_dfs(info_df_dict,empty_dim_tables)