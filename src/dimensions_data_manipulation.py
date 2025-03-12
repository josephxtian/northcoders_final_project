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
    # setup output dictionary
    filled_dim_tables = {}
    # dependent tables each dimension table relies on
    dimensions_dependencies = {
    "dim_date":["sales_order"],
    "dim_staff":["staff","department"],
    "dim_location":["address"],
    "dim_currency":["currency"],
    "dim_design":["design"],
    "dim_counterparty":["counterparty","address"]
    }
    # iterate through the keys of the dictionary
    for table_name, dataframe in df_dim_dictionary.items():
        print("TABLE>>>>>",table_name)
        if table_name == 'dim_date':
            dates = input_info_df["sales_order"][['created_at','last_updated','agreed_delivery_date','agreed_payment_date']]
            # print(dates)
            for date in dates:
                # print("DATE>>>>",dates[date])
                for indv_date in dates[date]:
                    formatted_date = extract_date_info_from_dim_date(indv_date)
        elif table_name == "dim_currency":
            continue
            # run currency operations
        # process all the other dim tables
        elif table_name in ["dim_staff", "dim_location", "dim_design", "dim_counterparty"]:
            # iterate through the input data staff information
            dim_table = df_dim_dictionary[table_name]
            no_of_dependencies = len(dimensions_dependencies[table_name])
            first_dependency = dimensions_dependencies[table_name][0]
            raw_info_df = input_info_df[first_dependency]
            if no_of_dependencies>1:
                for i in range(1,no_of_dependencies):
                    print(i,"<<<<<eye")
                    merging_dependency = dimensions_dependencies[table_name][i]
                    # define special case for counterparty joining key
                    if first_dependency == "counterparty":
                        left_join_key = "legal_address_id"
                    else:
                        left_join_key = input_info_df[merging_dependency].index.name
                    merged_df = raw_info_df.merge(input_info_df[merging_dependency], how="left",left_on=left_join_key,right_index=True)
                    dim_table = pd.concat([dim_table,merged_df], ignore_index=False,join='inner')
            else:
                dim_table = pd.concat([dim_table,raw_info_df], ignore_index=False,join='inner')
            print(dim_table,f"<<<<<DIMTABLE {table_name}")

            filled_dim_tables[table_name] = dim_table
            # print(filled_dim_tables,"<<<<<<FILLED_DIM_TABLES")
        else:
            # raise exception if dimension table name not valid
            raise Exception("Dimension table names requested are not valid")

    return filled_dim_tables

if __name__ == "__main__":
    import_files = load_local_files_for_testing()
    info_df_dict = create_pandas_raw_tables(import_files)
    empty_dim_tables = create_pandas_empty_dim_tables()
    populate_dim_dfs(info_df_dict,empty_dim_tables)