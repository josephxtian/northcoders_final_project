import pandas as pd
import json
from pprint import pprint

def create_pandas_raw_tables():
    info_df_dict = {}
    file_list = ["address", "counterparty", "currency","department", "design","payment", "payment_type", "purchase_order","sales_order", "staff", "transaction"]
    for file in file_list:
        with open(f'json_data/json-{file}.json','r') as f:
            import_dict = json.load(f)

        info_df_dict[file] = pd.DataFrame(import_dict[file])
        print(info_df_dict)

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
    print(dim_date_df)

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
            
    dim_staff_df = pd.DataFrame({
        'staff_id': pd.Series(dtype='int'),
        'first_name': pd.Series(dtype='str'),
        'last_name': pd.Series(dtype='str'),
        'department_name': pd.Series(dtype='str'),
        'location': pd.Series(dtype='str'),
        'email_address': pd.Series(dtype='str')
        })
            
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
            
    dim_currency_df = pd.DataFrame({
        'currency_id': pd.Series(dtype='int'),
        'currency_code': pd.Series(dtype='str'),
        'currency_name': pd.Series(dtype='str') 
        })
            
    dim_design_df = pd.DataFrame({
        'design_id': pd.Series(dtype='int'),
        'design_name': pd.Series(dtype='str'),
        'file_location': pd.Series(dtype='str'),
        'file_name': pd.Series(dtype='str') 
        })
            
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

    print(dim_date_df,dim_staff_df,dim_location_df,dim_currency_df,dim_design_df,dim_counterparty_df)


if __name__ == "__main__":
    create_pandas_raw_tables()
    create_pandas_empty_dim_tables()