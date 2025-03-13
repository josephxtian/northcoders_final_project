import pandas as pd
import json
from utils.get_bucket import get_bucket_name
import logging
from pprint import pprint
from src.dimensions_dependents_set_up import load_local_files_for_testing,create_pandas_raw_tables,create_pandas_empty_dim_tables
from src.get_currency_name import add_currency_names
from src.dim_date_function import extract_date_info_from_dim_date
from src.dimensions_data_manipulation import populate_dim_dfs_2

"""
This function is a new alternative for the fact_sales_order data. We originally tried to do this using sql and local postgres databases but realised this wouldn't work in Lambda

This approach uses purely Pandas to achieve the necessary transformations

This function relies on Dataframes having already been created as there is no longer a database to reference this from
"""


def transform_fact_data(raw_data, dim_tables_dict):

# loads the raw sales_order data into a pandas data frame (if exists)
    transformed_data = []

    try:
        # Ensure sales_order exists in raw data
        if "sales_order" not in raw_data or not raw_data["sales_order"]:
            logging.warning("No sales_order data found in raw data")
            return pd.DataFrame()
        
        df = pd.DataFrame(raw_data["sales_order"])

        print("Available columns in sales_order DataFrame:", df.columns)
        print("Raw sales_order data sample:", df.head())

        df["created_date"] = pd.to_datetime(df["created_at"]).dt.date
        df["created_time"] = pd.to_datetime(df["created_at"]).dt.time
        
        # df["last_updated_date"] = pd.to_datetime(df["last_updated"])
        df["last_updated_date"] = pd.to_datetime(df["last_updated"]).dt.date
        df["last_updated_time"] = pd.to_datetime(df["last_updated"]).dt.time

        print("Available columns in sales_order DataFrame:", df.columns)
        print("Raw sales_order data sample:", raw_data.get("sales_order", [])[:5])
    
        df["agreed_payment_date"] = pd.to_datetime(df["agreed_payment_date"]).dt.date
        df["agreed_delivery_date"] = pd.to_datetime(df["agreed_delivery_date"]).dt.date
        
        dim_date = dim_tables_dict.get("dim_date", pd.DataFrame())
        dim_staff = dim_tables_dict.get("dim_staff", pd.DataFrame())
        dim_counterparty = dim_tables_dict.get("dim_counterparty", pd.DataFrame())
        dim_currency = dim_tables_dict.get("dim_currency", pd.DataFrame())
        dim_design = dim_tables_dict.get("dim_design", pd.DataFrame())
        dim_location = dim_tables_dict.get("dim_location", pd.DataFrame())
        
        if not dim_date.empty and "date_id" in dim_date.columns:
            dim_date["date_id"] = pd.to_datetime(dim_date["date_id"]).dt.date

        # Extract source file names dynamically
        # for file_key, data_list in raw_data.items():
        #     if not data_list:
        #         logging.warning(f"Skipping {file_key}: Empty sales_order data.")
        #         continue

        #     try:
        #         df = pd.DataFrame(data_list)  # Assuming data_list is already a list of dictionaries
        #     except Exception as e:
        #         logging.error(f"Skipping {file_key}: Error creating DataFrame - {e}")
        #         continue

        #     # extracts information from created_at and separates it into data and time columns

        #     # df["created_date"] = pd.to_datetime(df["created_at"])
        #     df["created_date"] = pd.to_datetime(df["created_at"]).dt.date
        #     df["created_time"] = pd.to_datetime(df["created_at"]).dt.time
            
        #     # df["last_updated_date"] = pd.to_datetime(df["last_updated"])
        #     df["last_updated_date"] = pd.to_datetime(df["last_updated"]).dt.date
        #     df["last_updated_time"] = pd.to_datetime(df["last_updated"]).dt.time

        #     print("Available columns in sales_order DataFrame:", df.columns)
        #     print("Raw sales_order data sample:", raw_data.get("sales_order", [])[:5])
        
        #     df["agreed_payment_date"] = pd.to_datetime(df["agreed_payment_date"]).dt.date
        #     df["agreed_delivery_date"] = pd.to_datetime(df["agreed_delivery_date"]).dt.date

            # print("Raw agreed_delivery_date before merge:")
            # print(df["agreed_delivery_date"].unique())

            # joins relevant dim date based on key constraints present in ERD

        df = df.merge(dim_date, left_on="created_date", right_on="date_id", how="left", suffixes=("", "_created"))
        df = df.merge(dim_date, left_on="last_updated_date", right_on="date_id", how="left", suffixes=("", "_updated"))
        df = df.merge(dim_date, left_on="agreed_payment_date", right_on="date_id", how="left", suffixes=("", "_payment"))
        df = df.merge(dim_date, left_on="agreed_delivery_date", right_on="date_id", how="left", suffixes=("", "_delivery"))

        # missing_dates = df[df["date_id_delivery"].isna()]
        # print("Missing agreed_delivery_date values after merge:")
        # print(missing_dates[["agreed_delivery_date"]])

        # does the same for staff, counterparty, currency, design and location

        df = df.merge(dim_staff, left_on="sales_staff_id", right_on="staff_id", how="left")
        df = df.merge(dim_counterparty, left_on="counterparty_id", right_on="counterparty_id", how="left")
        df = df.merge(dim_currency, left_on="currency_id", right_on="currency_id", how="left")
        df = df.merge(dim_design, left_on="design_id", right_on="design_id", how="left")
        df = df.merge(dim_location, left_on="agreed_delivery_location_id", right_on="location_id", how="left")

        df = df[[
            "sales_order_id", "date_id", "created_time", "date_id_updated", "last_updated_time",
            "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id",
            "design_id", "date_id_payment", "date_id_delivery", "location_id"
        ]].rename(columns={
            "date_id": "created_date",
            "date_id_updated": "last_updated_date",
            "date_id_payment": "agreed_payment_date",
            "date_id_delivery": "agreed_delivery_date"
        })
        #     # appends df to transformed_data variable - may be redundant
        #     transformed_data.append(df)

        # if transformed_data:
        #     final_df = pd.concat(transformed_data, ignore_index=True)
        # else:
        #     final_df = pd.DataFrame()  # Empty DataFrame if no data was processed

        # logging.info(f"Transformed sales_order data shape: {final_df.shape}")
        fact_sales_order_df = df
        pprint(fact_sales_order_df)
        return fact_sales_order_df

    except Exception as e:
        logging.error(f"Error during sales_order transformation: {e}")
        raise


import pandas as pd
import logging

def transform_fact_data_2(raw_data, dim_tables_dict):
    """
    Transforms raw sales_order data into the fact_sales_order schema.

    Args:
        raw_data (dict): Dictionary containing raw sales_order data.
        dim_tables_dict (dict): Dictionary containing dimension tables as Pandas DataFrames.

    Returns:
        pd.DataFrame: Transformed fact_sales_order data.
    """
    try:
        # Ensure sales_order exists in raw data
        if "sales_order" not in raw_data or not raw_data["sales_order"]:
            logging.warning("No sales_order data found in raw data")
            return pd.DataFrame()

        df = pd.DataFrame(raw_data["sales_order"])

        # Convert date-time columns
        df["created_date"] = pd.to_datetime(df["created_at"]).dt.date
        df["created_time"] = pd.to_datetime(df["created_at"]).dt.time
        df["last_updated_date"] = pd.to_datetime(df["last_updated"]).dt.date
        df["last_updated_time"] = pd.to_datetime(df["last_updated"]).dt.time
        df["agreed_payment_date"] = pd.to_datetime(df["agreed_payment_date"]).dt.date
        df["agreed_delivery_date"] = pd.to_datetime(df["agreed_delivery_date"]).dt.date

        # Extract dimension tables from the dictionary
        dim_date = dim_tables_dict.get("dim_date", pd.DataFrame())
        dim_staff = dim_tables_dict.get("dim_staff", pd.DataFrame())
        dim_counterparty = dim_tables_dict.get("dim_counterparty", pd.DataFrame())
        dim_currency = dim_tables_dict.get("dim_currency", pd.DataFrame())
        dim_design = dim_tables_dict.get("dim_design", pd.DataFrame())
        dim_location = dim_tables_dict.get("dim_location", pd.DataFrame())

        # Ensure dim_date has correct format
        if not dim_date.empty and "date_id" in dim_date.columns:
            dim_date["date_id"] = pd.to_datetime(dim_date["date_id"]).dt.date

        # Merge sales_order with dimension tables
        df = df.merge(dim_date, left_on="created_date", right_on="date_id", how="left", suffixes=("", "_created"))
        df = df.merge(dim_date, left_on="last_updated_date", right_on="date_id", how="left", suffixes=("", "_updated"))
        df = df.merge(dim_date, left_on="agreed_payment_date", right_on="date_id", how="left", suffixes=("", "_payment"))
        df = df.merge(dim_date, left_on="agreed_delivery_date", right_on="date_id", how="left", suffixes=("", "_delivery"))

        # Merge with other dimension tables
        df = df.merge(dim_staff, left_on="sales_staff_id", right_on="staff_id", how="left")
        df = df.merge(dim_counterparty, left_on="counterparty_id", right_on="counterparty_id", how="left")
        df = df.merge(dim_currency, left_on="currency_id", right_on="currency_id", how="left")
        df = df.merge(dim_design, left_on="design_id", right_on="design_id", how="left")
        df = df.merge(dim_location, left_on="agreed_delivery_location_id", right_on="location_id", how="left")

        # Select and rename final columns
        df = df[[
            "sales_order_id", "date_id", "created_time", "date_id_updated", "last_updated_time",
            "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id",
            "design_id", "date_id_payment", "date_id_delivery", "location_id"
        ]].rename(columns={
            "date_id": "created_date",
            "date_id_updated": "last_updated_date",
            "date_id_payment": "agreed_payment_date",
            "date_id_delivery": "agreed_delivery_date"
        })

        # Optimize memory usage
        df["sales_order_id"] = df["sales_order_id"].astype("int32")
        df["staff_id"] = df["staff_id"].astype("int16")
        df["counterparty_id"] = df["counterparty_id"].astype("int16")
        df["units_sold"] = df["units_sold"].astype("int32")
        df["unit_price"] = df["unit_price"].astype("float32")
        df["currency_id"] = df["currency_id"].astype("int8")
        df["design_id"] = df["design_id"].astype("int16")
        df["location_id"] = df["location_id"].astype("int16")

        fact_sales_order_df = df
        logging.info(f"Transformed sales_order data shape: {fact_sales_order_df.shape}")
        print("\n==== Transformed fact_sales_order DataFrame ====")
        pprint(fact_sales_order_df.head(10).to_dict(orient="records"))  # Print first 10 rows in readable format
        print("\n================================================\n")

        return fact_sales_order_df

    except Exception as e:
        logging.error(f"Error during sales_order transformation: {e}")
        raise



if __name__ == "__main__":
    import_files = load_local_files_for_testing()
    info_df_dict = create_pandas_raw_tables(import_files)
    empty_dim_tables = create_pandas_empty_dim_tables()
    dim_dataframes = populate_dim_dfs_2(info_df_dict,empty_dim_tables)
    transform_fact_data(import_files, dim_dataframes)




# Deserialise Json data string into a format that can be converted to a pandas dataframe
# Perform necessary transformations based on key constraints from dim_data tables

# s3_bucket = get_bucket_name("ingestion-bucket")

# read_sales_order_from_s3(s3_bucket)


test_raw_data = {
    "sales_order": [
            {
                "sales_order_id": 1001,
                "created_at": "2024-01-01T10:15:00",
                "last_updated": "2024-01-02T12:30:00",
                "sales_staff_id": 101,
                "counterparty_id": 201,
                "units_sold": 10,
                "unit_price": 50.00,
                "currency_id": 1,
                "design_id": 501,
                "agreed_payment_date": "2024-01-05",
                "agreed_delivery_date": "2024-01-10",
                "agreed_delivery_location_id": 1
            },
            {
                "sales_order_id": 1002,
                "created_at": "2024-01-02T09:00:00",
                "last_updated": "2024-01-03T14:45:00",
                "sales_staff_id": 102,
                "counterparty_id": 202,
                "units_sold": 5,
                "unit_price": 120.00,
                "currency_id": 2,
                "design_id": 502,
                "agreed_payment_date": "2024-01-07",
                "agreed_delivery_date": "2024-01-12",
                "agreed_delivery_location_id": 2
            },
            {
                "sales_order_id": 1003,
                "created_at": "2024-01-03T11:20:00",
                "last_updated": "2024-01-04T15:00:00",
                "sales_staff_id": 103,
                "counterparty_id": 203,
                "units_sold": 8,
                "unit_price": 75.50,
                "currency_id": 3,
                "design_id": 503,
                "agreed_payment_date": "2024-01-08",
                "agreed_delivery_date": "2024-01-15",
                "agreed_delivery_location_id": 3
            }
        ]
    }



dim_date = pd.DataFrame({
    "date_id": pd.date_range(start="2024-01-01", periods=31, freq="D"),
    "year": [2024] * 31,
    "month": [1] * 31,
    "day": list(range(1, 32)),
    "day_of_week": [1, 2, 3, 4, 5, 6, 7] * 4 + [1, 2, 3],  
    "day_name": [
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    ] * 4 + ["Monday", "Tuesday", "Wednesday"], 
    "month_name": ["January"] * 31,
    "quarter": [1] * 31
})


dim_staff = pd.DataFrame({
    "staff_id": [101, 102, 103],
    "first_name": ["Alice", "Bob", "Charlie"],
    "last_name": ["Smith", "Jones", "Brown"],
    "department_name": ["Sales", "HR", "Engineering"],
    "location": ["New York", "London", "Berlin"],
    "email_address": ["alice.smith@example.com", "bob.jones@example.com", "charlie.brown@example.com"]
})

dim_counterparty = pd.DataFrame({
    "counterparty_id": [201, 202, 203],
    "counterparty_legal_name": ["Acme Corp", "Globex Inc", "Initech Ltd"],
    "counterparty_legal_address_line_1": ["10 Market St", "500 Tower Ave", "42 Startup Rd"],
    "counterparty_legal_address_line_2": [None, "Suite 300", "Apt 12C"],
    "counterparty_legal_district": ["Financial District", None, "Tech Valley"],
    "counterparty_legal_city": ["New York", "London", "San Francisco"],
    "counterparty_legal_postal_code": ["10005", "WC2N 5DU", "94107"],
    "counterparty_legal_country": ["USA", "UK", "USA"],
    "counterparty_legal_phone_number": ["+1-212-555-6789", "+44-20-7946-5678", "+1-415-555-9876"]
})

dim_currency = pd.DataFrame({
    "currency_id": [1, 2, 3],
    "currency_code": ["USD", "GBP", "EUR"],
    "currency_name": ["US Dollar", "British Pound", "Euro"]
})

dim_design = pd.DataFrame({
    "design_id": [501, 502, 503],
    "design_name": ["Modern Chair", "Luxury Sofa", "Wooden Table"],
    "file_location": ["/designs/chair.png", "/designs/sofa.png", "/designs/table.png"],
    "file_name": ["chair.png", "sofa.png", "table.png"]
})

dim_location = pd.DataFrame({
    "location_id": [1, 2, 3],
    "address_line_1": ["123 Main St", "456 High St", "789 Elm St"],
    "address_line_2": ["Suite 100", None, "Apt 3B"],
    "district": ["Downtown", "Central", None],
    "city": ["New York", "London", "Berlin"],
    "postal_code": ["10001", "EC1A 1BB", "10115"],
    "country": ["USA", "UK", "Germany"],
    "phone": ["+1-212-555-1234", "+44-20-7946-0123", "+49-30-5555-7890"]
})

# transform_fact_data(test_raw_data, dim_date, dim_staff, dim_counterparty, dim_currency, dim_design, dim_location)

