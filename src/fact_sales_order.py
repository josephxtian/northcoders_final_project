import pandas as pd
import json
from src.connection import connect_to_db, close_db_connection

"""""
This function reads from the S3 ingress bucket and converts the raw data tables into the fact_sales_order star schema

This function will run parallel to Joe's dim table conversion function

The schema should appear as follows:

fact_sales_order
dim_staff
dim_location
dim_design
dim_date
dim_currency
dim_counterparty

"""""

def create_fact_sales_order_table():
    """
    Creates the fact_sales_order table
    """
    create_table_query = """"
    CREATE TABLE IF NOT EXISTS "fact_sales_order" (
        "sales_record_id" SERIAL PRIMARY KEY,
        "sales_order_id" int NOT NULL,
        "created_date" date NOT NULL,
        "created_time" time NOT NULL,
        "last_updated_date" date NOT NULL,
        "last_updated_time" time NOT NULL,
        "sales_staff_id" int NOT NULL,
        "counterparty_id" int NOT NULL,
        "units_sold" int NOT NULL,
        "unit_price" "numeric(10, 2)" NOT NULL,
        "currency_id" int NOT NULL,
        "design_id" int NOT NULL,
        "agreed_payment_date" date NOT NULL,
        "agreed_delivery_date" date NOT NULL,
        "agreed_delivery_location_id" int NOT NULL
        );

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("created_date") REFERENCES "dim_date" ("date_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("last_updated_date") REFERENCES "dim_date" ("date_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("sales_staff_id") REFERENCES "dim_staff" ("staff_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("counterparty_id") REFERENCES "dim_counterparty" ("counterparty_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("currency_id") REFERENCES "dim_currency" ("currency_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("design_id") REFERENCES "dim_design" ("design_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_payment_date") REFERENCES "dim_date" ("date_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_delivery_date") REFERENCES "dim_date" ("date_id");

    ALTER TABLE "fact_sales_order" ADD FOREIGN KEY ("agreed_delivery_location_id") REFERENCES "dim_location" ("location_id");
    """
    conn = connect_to_db()

    try:
        conn.execute(create_table_query)
        print("`fact_sales_order` created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        # test 3 failed as I was only printing the exception rather than raising it - changed to raise
        raise
    finally:
        close_db_connection(conn)

if __name__ == "__main__":
    create_fact_sales_order_table()
    

# 0. AWS configuration variables set to the s3 ingestion bucket and variables for db_host, db_name, db_user,db_password
# 1. read the raw data from the ingestion bucket. Possibly a bot3 client.get
def transform_fact_data(raw_data, return_dataframe=True):
    # use pandas to convert raw json data to staging dataframe

    

    conn = connect_to_db()
    # variable that can be returned ready to be passed into s3 write L2 util function
    transformed_data = []
# 2. query string variable that contains the sql necessary for schema conversion - possible separate function to loading using pandas
    try:
        df = pd.DataFrame(raw_data)
        df.to_sql("staging_fact_sales_order", conn, if_exists="replace", index=False)
        print("Raw JSON data loaded into `staging_fact_sales_order`")

# 3. The query string should join the fact table to the ids of each respective dim tables
        transformation_query = """
        INSERT INTO fact_sales_order (
            sales_order_id, created_date, created_time, last_updated_date, last_updated_time,
            sales_staff_id, counterparty_id, units_sold, unit_price, currency_id,
            design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id
        )
        SELECT 
            s.sales_order_id, 
            d1.date_id AS created_date, 
            s.created_time, 
            d2.date_id AS last_updated_date, 
            s.last_updated_time,
            staff.staff_id, 
            cp.counterparty_id, 
            s.units_sold, 
            s.unit_price, 
            curr.currency_id, 
            des.design_id, 
            d3.date_id AS agreed_payment_date, 
            d4.date_id AS agreed_delivery_date, 
            loc.location_id
        FROM staging_fact_sales_order s
        JOIN dim_date d1 ON s.created_date = d1.date
        JOIN dim_date d2 ON s.last_updated_date = d2.date
        JOIN dim_date d3 ON s.agreed_payment_date = d3.date
        JOIN dim_date d4 ON s.agreed_delivery_date = d4.date
        JOIN dim_staff staff ON s.sales_staff_id = staff.staff_id
        JOIN dim_counterparty cp ON s.counterparty_id = cp.counterparty_id
        JOIN dim_currency curr ON s.currency_id = curr.currency_id
        JOIN dim_design des ON s.design_id = des.design_id
        JOIN dim_location loc ON s.agreed_delivery_location_id = loc.location_id
        RETURNING *;
        """
        # 4. The fact sales order table data should be returned ready to be passed into lambda 3 that loads into the processed bucket
        result = conn.execute(transformation_query)
        transformed_data = [dict(row) for row in result.mappings()]
        print("Transformation successful")
    except Exception as e:
        print(f"Error populating table: {e}")
        raise
    finally:
        close_db_connection(conn)
    return transformed_data





    
    