from src.connection import connect_to_db, close_db_connection
import json
from pprint import pprint
from datetime import datetime
from pg8000.native import literal, identifier
import decimal


def reformat_data_to_json(table):
    db = connect_to_db()
    db_run_column = db.run(f" SELECT * FROM {identifier(table)};")
    columns = [col["name"] for col in db.columns]
    format_data = [dict(zip(columns, row)) for row in db_run_column]
    for row in format_data:
        for key in row:
            if isinstance(row[key], datetime):
                row[key] = row[key].strftime('%Y-%m-%dT%H:%M:%S.%f')
            if isinstance(row[key], decimal.Decimal):
                row[key] = float(row[key])
    close_db_connection(db)
    return format_data

#for testing:

def table_reformat():
    list_of_tables = [
        "address",
        "staff",
        "department",
        "design",
        "counterparty",
        "sales_order",
        "transaction",
        "payment",
        "purchase_order",
        "payment_type",
        "currency",
    ]
    for table in list_of_tables:
        format_data = reformat_data_to_json(table)

        with open(f"./json_data/json-{table}.json", "w") as file:
            json.dump({f"{table}": format_data}, file, indent=4)

table_reformat()


