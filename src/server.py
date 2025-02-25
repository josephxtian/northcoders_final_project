from src.connection import connect_to_db, close_db_connection
import json
from pprint import pprint
import datetime
from pg8000.native import literal, identifier
import decimal


def connect_to_address_data():
    db = connect_to_db()


    list_of_tables = ['address', 'staff', 'department', 'design', 
                    'counterparty', 'sales_order', 'transaction', 'payment',
                    'purchase_order', 'payment_type', 'currency']
    
    for table in list_of_tables:
        db_run_column = db.run(f' SELECT * FROM {identifier(table)};') 
        columns = [col['name'] for col in db.columns]
        format_data = [dict(zip(columns,row)) for row in db_run_column]

        for row in format_data:
            for key in row:
                if isinstance(row[key], datetime.datetime):
                    row[key] = row[key].isoformat()
                if isinstance(row[key], decimal.Decimal):
                    row[key] = float(row[key])

        with open(f'"json-{table}.json"', "w") as file:
            json.dump(format_data, file, indent=4)
        
    
connect_to_address_data()