from db_setup.connection import connect_db,close_db
import json
from pg8000.native import identifier, literal

# This file seeds the test database from json files in json_data/test_data/

def seed_db(env='test'):
    print("\U0001FAB4", "Seeding Database...")
    db = connect_db()
    try:
        # drop all existing tables
        table_names = ["address","counterparty","currency","department","design","payment_type","payment","purchase_order","sales_order","staff","transaction"]
        for table_name in table_names:
            db.run("DROP TABLE IF EXISTS :table_name",table_name=table_name)
            print(f"dropped {table_name}")

        return_statement = {}

        table_count = 0
        for table_name in table_names:
        # open json file
            with open(f'json_data/{env}_data/json-{table_name}.json','r') as f:
                table_data = json.load(f)[table_name]
            # get column names
            column_names = list(table_data[0].keys())
            # add datatypes to column names
            column_names_with_types = []
            for name in column_names:
                if name.find('_id')>=0:
                    column_names_with_types.append(name + " int")
                else:
                    column_names_with_types.append(name + " text")
                # create SQL syntaxed string
                headers_string = str(tuple(column_names_with_types)).replace("'","")
                # create table and feedback to user
                db.run(f'''
                    CREATE TABLE IF NOT EXISTS {identifier(table_name)} {headers_string};
                    ''')
                print(f"{table_name} table created, with columns {headers_string}")

            # populate table with data
            row_count = 0
            for row in table_data:
                row_id_header = list(row.keys())[0]
                db.run(f'''
                INSERT INTO {identifier(table_name)} ({row_id_header})
                VALUES (:row_id);
                ''',row_id=row[row_id_header])
                for key in row:
                    if key !=row_id_header:
                        db.run(f'''
                        UPDATE {identifier(table_name)}
                        SET {identifier(key)} = {literal(row[key])}
                        WHERE {identifier(row_id_header)} = {literal(row[row_id_header])}
                        ''')
                row_count += 1
            return_statement[f'{table_name}_row_count'] = row_count
            print(
            f'\U0001F4BE Successfully seeded {row_count} rows to {table_name} table in the database. \U0001F44D')
            table_count += 1
        return_statement["table_count"] = table_count
        # return number of tables and rows made
        return return_statement
    # raise exception if there is an error making db
    except Exception as e:
        print(e)
        raise e

    finally:
        close_db(db)