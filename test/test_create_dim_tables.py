from src.create_dim_tables import set_up_dims_table, put_info_into_dims_schema
import pg8000.native
from src.connection import connect_to_db, close_db_connection
from src.create_temporary_tables import make_temporary_tables

# Table headers defined at bottom of document

class TestSetUpDimsTable:
    # test 6 tables have been created
    def test_six_tables_created(self):
        test_db = connect_to_db()
        test_input = ["address","counterparty","currency","department","design","sales_order","staff"]
        set_up_dims_table(test_db,test_input)
        for table in dim_tables_column_headers:
            test_db.run(f"SELECT * FROM {dim_tables_column_headers[table][0]};")
            column_headers = [c['name'] for c in test_db.columns]
            assert column_headers == dim_tables_column_headers[table]
        close_db_connection(test_db)

    def test_tables_are_empty(self):
        test_db = connect_to_db()
        test_input = ["address","counterparty","currency","department","design","sales_order","staff"]
        set_up_dims_table(test_db,test_input)
        for table in dim_tables_column_headers:
            result = test_db.run(f"SELECT * FROM {table[0]};")
            assert result == []
        close_db_connection(test_db)


class TestPutInformationIntoDimsSchema():
    # test 
    def test_insertion_for_one_table(self):
        test_input = {"staff":[
        {"staff_id": 1,
         "first_name": "Jeremie",
         "last_name": "Franey",
         "department_id": 2,
         "email_address": "jeremie.franey@terrifictotes.com",
         "created_at": "2022-11-03T14:20:51.563000",
         "last_updated": "2022-11-03T14:20:51.563000"}],
         "department": [
        {"department_id": 1,
         "department_name": "Sales",
         "location": "Manchester",
         "manager": "Richard Roma",
         "created_at": "2022-11-03T14:20:49.962000",
         "last_updated": "2022-11-03T14:20:49.962000"
        }]}
        test_db = connect_to_db()
        func_result = make_temporary_tables(test_db,test_input)
        table_names = func_result[0]
        set_up_dims_table(test_db,table_names)
        result = put_information_into_dims_schema(test_db)
        print(result)
        assert result == 0
        close_db_connection(test_db)



# Table headers
dim_tables_column_headers = {"dim_date":
  ["date_id",
  "year",
  "month",
  "day",
  "day_of_week",
  "day_name",
  "month_name",
  "quarter"],

  "dim_staff":
  ["staff_id",
  "first_name",
  "last_name" ,
  "department_name",
  "location",
  "email_address"],

  "dim_location":
  ["location_id",
  "address_line_1",
  "address_line_2",
  "district",
  "city",
  "postal_code",
  "country",
  "phone"],

  "dim_currency":
  ["currency_id",
  "currency_code",
  "currency_name"],


  "dim_design":
  ["design_id",
  "design_name",
  "file_location",
  "file_name"],

  "dim_counterparty":
  ["counterparty_id",
  "counterparty_legal_name",
  "counterparty_legal_address_line_1",
  "counterparty_legal_address_line_2",
  "counterparty_legal_district",
  "counterparty_legal_city",
  "counterparty_legal_postal_code",
  "counterparty_legal_country",
  "counterparty_legal_phone_number"]
}