from src.create_dim_tables import set_up_dims_table
import pg8000.native
from src.connection import connect_to_db, close_db_connection

# Table headers defined at bottom of document

class TestSetUpDimsTable:
    # test 6 tables have been created
    def test_six_tables_created(self):
        test_db = connect_to_db()
        set_up_dims_table(test_db)
        for table in dim_tables_column_headers:
            test_db.run(f"SELECT * FROM {table[0]};")
            column_headers = [c['name'] for c in test_db.columns]
            assert column_headers == table[1:]
        close_db_connection(test_db)

    def test_tables_are_empty(self):
        test_db = connect_to_db()
        set_up_dims_table(test_db)
        for table in dim_tables_column_headers:
            result = test_db.run(f"SELECT * FROM {table[0]};")
            assert result == []
        close_db_connection(test_db)


class TestPutInformationIntoDimsSchema():
    # test there are no duplicates
    def test_no_duplicates_in_data(self):
        test_db = connect_to_db()
        set_up_dims_table(test_db)
        for table in dim_tables_column_headers:
            test_db.run(f"SELECT * FROM {table[0]};")
            column_headers = [c['name'] for c in test_db.columns]
            assert column_headers == table[1:]
        close_db_connection(test_db)

    # test ID numbers are stepping
    def test_id_numbers_stepping(self):
        pass


# Table headers
dim_tables_column_headers = [
  ["dim_date",
  "date_id",
  "year",
  "month",
  "day",
  "day_of_week",
  "day_name",
  "month_name",
  "quarter"],

  ["dim_staff",
  "staff_id",
  "first_name",
  "last_name" ,
  "department_name",
  "location",
  "email_address"],

  ["dim_location",
  "location_id",
  "address_line_1",
  "address_line_2",
  "district",
  "city",
  "postal_code",
  "country",
  "phone"],

  ["dim_currency",
  "currency_id",
  "currency_code",
  "currency_name"],


  ["dim_design",
  "design_id",
  "design_name",
  "file_location",
  "file_name"],

  ["dim_counterparty",
  "counterparty_id",
  "counterparty_legal_name",
  "counterparty_legal_address_line_1",
  "counterparty_legal_address_line_2",
  "counterparty_legal_district",
  "counterparty_legal_city",
  "counterparty_legal_postal_code",
  "counterparty_legal_country",
  "counterparty_legal_phone_number"]
]