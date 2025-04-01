from db_setup.seed import seed_db



class TestDevEnv:
    # check correct number of tables made for
    def test_created_table_names_returned(self):
        env = 'dev'
        result = seed_db(env)

        assert result["table_count"] == 11

    # check correct number of rows inserted for each table
    def test_number_of_rows(self):
        env = 'dev'
        table_structure = {"address_row_count":10,
                    "counterparty_row_count":10,
                    "currency_row_count":3,
                    "department_row_count":8,
                    "design_row_count":50,
                    "payment_type_row_count":4,
                    "payment_row_count":37,
                    "purchase_order_row_count":20,
                    "sales_order_row_count":5,
                    "staff_row_count":10,
                    "transaction_row_count":20}
        result = seed_db(env)
        assert result == table_structure


class TestTestEnv:
    # check correct number of tables made for
    def test_created_table_names_returned(self):
        env = 'test'
        result = seed_db(env)

        assert result["table_count"] == 11

    # check correct number of rows inserted for each table
    def test_number_of_rows(self):
        env = 'test'
        table_structure = {"address_row_count":10,
                    "counterparty_row_count":10,
                    "currency_row_count":3,
                    "department_row_count":8,
                    "design_row_count":50,
                    "payment_type_row_count":4,
                    "payment_row_count":37,
                    "purchase_order_row_count":20,
                    "sales_order_row_count":5,
                    "staff_row_count":10,
                    "transaction_row_count":20}
        result = seed_db(env)
        assert result == table_structure

