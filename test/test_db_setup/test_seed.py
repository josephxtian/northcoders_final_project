from db_setup.seed import seed_db

# check correct number of tables made
def test_created_table_names_returned():
    result = seed_db()

    assert result["table_count"] == 11

# check correct number of rows inserted for each table
def test_number_of_rows():
    table_structure = {"address":5,
                   "counterparty":10,
                   "currency":3,
                   "department":8,
                   "design":50,
                   "payment_type":50,
                   "payment":50,
                   "purchase_order":50,
                   "sales_order":50,
                   "staff":50,
                   "transaction":50}
    result = seed_db()
    for table in table_structure:
        assert result[table] == table_structure[table]