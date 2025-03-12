from src.dimensions_dependents_set_up import create_pandas_raw_tables,create_pandas_empty_dim_tables
from src.connection import connect_to_db, close_db_connection
import pandas as pd
import json
import pytest
from pg8000.native import identifier
#test json data

@pytest.fixture
def tables():
    return ["address", "counterparty", "currency","department", "design","payment", "payment_type", "purchase_order","sales_order", "staff", "transaction"]
@pytest.fixture
def load_local_files_for_testing():
    output_dict = {}
    file_list = ["address", "counterparty", "currency","department", "design","payment", "payment_type", "purchase_order","sales_order", "staff", "transaction"]
    for file in file_list:
        with open(f'json_data/json-{file}.json','r') as f:
            import_dict = json.load(f)[file]
        output_dict[file] = import_dict
    return output_dict

def test_create_pandas_raw_tables_returns_all_tables_only(load_local_files_for_testing,tables):
    result = create_pandas_raw_tables(load_local_files_for_testing)
    count = 0
    for key in result:
        count+=1
        assert key in tables
    assert count == 11

def test_create_pandas_raw_tables_correct_headings(load_local_files_for_testing,tables):
    try:    
        db = connect_to_db()
        result = create_pandas_raw_tables(load_local_files_for_testing)
        for key in result:
            db_run_column = db.run(
                f""" SELECT * FROM {identifier(key)};"""
            )
            db_columns = [col["name"] for col in db.columns]
            print(db_columns)
            assert result[key].columns.tolist() == db_columns[1:]
    finally:
        close_db_connection(db)

def test_create_pandas_empty_dim_tables_returns_dict_of_data_frames():
    result = create_pandas_empty_dim_tables()
    assert isinstance(result,dict)
    for key in result:
        assert isinstance(result[key],pd.DataFrame)

def test_create_pandas_empty_dim_tables_returns_correct_keys():
    result = create_pandas_empty_dim_tables()
    list_of_dim_tables=[]
    for key in result:
        list_of_dim_tables.append(key)
    assert sorted(list_of_dim_tables) == sorted(["dim_date","dim_staff",
                                          "dim_location","dim_currency",
                                          "dim_design", "dim_counterparty"
                                          ])