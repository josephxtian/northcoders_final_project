from src.create_temporary_tables import make_temporary_tables, check_formatting_of_input
import pytest
from src.connection import connect_to_db, close_db_connection
import pg8000.native

class TestCheckFormattingOfInput:
    def test_for_empty_dictionary(self):
        with pytest.raises(TypeError):
            test_input = []
            check_formatting_of_input(test_input)


    def test_for_empty_dictionary_with_list(self):
        with pytest.raises(TypeError):
            test_input = {"my_list":["a","b"]}
            check_formatting_of_input(test_input)
    

    def test_with_correct_format(self):
        test_input = {
        "department": [
        {
            "department_id": 1,
            "department_name": "Sales",
            "location": "Manchester",
            "manager": "Richard Roma",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        },{
            "department_id": 3,
            "department_name": "Production",
            "location": "Leeds",
            "manager": "Chester Ming",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }],"staff": [
        {
            "staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_id": 2,
            "email_address": "jeremie.franey@terrifictotes.com",
            "created_at": "2022-11-03T14:20:51.563000",
            "last_updated": "2022-11-03T14:20:51.563000"
        },
        {
            "staff_id": 2,
            "first_name": "Deron",
            "last_name": "Beier",
            "department_id": 6,
            "email_address": "deron.beier@terrifictotes.com",
            "created_at": "2022-11-03T14:20:51.563000",
            "last_updated": "2022-11-03T14:20:51.563000"
        }]}
        result = check_formatting_of_input(test_input)
        assert result == True


class TestMakeTemporaryTables:
    def test_with_single_input_single_item(self):
        test_input = {"staff":[
        {"staff_id": 1,
         "first_name": "Jeremie",
         "last_name": "Franey",
         "department_id": 2,
         "email_address": "jeremie.franey@terrifictotes.com",
         "created_at": "2022-11-03T14:20:51.563000",
         "last_updated": "2022-11-03T14:20:51.563000"}]}
        db = connect_to_db()
        result = make_temporary_tables(db,test_input)
        assert result[1][0] == ["staff_id","first_name","last_name", "department_id","email_address","created_at","last_updated"]
        close_db_connection(db)


    def test_table_is_temporary(self):
        with pytest.raises(pg8000.exceptions.DatabaseError):
            test_input = {"staff":[
            {"staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_id": 2,
            "email_address": "jeremie.franey@terrifictotes.com",
            "created_at": "2022-11-03T14:20:51.563000",
            "last_updated": "2022-11-03T14:20:51.563000"}]}
            db = connect_to_db()
            make_temporary_tables(db,test_input)
            close_db_connection(db)
            db_2 = connect_to_db()
            db_2.run("SELECT * FROM staff;")
            close_db_connection(db)


    def test_with_single_input_multiple_item(self):
        test_input = {"staff":[
        {"staff_id": 1,
         "first_name": "Jeremie",
         "last_name": "Franey",
         "department_id": 2,
         "email_address": "jeremie.franey@terrifictotes.com",
         "created_at": "2022-11-03T14:20:51.563000",
         "last_updated": "2022-11-03T14:20:51.563000"},
         {
            "staff_id": 2,
            "first_name": "Deron",
            "last_name": "Beier",
            "department_id": 6,
            "email_address": "deron.beier@terrifictotes.com",
            "created_at": "2022-11-03T14:20:51.563000",
            "last_updated": "2022-11-03T14:20:51.563000"
        }]}
        db = connect_to_db()
        make_temporary_tables(db,test_input)
        result = db.run("SELECT * FROM staff;")
        assert result[0][0] == 1
        assert result[0][6] == '2022-11-03T14:20:51.563000'
        assert result[1][0] == 2
        assert result[1][6] == '2022-11-03T14:20:51.563000'
        close_db_connection(db)


    def test_with_multiple_input_single_item(self):
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
        db = connect_to_db()
        make_temporary_tables(db,test_input)
        result_staff = db.run("SELECT * FROM staff;")
        result_dep = db.run("SELECT * FROM department;")
        assert result_staff[0][0] == 1
        assert result_staff[0][6] == '2022-11-03T14:20:51.563000'
        assert result_dep[0][1] == 'Sales'
        assert result_dep[0][5] == '2022-11-03T14:20:49.962000'
        close_db_connection(db)

    def test_created_table_names_returned(self):
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
        db = connect_to_db()
        result = make_temporary_tables(db,test_input)
        assert result[0] == ["staff","department"]
        close_db_connection(db)