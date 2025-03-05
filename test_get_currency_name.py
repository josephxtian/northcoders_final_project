from get_currency_name import get_currency_details

def test_valid_id_1():
    currency_id = 1
    response = get_currency_details(currency_id)
    assert response == ("GBP", "British pound sterling")

def test_valid_id_2():
    currency_id = 2
    response = get_currency_details(currency_id)
    assert response == ("USD", "United States dollar")

def test_valid_id_3():
    currency_id = 3
    response = get_currency_details(currency_id)
    assert response == ("EUR", "The euro")

def test_invalid_id():
    currency_id = 4
    response = get_currency_details(currency_id)
    assert response == "Currency ID 4 is not in our system"


def test_invalid_id_type():
    currency_id = "square"
    response = get_currency_details(currency_id)
    assert response == "Error: Currency ID must be an integer"