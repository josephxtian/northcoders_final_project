import json

with open('json_data/json-currency.json', 'r') as file:
    try:
        data = json.load(file)
    except json.JSONDecodeError:
        print("Error: Failed to load JSON file.")
        data = {"currency": []} 
          
def get_currency_details(currency_id):
    if not isinstance(currency_id, int):
        return "Error: Currency ID must be an integer"
    
    for currency in data['currency']:
        if currency_id == currency["currency_id"]:
            currency_code = currency["currency_code"]
            break
    
    else:
        print(f"currency id {currency_id} is not in our system")
        return f"Currency ID {currency_id} is not in our system"
       
        
    currency_dict = {
        "GBP": "British pound sterling",
        "USD": "United States dollar",
        "EUR": "The euro"
    }

    currency_name = currency_dict.get(currency_code, "Unknown currency code")
    return {
        "currency_id": currency_id,
        "currency_code": currency_code,
        "currency_name": currency_name
    }


