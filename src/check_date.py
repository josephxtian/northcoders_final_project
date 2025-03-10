from datetime import datetime

def chec_date():

    date_updated = datetime.strptime(additional_data_from_op_db[i - 1]["last_updated"], "%Y-%m-%dT%H:%M:%S.%f")
    last_updated = "2025-11-03T14:20:51.563000"
    date_updated = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%f")
    year=last_updated.split("-")[0]
    month=last_updated.split('-')[1]
    day=last_updated.split('-')[2].split('T')[0]
    time=last_updated.split('-')[2].split('T')[1]

    # year = date_updated.year
    # month = date_updated.month
    # day = date_updated.day
    # time = date_updated.time()
    object_key = f"{year}/{month}/{day}/{time}.json"
    print(object_key)

chec_date()