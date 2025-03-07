CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    day_name VARCHAR,
    month_name VARCHAR,
    quarter INT
);

CREATE TABLE dim_currency (
    currency_id INT PRIMARY KEY,
    currency_code VARCHAR,
    currency_name VARCHAR
);
