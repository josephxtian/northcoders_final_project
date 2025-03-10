from src.dim_date_function import extract_date_info_from_dim_date
from datetime import datetime
import unittest

class TestDateExtrator(unittest.TestCase):

    def test_dim_date_data_format(self):
        # Arrange/ Act

        date_str = "2022-11-03T14:20:52.187000"
        result = extract_date_info_from_dim_date(date_str)
        expected = {
            "date_id": "2022-11-03",
            "year": 2022,
            "month": 11,
            "day": 3,
            "day_of_week": 3,  
            "day_of_week_name": "Thursday",
            "month_name": "November",
            "quarter": 4
        }
        # assert
        assert result == expected

    def test_short_date_format(self):
        #Arrange/Act
        date_str = "2022-11-09"
        result = extract_date_info_from_dim_date(date_str)
        expected = {
            "date_id": "2022-11-09",
            "year": 2022,
            "month": 11,
            "day": 9,
            "day_of_week": 2,  
            "day_of_week_name": "Wednesday",
            "month_name": "November",
            "quarter": 4
        }
        #assert
        assert result == expected

    def test_short_date_format(self):
        #Arrange/Act
        date_str = "2022-11-09"
        result = extract_date_info_from_dim_date(date_str)
        expected = {
            "date_id": "2022-11-09",
            "year": 2022,
            "month": 11,
            "day": 9,
            "day_of_week": 2,  
            "day_of_week_name": "Wednesday",
            "month_name": "November",
            "quarter": 4
        }
        #assert
        assert result == expected

    def test_leap_year(self):
        #Arrange/Act
        date_str = "2024-02-29"
        result = extract_date_info_from_dim_date(date_str)
        expected = {
            "date_id": "2024-02-29",
            "year": 2024,
            "month": 2,
            "day": 29,
            "day_of_week": 3,  
            "day_of_week_name": "Thursday",
            "month_name": "February",
            "quarter": 1
        }
        #assert
        assert result == expected

    def test_invalid_date_year(self):
        #Arrange/Act
        date_str = "2025-02-29"
        #assert
        with self.assertRaises(ValueError):
            extract_date_info_from_dim_date(date_str)

    def test_invalid_date_format(self):
        #Arrange/Act
        date_str = "2025-22-59"
        #assert
        with self.assertRaises(ValueError):
            extract_date_info_from_dim_date(date_str)

