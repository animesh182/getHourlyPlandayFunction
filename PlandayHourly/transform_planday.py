import os
import pandas as pd
import uuid
from datetime import datetime
from utils import convert_to_decimal_hours
import logging


def transform_planday(data, company):
    # Create a new DataFrame for transformed data
    transformed_data = pd.DataFrame()
    # Transform the date column
    transformed_data["date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")  # Adjust format as per your data
    transformed_data["hour"] = pd.to_datetime(data["Hour"], format="%H:%M:%S").dt.time  # Adjust format as per your data
    # Transform the employee_hours column
    transformed_data["employee_hours"] = data["Duration"]
    transformed_data["employee_cost"] = data["Cost"]
    transformed_data["employee_count"] = data["EmployeeCount"]# Include employee count
    # Add the company column from the original data
    transformed_data["restaurant"] = data["Restaurant"]
    transformed_data["company"] = company
    transformed_data["department"] = None
    transformed_data["id"] = [uuid.uuid4() for _ in range(len(transformed_data))]
    logging.info("Data transformed to Krunch format")
    return transformed_data
