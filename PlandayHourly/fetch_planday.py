import logging
import httpx
from datetime import datetime, timedelta
import pandas as pd

from utils import (
    timedelta_to_str,
    str_to_timedelta,
    ensure_timedelta,
    ensure_int,
    parse_datetime,
)
from PlandayHourly.constants import restaurantnames, list_to_ignore

async def fetch_planday(client_id, refresh_token):
    today_date = datetime.today()
    start_date = (today_date - timedelta(weeks=2)).strftime("%Y-%m-%d")
    end_date = (today_date + timedelta(weeks=2)).strftime("%Y-%m-%d")
    # start_date = "2023-12-01"
    # end_date = "2024-04-15"
    token_endpoint = "https://id.planday.com"
    api_endpoint = "https://openapi.planday.com"
    timeout_duration = 30.0

    access_token_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    access_token_data = {
        "client_id": client_id,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    access_token_endpoint = f"{token_endpoint}/connect/token"

    async with httpx.AsyncClient(timeout=timeout_duration) as client:
        access_token_response = await client.post(
            access_token_endpoint,
            headers=access_token_headers,
            data=access_token_data,
        )

        if access_token_response.status_code != 200:
            logging.info(
                f"Error: {access_token_response.status_code} - {access_token_response.text}"
            )
            return

        access_token_json = access_token_response.json()
        access_token = access_token_json["access_token"]

        dept_headers = {
            "X-ClientId": client_id,
            "Authorization": "Bearer " + access_token,
        }
        department_endpoint = f"{api_endpoint}/hr/v1/departments"
        dept_response = await client.get(department_endpoint, headers=dept_headers)

        if dept_response.status_code != 200:
            logging.info(f"Error: {dept_response.status_code} - {dept_response.text}")
            return

        departments = dept_response.json()["data"]
        actual_data = {}

        for department_data in departments:
            dep_name = department_data["name"]
            dep_id = department_data["id"]

            if dep_id in list_to_ignore:
                continue

            payroll_headers = {
                "X-ClientId": client_id,
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            payroll_url = f"{api_endpoint}/payroll/v1/payroll/?departmentIds={dep_id}&from={start_date}&to={end_date}&returnFullSalaryForMonthlyPaid=true"
            payroll_response = await client.get(payroll_url, headers=payroll_headers)

            if payroll_response.status_code != 200:
                logging.info(
                    f"Error: {payroll_response.status_code} - {payroll_response.text}"
                )
                continue

            payroll_json_response = payroll_response.json()
            payroll_data = payroll_json_response["shiftsPayroll"]

            for data in payroll_data:
                start = parse_datetime(data["start"])
                end = parse_datetime(data["end"])
                rate = data["wage"]["rate"]
                employee_id = data["employeeId"]
                if rate == 0:
                    pay_headers = {
                        "X-ClientId": client_id,
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    }
                    pay_url = f"{api_endpoint}/pay/v1/salaries/employees/{employee_id}"
                    pay_response = await client.get(pay_url, headers=pay_headers)

                    if pay_response.status_code != 200:
                        logging.info(
                            f"Error: {pay_response.status_code} - {pay_response.text}"
                        )
                        rate = 230  # Default rate if fetch fails
                        continue
                    pay_json_response = pay_response.json()
                    pay_data = pay_json_response["data"]
                    salary = pay_data["salary"]
                    hours = pay_data["hours"]
                    rate = (float(salary) / float(hours)) if hours != 0 else 0

                hourly_intervals = pd.date_range(start=start, end=end, freq="H")

                for i, hour in enumerate(hourly_intervals):
                    if i < len(hourly_intervals) - 1:
                        hour_end = hourly_intervals[i + 1]
                    else:
                        hour_end = end

                    if i == 0:  # First interval
                        duration = (hour.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1) - hour).total_seconds() / 3600
                    elif i == len(hourly_intervals) - 1:  # Last interval
                        duration = (hour_end - hour).total_seconds() / 3600
                    else:
                        duration = 1  # Full hour

                    cost = rate * duration
                    date_key = hour.strftime("%Y-%m-%d")
                    hour_key = hour.strftime("%H:00:00")

                    if dep_name not in actual_data:
                        actual_data[dep_name] = {}
                    if date_key not in actual_data[dep_name]:
                        actual_data[dep_name][date_key] = {}
                    if hour_key not in actual_data[dep_name][date_key]:
                        actual_data[dep_name][date_key][hour_key] = {
                            "employees": set(),  # Initialize set to store unique employee IDs
                            "duration": timedelta(hours=duration),
                            "cost": cost,
                        }
                    actual_data[dep_name][date_key][hour_key]["employees"].add(employee_id)
                    actual_data[dep_name][date_key][hour_key]["duration"] += timedelta(hours=duration)
                    actual_data[dep_name][date_key][hour_key]["cost"] += cost

        flattened_data = []
        for dep_name, dep_data in actual_data.items():
            sheet_name = restaurantnames.get(dep_name, dep_name)  # Fallback to dep_name if not in restaurant_names

            if not sheet_name or len(sheet_name) > 31 or any(char in sheet_name for char in "/*\\[]:?"):
                logging.info(
                    f"Invalid sheet name for department {dep_name}: {sheet_name}. Skipping."
                )
                continue

            for date, hours in dep_data.items():
                for hour, values in hours.items():
                    duration_hours = values["duration"].total_seconds() / 3600
                    employee_count = len(values["employees"])  # Count unique employee IDs
                    row = {
                        "Date": date,
                        "Hour": hour,
                        "Duration": duration_hours,
                        "Cost": values["cost"],
                        "EmployeeCount": employee_count,  # Include employee count
                        "Restaurant": sheet_name,
                    }
                    flattened_data.append(row)
        df = pd.DataFrame(flattened_data)
        return df
