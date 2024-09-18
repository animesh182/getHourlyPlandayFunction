import pandas as pd
import os
import logging
import psycopg2
import psycopg2.extras

# prod
params = {
    "dbname": "salesdb1",
    "user": "salespredictionsql",
    "password": "Shajir86@ms9",
    "host": "sales-prediction-svr-v2.postgres.database.azure.com",
    "port": "5432",
}
# staging
# params = {
#     'dbname': 'salesdb1',
#     'user': 'salespredstaging',
#     'password': 'Shajir86@ms9',
#     'host': 'krunch-staging-svr.postgres.database.azure.com',
#     'port': '5432'
# }
# local
# params = {
#     'dbname': 'salesdb',
#     'user': 'postgres',
#     'password': 'admin123',
#     'host': 'localhost',
#     'port': '5432'
# }

def handle_planday(df):
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["id"] = df["id"].apply(lambda x: str(x))

    insert_statement = """
    INSERT INTO public."Predictions_hourlyemployeecostandhoursinfo"(
    date,hour,employee_hours, employee_cost, employee_count, restaurant, company,department,id  )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s ,%s)
    ON CONFLICT (date,hour,restaurant,company,department)
    DO UPDATE SET
        employee_cost = EXCLUDED.employee_cost,
        employee_hours = EXCLUDED.employee_hours,
        employee_count = EXCLUDED.employee_count
    """
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                for index, row in df.iterrows():
                    date = row["date"]
                    hour = row["hour"]
                    employee_hours = float(row["employee_hours"])
                    employee_cost = float(row["employee_cost"])
                    employee_count = int(row["employee_count"])
                    restaurant= row["restaurant"]
                    company = row["company"]
                    id = row["id"]
                    department = None
                    cur.execute(insert_statement,(date, hour, employee_hours, employee_cost, employee_count, restaurant, company, department, id) )

                # # Transform your DataFrame to a list of tuples, including the id column
                # tuples = [tuple(x) for x in df.to_records(index=False)]
                # # Use execute_values to insert the data
                # psycopg2.extras.execute_values(
                #     cur, insert_statement, tuples, template=None, page_size=100
                # )
                conn.commit()
        logging.info("Data successfully imported into the table")
    except Exception as e:
        logging.info("Error while importing data")
        logging.info(f"Error: {e}")
