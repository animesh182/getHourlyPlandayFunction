import asyncio
import logging
import datetime
import azure.functions as func
import pandas as pd

# Assuming `handle` is part of a class named `PlandayDataFetcher`
from PlandayHourly.fetch_planday import fetch_planday

# from import_xlsx_planday_data import handle_planday
from PlandayHourly.import_from_planday import handle_planday
from PlandayHourly.transform_planday import transform_planday
from PlandayHourly.constants import company_data


async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    logging.info("Python timer trigger function ran at %s", utc_timestamp)
    all_data = []
    for company, details in company_data.items():
        client_id = details["client_id"]
        refresh_token = details["refresh_token"]
        # Pass the client_id and app_id to fetch_planday function
        data_fetcher = await fetch_planday(client_id, refresh_token)

        if data_fetcher.empty or isinstance(data_fetcher, str):
            logging.info("No approved shifts found. Please try again later.")
        else:
            planday_transformed = transform_planday(data_fetcher, company)
            all_data.append(planday_transformed)
    if all_data:
        all_data_df = pd.concat(all_data, ignore_index=True)
        handle_planday(all_data_df)
    else:
        logging.info("No data available to handle.")
    # import_to_db(transformed_df, restaurant)
