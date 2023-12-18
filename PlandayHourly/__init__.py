import asyncio
import logging
import datetime
import azure.functions as func
import pandas as pd
# Assuming `handle` is part of a class named `PlandayDataFetcher`
from PlandayHourly.fetch_planday import fetch_planday
# from import_xlsx_planday_data import handle_planday
from PlandayHourly.import_to_planday import handle_planday
from PlandayHourly.transform_planday import transform_planday
async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # Instantiate the class containing the `handle` method
    data_fetcher = await fetch_planday()
    if data_fetcher.empty or isinstance(data_fetcher, str):
        logging.info("No employee data found. Please try again later.") 
    else:
        planday_transformed = transform_planday(data_fetcher)
        handle_planday(planday_transformed)
    # import_to_db(transformed_df, restaurant)
