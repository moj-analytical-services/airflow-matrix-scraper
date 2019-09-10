import argparse
from dateutil.parser import parse
from api_requests import scrape_days_from_api
from refresh_app_db import refresh_app_db

argp = argparse.ArgumentParser(description="Optional app description")

argp.add_argument(
    "--scrape_date",
    type="str",
    description="Date to scrape, as string in format %Y-%m-%d",
)

scrape_date = parse(argp.scrape_date)

scrape_days_from_api(scrape_date, "eod")

refresh_app_db()
