import argparse
from dateutil.parser import parse
from api_requests import scrape_days_from_api
from refresh_app_db import refresh_app_db

argp = argparse.ArgumentParser(description="Optional app description")

# Date to scrape
argp.add_argument(
    "--scrape_date",
    type=str,
    required=True,
    help="Date to scrape, as string in format %Y-%m-%d",
)

# Environment (development or production)
argp.add_argument(
    "--env", "-e", 
    type=str,
    choices=['dev', 'prod'],
    required=True,
    help="Environment (development or production) to store results in. Takes values dev or prod",
    
)

# Writing to s3
argp.add_argument(
    "--skip-write-s3", 
    action=argparse.BooleanOptionalAction,
    help="If passed, this will skip writing to s3 (default is to write to s3 when scaper runs)",
    
)


args = argp.parse_args()

scrape_date = parse(args.scrape_date).strftime("%Y-%m-%d")

if __name__=="__main__":
    
    # Current db_version
    db_version = "db_v2"
    
    # Get bookings and locations 
    # Optionally writes to s3 
    bookings, locations = scrape_days_from_api(scrape_date, "eod", db_version, args.env, args.skip_write_s3)

    #refresh_app_db()
