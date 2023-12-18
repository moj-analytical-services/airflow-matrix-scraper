from dateutil.parser import parse
from functions.api_requests import scrape_days_from_api

from functions.general_helpers import (
    get_command_line_arguments
)

from constants import (
    db_version
)

#from refresh_app_db import refresh_app_db




if __name__=="__main__":


    # Get command line arguments
    args = get_command_line_arguments()

    # Get date from string
    scrape_date = parse(args.scrape_date).strftime("%Y-%m-%d")
    
    # Get bookings and locations 
    # Optionally writes to s3 
    bookings, locations = scrape_days_from_api(scrape_date, "eod", db_version, args.env, args.skip_write_s3)

    #refresh_app_db()
