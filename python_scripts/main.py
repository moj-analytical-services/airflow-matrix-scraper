from dateutil.parser import parse
from functions.api_requests import (
    scrape_days_from_api,
    write_raw_data_to_s3,
)
from functions.data_validation import validate_data, read_and_write_cleaned_data
from functions.general_helpers import get_command_line_arguments

from constants import db_version

# from refresh_app_db import refresh_app_db


def retrieve_and_transform_data(
    db_version: str,
    env: str,
    start_date: str,
    end_date: str,
    skip_write_s3: bool = False,
) -> tuple[dict]:
    """Scrapes data from the api, sends raw data to history bucket,
    transforms into correct format and writes the data to s3.

    Parameters
    ----------
    db_version :
        Denotes which database version to use (1 or 2)
    env :
        Denotes whether to save results in
        production (prod) or development (dev)
    start_date :
        Start date in format %Y-%m-%d
    end_date :
        End date in format %Y-%m-%d
            can also be 'eod' to denote end of day
    skip_write_s3 : optional
        Allow user to skip writing to s3,
        by default False

    Returns
    -------
        Transformed booking and location data.
    """
    bookings, locations, start_date = scrape_days_from_api(start_date, end_date)
    write_raw_data_to_s3(bookings, locations, start_date)
    bookings_filepath, locations_filepath = validate_data(start_date)

    clean_data = {}

    for name, loc in zip(
        ["bookings", "locations"], [bookings_filepath, locations_filepath]
    ):
        clean_data[name] = read_and_write_cleaned_data(
            loc, start_date, name, db_version, env, skip_write_s3
        )
    return clean_data


if __name__ == "__main__":
    # Get command line arguments
    args = get_command_line_arguments()

    # Get date from string
    scrape_date = parse(args.scrape_date).strftime("%Y-%m-%d")

    # Get bookings and locations
    # Optionally writes to s3
    data_dict = retrieve_and_transform_data(
        db_version, args.env, scrape_date, "eod", args.skip_write_s3
    )

    # refresh_app_db()
