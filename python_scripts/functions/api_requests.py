import pandas as pd
import requests
from datetime import datetime
from arrow_pd_parser import writer
from mojap_metadata import Metadata

from python_scripts.functions.api_helpers import (
    get_payload,
    make_booking_params,
    matrix_authenticate,
    extract_locations,
    fix_faulty_time_col,
    camel_to_snake_case,
)
from python_scripts.constants import land_location

from python_scripts.column_renames import bookings_renames, location_renames

from logging import getLogger

logger = getLogger(__name__)


def scrape_days_from_api(
    start_date: str, end_date: str
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Scrapes the matrix API for a given period
    Writes outputs to raw-history bucket with folder specified by 'env'

    Parameters:
        start_date: Start date in format %Y-%m-%d
        end_date: End date in format %Y-%m-%d
            can also be 'eod' to denote end of day
    """

    url = "https://app.matrixbooking.com/api/v1/booking"
    page_size = 2500

    bookings = []

    # Authenticate session with API
    ses = requests.session()
    matrix_authenticate(ses)

    # Derive booking parameters
    params = make_booking_params(
        start_date,
        end_date,
        pageNum=0,
        pageSize=page_size,
    )

    # Scrape the first page of data
    logger.info("Scraping page 0")
    data = get_payload(ses, url, params)
    rowcount = len(data)
    logger.info(f"Records scraped: {rowcount}")

    # Pull out the bookings and location data seperately
    bookings = data

    i = 1
    total_rows = rowcount
    while rowcount == page_size:
        logger.info(f"Scraping page {i}")
        params = make_booking_params(
            start_date,
            end_date,
            pageNum=i,
            pageSize=page_size,
        )
        data = get_payload(ses, url, params)
        rowcount = len(data)
        logger.info(f"Records scraped: {rowcount}")
        if rowcount > 0:
            bookings.extend(data)
        i += 1
        total_rows += rowcount

    logger.info(f"Retrieved {total_rows} bookings")

    raw_bookings = pd.json_normalize(bookings, sep="_").rename(
        mapper=camel_to_snake_case, axis="columns"
    )

    return raw_bookings


def scrape_locations_from_api(start_date: str) -> pd.DataFrame:
    ses = requests.session()
    matrix_authenticate(ses)
    params = {"f": start_date, "t": "eod"}
    url = "https://app.matrixbooking.com/api/v1/org/43/locations"
    logger.info("Scraping locations info")
    raw_locations = get_payload(ses, url, params)
    unnest_locs = extract_locations(raw_locations)
    raw_unpacked_locations = (
        pd.json_normalize(unnest_locs, sep="_")
        .drop(columns=["locations", "organisation_id"])
        .rename(mapper=camel_to_snake_case, axis="columns")
    )
    return raw_unpacked_locations


def rename_df(df: pd.DataFrame, renames: dict) -> pd.DataFrame:
    """_summary_

    Parameters
    ----------
    df :
        _description_
    renames : _type_
        _description_

    Returns
    -------
    _type_
        _description_

    Raises
    ------
    ValueError
        _description_
    """
    # Find any names that are not in the renames dict
    renames_data = [name for name in renames if name not in df.columns]
    if len(renames_data) > 0:
        logger.info(f"{renames_data} not in scraped dataframe")
    else:
        df = df.rename(columns=renames)
    return df


def fix_faulty_time_cols(df):
    """_summary_

    Returns
    -------
    _type_
        _description_
    """
    bookings_metadata = Metadata.from_json("metadata/db_v2/preprod/bookings.json")
    for col in bookings_metadata:
        if "timestamp" in col["type"]:
            if col["name"] in df.columns:
                df[col["name"]] = fix_faulty_time_col(df, col["name"])
    return df


def add_date_time_columns(df, scrape_date):
    df = df.copy()
    df["scrape_date"] = scrape_date
    df["ingestion_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    return df


def write_raw_data_to_s3(
    df: pd.DataFrame, renames: dict, raw_loc: str, env: str, name: str
):
    """_summary_

    Parameters
    ----------
    bookings : _type_
        _description_
    locations : _type_
        _description_
    start_date : _type_
        _description_
    """
    df = rename_df(df, renames)
    df = fix_faulty_time_cols(df)
    writer.write(
        df,
        raw_loc,
    )
    logger.info(f"{env}: raw {name} data written to {raw_loc}.")


def scrape_and_write_raw_bookings_data(start_date, env):
    raw_bookings_loc = f"{land_location}/bookings/{start_date}/raw-{start_date}.jsonl"
    bookings = scrape_days_from_api(start_date, "eod")
    bookings = add_date_time_columns(bookings, start_date)
    write_raw_data_to_s3(bookings, bookings_renames, raw_bookings_loc, env, "bookings")


def scrape_and_write_raw_locations_data(start_date, env):
    raw_locations_loc = f"{land_location}/locations/{start_date}/raw-{start_date}.jsonl"
    locations = scrape_locations_from_api(start_date)
    locations = add_date_time_columns(locations, start_date)
    write_raw_data_to_s3(
        locations, location_renames, raw_locations_loc, env, "locations"
    )
