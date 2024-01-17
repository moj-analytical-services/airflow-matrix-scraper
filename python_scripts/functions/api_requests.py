import pandas as pd
import requests
import re
import json
from arrow_pd_parser import writer
import python_scripts.s3_utils as s3_utils
from datetime import datetime, timedelta
from mojap_metadata import Metadata

from python_scripts.constants import (
    raw_history_location,
)
from python_scripts.column_renames import (
    location_renames,
    bookings_renames,
)

from logging import getLogger

logger = getLogger(__name__)


def read_json(file_path: str) -> dict:
    """Reads a json file in as a dictionary

    Parameters
    ----------
    file_path :
        file path of the JSON to read from

    Returns
    -------
        dictionary representing the json file
    """
    f = open(file_path)
    return json.loads(f.read())


def get_secrets() -> dict:
    return s3_utils.read_json_from_s3("alpha-dag-matrix/api_secrets/secrets.json")


def matrix_authenticate(session: requests.Session) -> requests.Session:
    # Add this before the error occurs
    for handler in logger.handlers:
        for record in handler.records:
            print(record.__dict__)
    secrets = get_secrets()
    username = secrets["username"]
    password = secrets["password"]

    url = "https://app.matrixbooking.com/api/v1/user/login"
    session.post(url, json={"username": username, "password": password})
    return session


def get_booking_categories(session: requests.Session) -> pd.DataFrame:
    """Returns pandas dataframe containing all booking categories
    that are available to organisation

     Parameters:
    session (requests.sessions.Session): Authenticated session to
        matrix booking API
    """

    # Booking categories API url
    url_booking_cats = "https://app.matrixbooking.com/api/v1/category"

    # Make request and create dataframe
    res = requests.get(url_booking_cats, cookies=session.cookies).json()
    df_booking_categories = pd.json_normalize(res)

    return df_booking_categories


def make_booking_params(
    time_from: str,
    time_to: str,
    booking_categories: str,
    status: str = None,
    pageSize: int = None,
    pageNum: int = 0,
) -> dict:
    params = {
        "f": time_from,
        "t": time_to,
        "bc": booking_categories,
        "status": status,
        "include": ["audit", "locations"],
        "pageSize": pageSize,
        "pageNum": pageNum,
    }
    return params


def get_payload(session, url, parameters):
    resp = session.get(url=url, cookies=session.cookies, params=parameters)
    logger.debug(f"GET {resp.url}")
    logger.debug(f"response status code: {resp.status_code}")
    return resp.json()


def split_s3_path(s3_path: str) -> tuple[str]:
    """Splits an s3 file path into a bucket and key

    Parameters
    ----------
    s3_path :
        The full (incl s3://) path of a file.

    Returns
    -------
        Tuple of the bucket name and key (file path) within that bucket.
    """
    if s3_path[:2] != "s3":
        raise ValueError("S3 file path should start with 's3://'.")
    path_split = s3_path.split("/")
    bucket = path_split[2]
    key = "/".join(path_split[3:])
    return bucket, key


def get_scrape_dates(start_date, end_date):
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days + 1)):
            yield datetime.strftime(start_date + timedelta(n), "%Y-%m-%d")

    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_1 = datetime.now().date() - timedelta(days=1)
    end_date_2 = datetime.strptime(end_date, "%Y-%m-%d").date()

    if end_date_1 < end_date_2:
        end_date = end_date_1
    else:
        end_date = end_date_2

    return daterange(start_date, end_date)


def add_microseconds(df: pd.DataFrame, col: str):
    """_summary_

    Parameters
    ----------
    df :
        _description_
    col :
        _description_

    Returns
    -------
        _description_
    """
    original_nulls = df[col].isna()
    datetime_col = pd.to_datetime(
        df[col], format="%Y-%m-%dT%H:%M:%S.%f", errors="coerce"
    )
    datetime_null_locs = datetime_col.isna()
    wrong_casted_dts = datetime_null_locs & ~original_nulls
    df.loc[wrong_casted_dts, col] = df.loc[wrong_casted_dts, col] + ".000"
    return df


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
    status = ["CONFIRMED", "TENTATIVE", "CANCELLED"]

    bookings = []

    # Authenticate session with API
    ses = requests.session()
    matrix_authenticate(ses)

    # Get booking categories available
    df_booking_categories = get_booking_categories(ses)

    # List with unique booking categories
    booking_categories = list(df_booking_categories["locationKind"])

    # Derive booking parameters
    params = make_booking_params(
        start_date,
        end_date,
        booking_categories,
        pageNum=0,
        pageSize=page_size,
        status=status,
    )

    # Scrape the first page of data
    logger.info("Scraping page 0")
    data = get_payload(ses, url, params)
    rowcount = len(data["bookings"])
    logger.info(f"Records scraped: {rowcount}")

    # Pull out the bookings and location data seperately
    bookings = data["bookings"]
    locations = data["locations"]

    i = 1
    total_rows = rowcount
    while rowcount == page_size:
        logger.info(f"Scraping page {i}")
        params = make_booking_params(
            start_date, end_date, pageNum=i, pageSize=page_size, status=status
        )
        data = get_payload(ses, url, params)
        rowcount = len(data["bookings"])
        logger.info(f"Records scraped: {rowcount}")
        if rowcount > 0:
            bookings.extend(data["bookings"])
        i += 1
        total_rows += rowcount

    logger.info(f"Retrieved {len(locations)} locations")
    logger.info(f"Retrieved {total_rows} bookings")

    raw_bookings = pd.json_normalize(bookings, sep="_").rename(
        mapper=camel_to_snake_case, axis="columns"
    )
    raw_locations = pd.json_normalize(locations, sep="_").rename(
        mapper=camel_to_snake_case, axis="columns"
    )

    return raw_bookings, raw_locations


def camel_to_snake_case(input_str: str) -> str:
    # Using regular expressions to find positions with capital letters and insert underscores
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", input_str)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)

    # Handle the case where multiple uppercase letters are present
    snake_case_str = re.sub("([a-z])([A-Z]+)", r"\1_\2", s2).lower()

    return snake_case_str


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
        logger.error(f"{renames_data} not in scraped dataframe")
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
            df = add_microseconds(df, col["name"])
    return df


def write_raw_data_to_s3(
    bookings: pd.DataFrame, locations: pd.DataFrame, start_date: str
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
    raw_bookings_loc = f"{raw_history_location}/bookings/raw-{start_date}.jsonl"
    raw_locations_loc = f"{raw_history_location}/locations/raw-{start_date}.jsonl"
    bookings = rename_df(bookings, bookings_renames)
    bookings = fix_faulty_time_cols(bookings)
    locations = rename_df(locations, location_renames)
    writer.write(
        bookings,
        raw_bookings_loc,
    )
    writer.write(
        locations,
        raw_locations_loc,
    )
    logger.info(f"Raw booking and location data written to {raw_history_location}.")


def scrape_and_write_raw_data(start_date):
    bookings, locations = scrape_days_from_api(start_date, "eod")
    write_raw_data_to_s3(bookings, locations, start_date)
