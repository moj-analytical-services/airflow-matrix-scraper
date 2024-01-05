import pandas as pd
import requests
import json
import os
import re
from data_linter import validation
from dataengineeringutils3.s3 import get_filepaths_from_s3_folder
from mojap_metadata import Metadata
from arrow_pd_parser import writer, reader
import python_scripts.s3_utils as s3_utils
from datetime import datetime, timedelta
import yaml

from python_scripts.constants import (
    db_location,
    raw_history_location,
)

from logging import getLogger

logger = getLogger(__name__)


def read_json(file_path: str):
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


def get_secrets():
    return s3_utils.read_json_from_s3("alpha-dag-matrix/api_secrets/secrets.json")


def matrix_authenticate(session):
    secrets = get_secrets()
    username = secrets["username"]
    password = secrets["password"]

    url = "https://app.matrixbooking.com/api/v1/user/login"
    session.post(url, json={"username": username, "password": password})
    return session


def get_booking_categories(session):
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
    time_from, time_to, booking_categories, status=None, pageSize=None, pageNum=0
):
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


def unpack_row(data: dict, joiner: str = ".") -> dict:
    """Function to unpack a nested dictionary (nested with both lists and dictionaries).

    Parameters
    ----------
    data:
        Dict of data to unpack
    joiner:
        The string with which to join the nested dict keys

    Returns
    -------
    dict
        Dict of data unpacked, with nested rows joined by the defined joiner.
    """
    unpacked_data = {}

    def unpack(item, keys_name=""):
        if isinstance(item, dict):
            for key in item:
                unpack(item[key], f"{keys_name}{key}{joiner}")
        elif isinstance(item, list):
            if len(item) == 1:
                unpack(item[0], f"{keys_name}{joiner}")
            for i, it in enumerate(item):
                unpack(it, f"{keys_name}{i + 1}{joiner}")
        else:
            joiner_len = len(joiner)
            unpacked_data[keys_name[:-joiner_len]] = item

    unpack(data)
    return unpacked_data


def unpack_data(data: list[dict]) -> list[dict]:
    """Unpacks each row of data from a list of nested dictionaries.

    Parameters
    ----------
    data:
        List of dictionaries that are nested, and need unpacking

    Returns
    -------
        List of unpacked dictionaries
    """
    unpacked_data = [unpack_row(row) for row in data]
    return unpacked_data


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


def scrape_days_from_api(start_date: str, end_date: str) -> tuple[dict]:
    """
    Scrapes the matrix API for a given period
    Writes outputs to raw-history bucket with folder specified by 'env'

    Parameters:
        start_date (str): Start date in format %Y-%m-%d
        end_date (str): End date in format %Y-%m-%d
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

    bookings = data["bookings"]
    locations = data["locations"]

    unpacked_bookings = unpack_data(bookings)
    unpacked_locations = unpack_data(locations)

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
            unpacked_bookings.extend(unpack_data(data["bookings"]))
        i += 1
        total_rows += rowcount

    logger.info(f"Retrieved {len(locations)} locations")
    logger.info(f"Retrieved {total_rows} bookings")

    raw_bookings = pd.DataFrame(unpacked_bookings)
    raw_locations = pd.DataFrame(unpacked_locations)

    return raw_bookings, raw_locations, start_date


def rename_bookings_df(bookings, db_version, env):
    """_summary_

    Parameters
    ----------
    bookings : _type_
        _description_
    db_version : _type_
        _description_
    env : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    # Convert the nested data into tablular format
    renames = read_json(f"metadata/{db_version}/{env}/bookings_renames.json")

    if len(bookings) > 0:
        bookings = bookings.rename(columns=renames)
    else:
        #
        bookings = pd.DataFrame(columns=renames.values())
    return bookings


def rename_locations_df(locations_df, db_version, env):
    """_summary_

    Parameters
    ----------
    locations_df : _type_
        _description_
    db_version : _type_
        _description_
    env : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    renames = read_json(f"metadata/{db_version}/{env}/locations_renames.json")
    locations_df = locations_df.rename(columns=renames)
    return locations_df


def write_raw_data_to_s3(bookings, locations, start_date):
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
    writer.write(
        bookings,
        raw_bookings_loc,
    )
    writer.write(
        locations,
        raw_locations_loc,
    )
    logger.info(f"Raw booking and location data written to {raw_history_location}.")


def extract_timestamp(file_path):
    """_summary_

    Parameters
    ----------
    file_path : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    file_name = os.path.basename(file_path)
    match = re.search(r"raw-\d{4}-\d{2}-\d{2}-\d+-([0-9]+)\.parquet", file_name)
    if match:
        epoch_time = match.groups()
        epoch_timestamp = int(epoch_time)
        return epoch_timestamp
    else:
        return None


def get_latest_file(file_paths):
    """_summary_

    Parameters
    ----------
    file_paths : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    final_path = None
    for path in file_paths:
        result = extract_timestamp(path)
        if result:
            timestamp = result
            if final_path is None:
                final_path = path
                final_epoch = timestamp
            elif final_epoch < timestamp:
                final_path = path
                final_epoch = timestamp
            else:
                continue
    return final_path


def validate_data(start_date):
    """_summary_

    Parameters
    ----------
    start_date : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    config_path = "config.yaml"
    config = yaml.safe_load(config_path)
    validation.para_run_init(2, config_path)
    validation.para_run_validation(0, config)
    validation.para_run_validation(1, config)

    land_files = get_filepaths_from_s3_folder(config["land-base-path"])
    pass_files = get_filepaths_from_s3_folder(config["pass-base-path"])
    fail_files = get_filepaths_from_s3_folder(config["fail-base-path"])
    assert (not land_files and not fail_files) and pass_files
    booking_start_date_files = [
        file for file in pass_files if start_date and "bookings" in file
    ]
    locations_start_date_files = [
        file for file in pass_files if start_date and "locations" in file
    ]
    bookings_filepath = get_latest_file(booking_start_date_files)
    locations_filepath = get_latest_file(locations_start_date_files)
    return bookings_filepath, locations_filepath


def read_and_write_cleaned_data(
    raw_loc: str,
    start_date: str,
    name: str,
    db_version: str,
    env: str,
    skip_write_s3: bool = False,
):
    """_summary_

    Parameters
    ----------
    raw_loc : _type_
        _description_
    start_date : _type_
        _description_
    name : _type_
        _description_
    db_version : _type_
        _description_
    env : _type_
        _description_
    skip_write_s3 : _type_, optional
        _description_, by default False

    Returns
    -------
    _type_
        _description_
    """
    metadata = Metadata.from_json(f"metadata/{db_version}/{env}/bookings.json")
    df = reader.read(raw_loc, metadata)
    df = df.reindex(columns=metadata.column_names)
    df = df[metadata.column_names]
    if not skip_write_s3:
        # Write out dataframe, ensuring conformance with metadata
        writer.write(
            df,
            f"{db_location}/{name}/{start_date}.parquet",
            metadata=metadata,
        )
    return df


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
    bookings, locations = scrape_days_from_api(start_date, end_date)
    bookings = rename_bookings_df(bookings)
    locations = rename_locations_df(locations)
    write_raw_data_to_s3(bookings, locations, start_date)
    bookings_filepath, locations_filepath = validate_data(start_date, env)

    clean_data = {}

    for name, loc in zip(
        ["bookings", "locations"], [bookings_filepath, locations_filepath]
    ):
        clean_data[name] = read_and_write_cleaned_data(
            loc, start_date, name, db_version, env, skip_write_s3
        )
    return clean_data
