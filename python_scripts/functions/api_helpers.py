import requests
import json
import pandas as pd
import re
import python_scripts.s3_utils as s3_utils
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
    secrets = get_secrets()
    username = secrets["username"]
    password = secrets["password"]

    url = "https://app.matrixbooking.com/api/v1/user/login"
    session.post(url, json={"username": username, "password": password})
    return session


def make_booking_params(
    time_from: str,
    time_to: str,
    pageSize: int = None,
    pageNum: int = 0,
) -> dict:
    params = {
        "f": time_from,
        "t": time_to,
        "include": "audit",
        "pageSize": pageSize,
        "pageNum": pageNum,
    }
    return params


def extract_locations(json_list):
    all_locations = []

    for json_data in json_list:
        extract_locations_recursive(json_data, all_locations)

    for json_data in json_list:
        if "organisation" in json_data:
            del json_data["organisationId"]

    return all_locations


def extract_locations_recursive(json_data, result):
    if "locations" in json_data:
        for location in json_data["locations"]:
            result.append(location)
            extract_locations_recursive(location, result)


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


def camel_to_snake_case(input_str: str) -> str:
    # Using regular expressions to find positions with capital letters and insert underscores
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", input_str)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)

    # Handle the case where multiple uppercase letters are present
    snake_case_str = re.sub("([a-z])([A-Z]+)", r"\1_\2", s2).lower()

    return snake_case_str


def fix_faulty_time_col(df, col):
    column = df[col].copy()
    # Check for missing parts (seconds, minutes, hours, microseconds)
    missing_parts = column.apply(
        lambda x: (pd.notna(x) and (len(str(x).split(":")) < 3 or "." not in str(x)))
    )

    def format_timestamp(raw_string):
        # Generate dynamic format based on missing parts
        num_parts = len(raw_string.split(":"))

        format_str = (
            "%Y-%m-%dT"
            + ":".join(["%H", "%M", "%S"][:num_parts])
            + (".%f" if "." in raw_string else "")
        )

        return pd.to_datetime(raw_string, format=format_str).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )

    column.loc[missing_parts] = column.loc[missing_parts].apply(format_timestamp)
    return column
