import pandas as pd
import requests
from dataengineeringutils.utils import read_json
from dataengineeringutils.pd_metadata_conformance import (
    impose_metadata_column_order_on_pd_df,
    impose_metadata_data_types_on_pd_df,
)
import s3_utils
from datetime import datetime, timedelta


def get_secrets():
    return s3_utils.read_json_from_s3(
        "alpha-dag-matrix/api_secrets/secrets.json"
    )


def matrix_authenticate(session):
    secrets = get_secrets()
    username = secrets["username"]
    password = secrets["password"]

    url = "https://app.matrixbooking.com/api/v1/user/login"
    session.post(url, json={"username": username, "password": password})
    return session


def make_booking_params(
    time_from, time_to, status=None, pageSize=None, pageNum=0
):
    params = {
        "f": time_from,
        "t": time_to,
        "bc": "ROOM",
        "status": status,
        "include": ["audit", "locations"],
        "pageSize": pageSize,
        "pageNum": pageNum,
    }
    return params


def get_payload(session, url, parameters):
    resp = session.get(url=url, cookies=session.cookies, params=parameters)
    print(f"GET {resp.url}")
    print(f"response status code: {resp.status_code}")
    return resp.json()


def scrape_days_from_api(start_date, end_date):

    url = "https://app.matrixbooking.com/api/v1/booking"
    page_size = 2500
    status = ["CONFIRMED", "TENTATIVE", "CANCELLED"]

    params = make_booking_params(
        start_date, end_date, pageNum=0, pageSize=page_size, status=status
    )

    bookings = []

    ses = requests.session()
    matrix_authenticate(ses)
    # Scrape the first page of data
    print(f"scraping page 0")
    data = get_payload(ses, url, params)
    rowcount = len(data["bookings"])
    print(f"records scraped: {rowcount}")

    bookings = data["bookings"]
    locations = data["locations"]

    i = 1
    total_rows = rowcount
    while rowcount == page_size:
        print(f"scraping page {i}")
        params = make_booking_params(
            start_date, end_date, pageNum=i, pageSize=page_size, status=status
        )
        data = get_payload(ses, url, params)
        rowcount = len(data["bookings"])
        print(f"records scraped: {rowcount}")
        if rowcount > 0:
            bookings.extend(data["bookings"])
        i += 1
        total_rows += rowcount

    print(f"Retrieved {len(locations)} locations")

    bookings_data = get_bookings_df(bookings)
    #bookings_data.to_parquet(
    #    f"s3://alpha-dag-matrix/bookings/{start_date}.parquet", index=False
    #)

    locations_data = get_locations_df(locations)
    #locations_data.to_parquet(
    #    f"s3://alpha-dag-matrix/locations/data.parquet", index=False
    #)

    return (bookings, locations)


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


def get_bookings_df(bookings):
    bookings_df = pd.io.json.json_normalize(bookings)
    renames = read_json("metadata/bookings_renames.json")
    bookings_metadata = read_json("metadata/bookings.json")

    if len(bookings_df) > 0:
        bookings_df = bookings_df.reindex(columns=renames.keys())
        bookings_df = bookings_df[renames.keys()].rename(columns=renames)
    else:
        bookings_df = pd.DataFrame(columns=renames.values())

    bookings_df = impose_exact_conformance_on_pd_df(
        bookings_df, bookings_metadata
    )

    return bookings_df


def get_locations_df(locations):
    locations_df = pd.io.json.json_normalize(locations)
    renames = read_json("metadata/locations_renames.json")
    locations_df = locations_df[renames.keys()].rename(columns=renames)
    locations_metadata = read_json("metadata/locations.json")

    locations_df = impose_exact_conformance_on_pd_df(
        locations_df, locations_metadata
    )

    return locations_df


def impose_exact_conformance_on_pd_df(df, table_metadata):
    df = impose_metadata_column_order_on_pd_df(
        df,
        table_metadata,
        delete_superflous_colums=True,
        create_cols_if_not_exist=True,
    )
    df = impose_metadata_data_types_on_pd_df(df, table_metadata)
    return df
