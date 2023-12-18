import re
import pandas as pd
import requests
from dataengineeringutils.utils import read_json
from dataengineeringutils.pd_metadata_conformance import (
    impose_metadata_column_order_on_pd_df,
    impose_metadata_data_types_on_pd_df,
)
from arrow_pd_parser import writer
import s3_utils
from datetime import datetime, timedelta
from arrow_pd_parser import writer
from constants import (
    meta_path_bookings,
    table_location_bookings,
    table_location_locations,
)

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

def get_booking_categories(session):
    
    """ Returns pandas dataframe containing all booking categories 
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
    print(f"GET {resp.url}")
    print(f"response status code: {resp.status_code}")
    return resp.json()


def scrape_days_from_api(start_date, end_date, db_version, env, skip_write_s3 = True):
    
    """ 
        Scrapes the matrix API for a given period
        Writes outputs to s3 path as specified by 'env'
        
        Parameters:
            start_date (str): Start date in format %Y-%m-%d
            end_date (str): End date in format %Y-%m-%d
                can also be 'eod' to denote end of day
            env (str): Denotes whether to save results in 
                production (prod) or development (dev)
            write_to_s3 (bool): Allow user to skip writing to s3. 
                Default True
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
    booking_categories = list(df_booking_categories['locationKind'])
    
    # Derive booking parameters
    params = make_booking_params(
        start_date, end_date, booking_categories, pageNum=0, pageSize=page_size, status=status
    )

    
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
    
    # Get final dataframes with correct column names and data types
    bookings_data = get_bookings_df(bookings, db_version, env, start_date, skip_write_s3)
    locations_data = get_locations_df(locations, db_version, env, start_date, skip_write_s3)

    # User can skip writing to s3 if testing
    # if not skip_write_s3:
    #     bookings_data.to_parquet(
    #         f"s3://alpha-dag-matrix/db/{env}/bookings/{start_date}.parquet", index=False
    #     )


    #     locations_data.to_parquet(
    #         f"s3://alpha-dag-matrix/db/{env}/locations/data.parquet", index=False
    #     )

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


def get_bookings_df(bookings, db_version, env, start_date, skip_write_s3=False):

    # Convert the nested data into tablular format
    bookings_df = pd.json_normalize(bookings)
    renames = read_json(f"metadata/{db_version}/{env}/bookings_renames.json")

    # Table metadata
    bookings_metadata = read_json(meta_path_bookings)

    if len(bookings_df) > 0:
        bookings_df = bookings_df.reindex(columns=renames.keys())
        bookings_df = bookings_df[renames.keys()].rename(columns=renames)
    else:
        #
        bookings_df = pd.DataFrame(columns=renames.values())
    
    # Ensure column order correct
    bookings_df = impose_exact_conformance_on_pd_df(bookings_df, bookings_metadata)

    # Write out dataframe, ensuring conformance with metadata
    if not skip_write_s3: 
        writer.write(bookings_df, f"{table_location_bookings}/{start_date}.parquet", metadata=bookings_metadata)

    return bookings_df


def get_locations_df(locations, db_version, env,start_date, skip_write_s3):
    locations_df = pd.json_normalize(locations)
    renames = read_json(f"metadata/{db_version}/{env}/locations_renames.json")
    locations_df = locations_df[renames.keys()].rename(columns=renames)
    locations_metadata = read_json(f"metadata/{db_version}/{env}/locations.json")

    locations_df = impose_exact_conformance_on_pd_df(locations_df, locations_metadata)

    if not skip_write_s3: 
        writer.write(locations_df, f"{table_location_locations}/{start_date}.parquet", metadata=locations_metadata)
    
    return locations_df


def impose_metadata_data_types(df, metadata):
    """Convert pandas datatypes
        given mojap-metadata object
        Original function (impose_metadata_data_types_on_pd_df) 
            from dataengineeringutils.pd_metadata_conformance
            was failing.


    Args:
        df (pandas dataframe): Dataframe to have dtypes updated
        metadata (mojap_metadata.Metadata): Table schema

    Returns:
        pandas dataframe: Original dataframe with new datatypes
    """

    # Convert schema datatypes to class objects (mapping)
    typemap = {'string':str,
               'timestamp':'datetime',
               'datetime':'datetime',
               'int':"Int64",
               'float':float,
               'bool':bool

               }
    
    # For each column specified in the metadata object
    for column in metadata['columns']:

        # Variable name
        name = column['name']

        # Col type from metadata
        type = column['type']
        
        # Mapped column type (e.g. int64 and int32 both mapped to int)
        map_type = [typemap[key] for key in typemap if re.search(key, type)]

        # Check exactly one match returned
        if len(map_type) != 1:
            raise KeyError(f"Data type {type} not found in typemap")
        else: 
            map_type = map_type[0]
    
        # If the mapped datatype is a datetime
        if map_type == 'datetime':
            df[name] = pd.to_datetime(df[name])
        # Some strings have 'nan' values as the original data was json 
        # The normalized data doesn't always have data and pandas inserts 'nan' 
        elif type == 'string':
            df[name] = df[name].fillna('').astype(map_type)
        # Else, use the class object
        else:
            df[name] = df[name].astype(map_type)
    return df

def impose_exact_conformance_on_pd_df(df, table_metadata):
    df = impose_metadata_column_order_on_pd_df(
        df,
        table_metadata,
        delete_superflous_colums=True,
        create_cols_if_not_exist=True,
    )
    df = impose_metadata_data_types(df, table_metadata)
    return df
