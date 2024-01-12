import re
import os
from mojap_metadata import Metadata
from arrow_pd_parser import writer, reader, caster
import yaml
from data_linter import validation
from dataengineeringutils3.s3 import get_filepaths_from_s3_folder
from python_scripts.constants import (
    db_location,
)

from logging import getLogger

logger = getLogger(__name__)


def extract_timestamp(file_path: str) -> int:
    """From a filepath (transformed by data_linter), return the epoch timestamp in filename

    Parameters
    ----------
    file_path :
        Filepath with timestamp to extract

    Returns
    -------

        _description_
    """
    file_name = os.path.basename(file_path)
    match = re.search(r"raw-\d{4}-\d{2}-\d{2}-\d+-([0-9]+)\.jsonl", file_name)
    if match:
        epoch_time = match.groups()
        epoch_timestamp = int(epoch_time[0])
        return epoch_timestamp
    else:
        print(f"No timestamp in {file_name}")


def get_latest_file(file_paths: list[str]) -> str:
    """Function to get the latest file, dependent on latest epoch timestamp

    Parameters
    ----------
    file_paths :
        List of filenames with epoch timestamps in them

    Returns
    -------
        The one filepath with the latest epoch timestamp
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


def get_config(config_path):
    with open(config_path) as stream:
        config = yaml.safe_load(stream)
    return config


def validate_data():
    """Validates the data from the API given a start date."""
    config = get_config("config.yaml")
    validation.para_run_init(2, config)
    validation.para_run_validation(0, config)
    validation.para_run_validation(1, config)
    validation.para_collect_all_status(config)
    validation.para_collect_all_logs(config)
    land_files = get_filepaths_from_s3_folder(config["land-base-path"])
    pass_files = get_filepaths_from_s3_folder(config["pass-base-path"])
    fail_files = get_filepaths_from_s3_folder(config["fail-base-path"])
    assert (not land_files and not fail_files) and pass_files, logger.error(
        f"Failed to validate data, see one of {fail_files}"
    )
    logger.info("Latest ingest validated against schema")


def read_and_write_cleaned_data(
    start_date: str,
    db_version: str,
    env: str,
    skip_write_s3: bool = False,
):
    """Reads the clean data from s3, and writes to the database location
    in parquet format

    Parameters
    ----------
    raw_loc :
        Location of the raw, validated data to read from
    start_date :
        Start date of this data scrape
    name :
        Locations or bookings
    db_version :
        Version of the database
    env :
        Environment we to work in
    skip_write_s3 : optional
        Write to s3 or not, by default False
    """
    config = get_config("config.yaml")

    files = get_filepaths_from_s3_folder(config["pass-base-path"])

    booking_start_date_files = [
        file for file in files if start_date and "bookings" in file
    ]
    locations_start_date_files = [
        file for file in files if start_date and "locations" in file
    ]
    bookings_filepath = get_latest_file(booking_start_date_files)
    locations_filepath = get_latest_file(locations_start_date_files)
    logger.info(f"Files to read in: {bookings_filepath}, {locations_filepath}")
    for filepath, name in zip(
        [bookings_filepath, locations_filepath], ["bookings", "locations"]
    ):
        metadata = Metadata.from_json(f"metadata/{db_version}/{env}/{name}.json")
        df = reader.read(filepath)
        df = df.reindex(columns=metadata.column_names)
        df = df[metadata.column_names]
        df = caster.cast_pandas_table_to_schema(df, metadata)
        if not skip_write_s3:
            # Write out dataframe, ensuring conformance with metadata
            writer.write(
                df,
                f"{db_location}/{name}/{start_date}.parquet",
                metadata=metadata,
            )
            logger.info(f"{name} data for {start_date} written to s3.")
