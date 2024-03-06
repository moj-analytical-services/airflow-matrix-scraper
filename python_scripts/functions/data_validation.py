import re
import os
from mojap_metadata import Metadata
from arrow_pd_parser import writer, reader, caster
from data_linter import validation
from dataengineeringutils3.s3 import get_filepaths_from_s3_folder
from python_scripts.constants import (
    db_location,
    meta_path_bookings,
    meta_path_locations,
)
import logging
from context_filter import ContextFilter

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(funcName)s | %(levelname)s | %(context)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

root_logger = logging.getLogger()

for handler in root_logger.handlers:
    handler.filters = []
    handler.addFilter(ContextFilter())


LAND_BUCKETS = {"preprod": "mojap-land-dev", "prod": "mojap-land"}
# temp change!
RAW_HIST_BUCKETS = {"preprod": "mojap-raw-hist", "prod": "mojap-raw-hist"}

BASE_CONFIG = {
    "land-base-path": "s3://{bucket}/corporate/matrix",
    "fail-base-path": "s3://{bucket}/corporate/matrix/fail/",
    "pass-base-path": "s3://{bucket}/corporate/matrix/pass/",
    "log-base-path": "s3://{bucket}/corporate/matrix/log/",
    "compress-data": False,
    "remove-tables-on-pass": True,
    "all-must-pass": False,
}

TABLE_CONFIG = {
    "required": True,
    "allow-unexpected-data": True,
    "allow-missing-cols": True,
}

META_PATH = {
    "bookings": meta_path_bookings,
    "locations": meta_path_locations,
}


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


def create_config(scrape_date, env, table):
    buckets = {"land": LAND_BUCKETS[env], "raw-hist": RAW_HIST_BUCKETS[env]}
    config = BASE_CONFIG
    config["land-base-path"] = config["land-base-path"].format(bucket=buckets["land"])
    config["fail-base-path"] = config["fail-base-path"].format(
        bucket=buckets["raw-hist"]
    )
    config["pass-base-path"] = config["pass-base-path"].format(
        bucket=buckets["raw-hist"]
    )
    config["log-base-path"] = config["log-base-path"].format(bucket=buckets["raw-hist"])
    config["tables"] = {}
    config["tables"][table] = TABLE_CONFIG
    if scrape_date:
        config["tables"][table]["pattern"] = f"/{table}/{scrape_date}"
    else:
        config["tables"][table]["pattern"] = f"/{table}"
    config["tables"][table]["metadata"] = META_PATH[table]
    return config


def validate_data(scrape_date, env, table):
    """Validates the data from the API given a start date."""
    config = create_config(scrape_date, env, table)
    logger.info(
        f"looking for data at: {config['land-base-path']}/{table}/{config['tables'][table]['pattern']}"
    )
    validation.para_run_init(1, config)
    validation.para_run_validation(0, config)
    validation.para_collect_all_status(config)
    validation.para_collect_all_logs(config)


def assert_no_files(scrape_date, env, table):
    config = create_config(scrape_date, env, table)
    land_files = get_filepaths_from_s3_folder(config["land-base-path"])
    land_files = [
        file
        for file in land_files
        if re.search(
            f"{table}.*{scrape_date}",
            file.replace(config["land-base-path"], ""),
        )
    ]
    pass_files = get_filepaths_from_s3_folder(config["pass-base-path"])
    pass_files = [
        file
        for file in pass_files
        if re.search(
            f"{table}.*{scrape_date}",
            file.replace(config["pass-base-path"], ""),
        )
    ]
    fail_files = get_filepaths_from_s3_folder(config["fail-base-path"])
    fail_files = [
        file
        for file in fail_files
        if re.search(
            f"{table}.*{scrape_date}",
            file.replace(config["fail-base-path"], ""),
        )
    ]
    assert (not land_files and not fail_files) and pass_files, logger.error(
        f"Failed to validate data for {scrape_date}, see one of {fail_files}"
    )
    logger.info(f"Latest ingest validated against schema for {scrape_date}")


def validate_bookings_data(scrape_date, env):
    validate_data(scrape_date, env, "bookings")
    assert_no_files(scrape_date, env, "bookings")


def validate_locations_data(scrape_date, env):
    validate_data(scrape_date, env, "locations")
    assert_no_files(scrape_date, env, "locations")


def read_and_write_cleaned_data(
    start_date: str,
    env: str,
    name: str,
    skip_write_s3: bool = False,
    latest: bool = True,
):
    """Reads the clean data from s3, and writes to the database location
    in parquet format

    Parameters
    ----------
    start_date :
        Start date of this data scrape
    env :
        Environment we to work in
    skip_write_s3 : optional
        Write to s3 or not, by default False
    """
    config = create_config(start_date, env, name)

    files = get_filepaths_from_s3_folder(config["pass-base-path"])

    start_date_files = [file for file in files if start_date and "bookings" in file]
    metapath = config["tables"][name]["metadata"]
    if latest:
        filepath = get_latest_file(start_date_files)
    else:
        filepath = start_date_files[0]
    logger.info(f"File to read in: {filepath}")
    metadata = Metadata.from_json(metapath)
    df = reader.read(filepath)
    df = df.reindex(columns=metadata.column_names)
    df = df[metadata.column_names]
    df = caster.cast_pandas_table_to_schema(df, metadata)
    if not skip_write_s3:
        # Write out dataframe, ensuring conformance with metadata
        writer.write(
            df,
            f"{db_location}/{name}/scrape_date={start_date}/{start_date}.parquet",
            metadata=metadata,
        )
        logger.info(f"{name} data for {start_date} written to s3.")


def read_and_write_cleaned_bookings(start_date, env):
    read_and_write_cleaned_data(start_date, env, "bookings")


def read_and_write_cleaned_locations(start_date, env):
    read_and_write_cleaned_data(start_date, env, "locations")


def rebuild_all_s3_data_from_raw(env):
    for name in ["bookings", "locations"]:
        config = create_config(None, env, name)
        files = get_filepaths_from_s3_folder(config["pass-base-path"])
        for file in files:
            match = re.search(r"raw-(\d{4})-(\d{2})-(\d{2})-\d+-[0-9]+\.jsonl", file)
            if match:
                matches = match.groups()
                start_date = matches[0] + "-" + matches[1] + "-" + matches[2]
                start_date_file = r"{name}/raw-{start_date}-\d+-\d+\.jsonl".format(
                    start_date=start_date, name=name
                )
                start_date_files = [
                    file
                    for file in files
                    if re.match(start_date_file, file.split("pass/")[-1])
                ]
                metapath = config["tables"][name]["metadata"]
                filepath = start_date_files[-1]
                logger.info(f"File to read in: {filepath}")
                metadata = Metadata.from_json(metapath)
                df = reader.read(filepath)
                df = df.reindex(columns=metadata.column_names)
                df = df[metadata.column_names]
                df = caster.cast_pandas_table_to_schema(df, metadata)
                # Write out dataframe, ensuring conformance with metadata
                writer.write(
                    df,
                    f"{db_location}/{name}/scrape_date={start_date}/{start_date}.parquet",
                    metadata=metadata,
                )
                logger.info(f"{name} data for {start_date} written to s3.")
