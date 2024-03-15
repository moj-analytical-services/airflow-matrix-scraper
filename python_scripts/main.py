from functions.api_requests import (
    scrape_and_write_raw_bookings_data,
    scrape_and_write_raw_locations_data,
)
import logging
from context_filter import ContextFilter
from functions.data_validation import (
    validate_bookings_data,
    validate_locations_data,
    read_and_write_cleaned_bookings,
    read_and_write_cleaned_locations,
)
from constants import scrape_date, function_to_run

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


def main():
    functions = [
        scrape_and_write_raw_bookings_data,
        scrape_and_write_raw_locations_data,
        validate_bookings_data,
        validate_locations_data,
        read_and_write_cleaned_bookings,
        read_and_write_cleaned_locations,
    ]
    if not function_to_run:
        for func in functions:
            logger.info(f"Running function: {func.__name__}")
            func(scrape_date)
    else:
        function_map = {
            "scrape_and_write_raw_bookings_data": scrape_and_write_raw_bookings_data,
            "scrape_and_write_raw_locations_data": scrape_and_write_raw_locations_data,
            "validate_bookings_data": validate_bookings_data,
            "validate_locations_data": validate_locations_data,
            "read_and_write_cleaned_bookings": read_and_write_cleaned_bookings,
            "read_and_write_cleaned_locations": read_and_write_cleaned_locations,
        }
        run_function = function_map.get(function_to_run)
        logger.info(f"Running function: {function_to_run}")
        run_function(scrape_date)


if __name__ == "__main__":
    main()
