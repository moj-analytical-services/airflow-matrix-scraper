from api_requests import scrape_and_write_raw_data
from logging import getLogger
from functions.data_validation import validate_data, read_and_write_cleaned_data
from constants import scrape_date, function_to_run, env

logger = getLogger(__name__)


def main():
    functions = [
        scrape_and_write_raw_data,
        validate_data,
        read_and_write_cleaned_data,
    ]
    if not function_to_run:
        for func in functions:
            logger.info(f"Running function: {func.__name__}")
            func(scrape_date, env)
    else:
        function_map = {
            "scrape_and_write_raw_data": scrape_and_write_raw_data,
            "validate_data": validate_data,
            "read_and_write_cleaned_data": read_and_write_cleaned_data,
        }
        run_function = function_map.get(function_to_run)
        logger.info(f"Running function: {function_to_run}")
        run_function(scrape_date, env)


if __name__ == "__main__":
    main()
