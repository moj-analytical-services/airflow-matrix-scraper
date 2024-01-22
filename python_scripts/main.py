from functions.api_requests import scrape_and_write_raw_data
import logging
from context_filter import ContextFilter
from functions.data_validation import validate_data, read_and_write_cleaned_data
from constants import scrape_date, function_to_run, env

logging.basicConfig(
    level=logging.INFO,
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
