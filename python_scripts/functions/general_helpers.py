import argparse
import logging

logger = logging.getLogger(__name__)


def get_command_line_arguments():
    # Init the parser
    parser = argparse.ArgumentParser(description="Optional app description")

    # Date to scrape
    parser.add_argument(
        "--scrape_date",
        type=str,
        required=True,
        help="Date to scrape, as string in format %Y-%m-%d",
    )

    # Environment (preproduction or production)
    parser.add_argument(
        "--env",
        "-e",
        type=str,
        choices=["dev", "preprod", "prod"],
        required=True,
        help="Environment (preproduction or production) to store results in. Takes values preprod or prod",
    )

    # Writing to s3
    parser.add_argument(
        "--skip-write-s3",
        action=argparse.BooleanOptionalAction,
        help="If passed, this will skip writing to s3 (default is to write to s3 when scaper runs)",
    )

    parser.add_argument(
        "--function", type=str, help="Name of the function to run (optional)"
    )

    return parser.parse_args()
