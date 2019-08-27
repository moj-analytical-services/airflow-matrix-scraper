import argparse
import json
import datetime
import logging
from dateutil.parser import parse
import sys

from api_requests import scrape_days_from_api

argp = argparse.ArgumentParser(description='Optional app description')

argp.add_argument('--scrape_date', type = 'str', description = 'Date to scrape, as string in format %Y-%m-%d')

scrape_date = parse(argp.scrape_date)

scrape_days_from_api(scrape_date,'eod')