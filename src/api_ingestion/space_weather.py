# from ..logging_util import setup_logging
import requests
import polars as pl
from minio import Minio
import dotenv
import os

import logging
import logging.handlers
import json
import datetime


def format_json(record):
    """Format log record as simplified JSON string"""
    log_entry = {
        "time": datetime.datetime.fromtimestamp(record.created).isoformat(),
        "logger": record.name,
        "level": record.levelname,
        "message": record.getMessage(),
        "line": record.lineno,
    }
    if record.exc_info:
        log_entry["exception"] = logging._defaultFormatter.formatException(
            record.exc_info
        )
    return json.dumps(log_entry)


def setup_logging():
    """Setup logging with simplified JSON format and file rotation"""
    formatter = logging.Formatter()
    formatter.format = format_json

    file_handler = logging.handlers.RotatingFileHandler(
        "./logs/application.log", maxBytes=2 * 1024 * 1024, backupCount=1
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger("json_logger")
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()
dotenv.load_dotenv()

endpoint = os.getenv("MINIO_URL")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
secure_connection = False
API_URL = os.getenv("NASA_WEATHER_ALERTS_ENDPOINT")

client = Minio(
    endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=secure_connection
)


def fetch_api_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        response = response.json()
    else:
        logger.error(
            f"Error fetching API data from {url}: {response.status_code}")
        return None

    space_weather_data = pl.DataFrame(response)
    print(f"Fetched {len(space_weather_data)} records from {url}")
    print(space_weather_data.head())
    return space_weather_data


fetch_api_data(API_URL)

# def load_api_data(data):
#     bucket_name = "bronze_json"

#     try:
#         if not client.bucket_exists(bucket_name):
#             client.make_bucket(bucket_name)
#             logger.info(f"Bucket '{bucket_name}' created successfully.")
#         else:
#             logger.info(f"Bucket '{bucket_name}' already exists.")
#         client.fput_object(bucket_name)

#     except Exception as e:
#         logger.error(f"Error occurred while loading API data: {e}")
