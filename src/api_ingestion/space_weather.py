import requests
import polars as pl
from minio import Minio
import dotenv
import os
from io import BytesIO
import logging
import logging.handlers
import json
import datetime
from src.logger_config import setup_logging

logger = setup_logging()
dotenv.load_dotenv()

endpoint = os.getenv("MINIO_EXTERNAL_URL")
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
    space_weather_data = requests.get(url)
    if space_weather_data.status_code == 200:
        space_weather_data = space_weather_data.json()
    else:
        logger.error(
            f"Error fetching API data from {url}: {space_weather_data.status_code}")
        return None

    return space_weather_data


def load_api_data(data):
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    object_name = "space_weather.json"

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")

        # Convert JSON data to string
        json_data = json.dumps(data, indent=2)

        json_bytes = BytesIO(json_data.encode('utf-8'))

        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=json_bytes,
            length=len(json_data.encode('utf-8')),
            content_type='application/json'
        )

        logger.info(
            f"Successfully uploaded {object_name} to bucket '{bucket_name}'")
        return object_name

    except Exception as e:
        logger.error(f"Error occurred while loading API data: {e}")
        return None


if __name__ == "__main__":
    data = fetch_api_data(API_URL)
    if data is not None:
        result = load_api_data(data)
        if result:
            logger.info(
                f"Data pipeline completed successfully. File: {result}")
        else:
            logger.error("Failed to upload data to MinIO")
    else:
        logger.error("Failed to fetch data from API")
