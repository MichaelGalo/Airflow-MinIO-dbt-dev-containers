import os
from minio import Minio
from minio.error import S3Error
import dotenv
# from src.logger_config import setup_logging
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import logging
import logging.handlers
import json
import datetime
import snowflake.connector
import io


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


def minio_raw_data_to_snowflake():

    try:

        minio_client = Minio(
            endpoint=os.getenv("MINIO_EXTERNAL_URL"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )
        logger.info("Connected to MinIO")

        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA_BRONZE"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
        cursor = conn.cursor()
        logger.info("Connected to Snowflake")

        objects_to_process = minio_client.list_objects(
            bucket_name=os.getenv("MINIO_BUCKET_NAME"),
            recursive=True
        )

        space_weather_data = []
        for obj in objects_to_process:
            if obj.object_name.endswith('.json'):
                space_weather_data.append(obj.object_name)
                logger.info(f"Found JSON file: {obj.object_name}")

                try:
                    minio_response = minio_client.get_object(
                        bucket_name=os.getenv("MINIO_BUCKET_NAME"),
                        object_name=obj.object_name
                    )
                    data = minio_response.read().decode('utf-8')
                    logger.info(f"Read data from {obj.object_name}")
                    minio_response.close()
                    minio_response.release_conn()
                    logger.info(
                        f"Data from {obj.object_name} processed successfully")

                    df = pd.read_json(io.StringIO(data))
                    logger.info(
                        f"Transformed data from {obj.object_name} to DataFrame")

                    result = write_pandas(
                        conn,
                        df,
                        table_name="space_weather_raw",
                        database=os.getenv("SNOWFLAKE_DATABASE"),
                        auto_create_table=True,
                        schema=os.getenv("SNOWFLAKE_SCHEMA_BRONZE"),
                        overwrite=True
                    )
                    success = result[0]
                    nrows = result[2] if len(result) > 2 else result[1]
                    if success:
                        logger.info(
                            f"Data from {obj.object_name} written to Snowflake successfully: {nrows} rows")
                    else:
                        logger.error(
                            f"Failed to write data from {obj.object_name} to Snowflake")
                except Exception as e:
                    logger.error(
                        f"Error processing {obj.object_name}: {e}")

    except S3Error as e:
        logger.error(f"MinIO S3Error: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


minio_raw_data_to_snowflake()
