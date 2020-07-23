import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import geopandas
import numpy
import pandas

BUCKET = 'covid'
CLASSIFICATION = minio_utils.DataClassification.EDGE
VODACOM_HOURLY_METRIC_PREFIX = "data/staging/vodacom/hourly-metrics/"
VODACOM_HOURLY_METRIC_FILE_TEMPLATE = "{year:04d}{month:02d}{day:02d}_hourly_metrics.csv"

VODACOM_CT_POLYGONS = "data/private/ct_vodacom_polygons.geojson"
POLYGON_ID_COL = "id"

CITY_HEX_BUCKET = "city-hex_polygons"
HEX8_FILENAME = "city-hex-polygons-8.geojson"

HOURLY_METRIC_START = "2020-03-01"
HOURLY_METRIC_END = "2020-07-06"

START_DATE_COL = "start_date"
DATE_FORMAT = "%Y%m%d"
START_HOUR_COL = "start_hour"
HOUR_UNIT = "hours"
TIMESTAMP_COL = "timestamp"
HOURLY_METRIC_POLYGON_ID = "polygon_id"
UNIQUE_COUNT_COL = "unique_count"

ALL_COMBINED_HOURLY_METRICS = "data/private/all_mobile_device_counts"
CT_COMBINED_HOURLY_METRICS = "data/private/ct_mobile_device_counts"


def get_hourly_metric_file(date, minio_access, minio_secret):
    logging.debug(f"Fetching {date.strftime('%Y%m%d')}...")
    hm_filename = VODACOM_HOURLY_METRIC_FILE_TEMPLATE.format(year=date.year, month=date.month, day=date.day)

    with tempfile.NamedTemporaryFile("r") as temp_file:
        minio_utils.minio_to_file(
            filename=temp_file.name,
            minio_filename_override=f"{VODACOM_HOURLY_METRIC_PREFIX}{hm_filename}",
            minio_bucket=BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=CLASSIFICATION,
        )
        hm_df = pandas.read_csv(temp_file.name)

    return hm_df


def create_combined_hourly_metric_dataset(minio_access, minio_secret):
    metric_file_generator = (
        get_hourly_metric_file(date, minio_access, minio_secret)
        for date in pandas.date_range(HOURLY_METRIC_START, HOURLY_METRIC_END)
    )

    combined_df = pandas.concat(metric_file_generator)
    combined_df[TIMESTAMP_COL] = (
            pandas.to_datetime(combined_df[START_DATE_COL], format=DATE_FORMAT) +
            pandas.to_timedelta(combined_df[START_HOUR_COL], unit=HOUR_UNIT)
    )

    combined_df.drop([START_DATE_COL, START_HOUR_COL], axis='columns', inplace=True)

    logging.debug(f"combined_df.shape={combined_df.shape}")
    logging.debug(f"combined_df.head(5)=\n{combined_df.head(5)}")

    return combined_df[[TIMESTAMP_COL, HOURLY_METRIC_POLYGON_ID, UNIQUE_COUNT_COL]]


def get_minio_gdf(bucket_name, bucket_filename, minio_access, minio_secret):
    with tempfile.NamedTemporaryFile("r") as temp_file:
        minio_utils.minio_to_file(
            filename=temp_file.name,
            minio_filename_override=bucket_filename,
            minio_bucket=bucket_name,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=CLASSIFICATION,
        )
        minio_gdf = geopandas.read_file(temp_file.name)

    return minio_gdf


def filter_to_ct(hourly_metric_df, vodacom_polygon_gdf):
    return hourly_metric_df[
        hourly_metric_df[HOURLY_METRIC_POLYGON_ID].isin(vodacom_polygon_gdf[POLYGON_ID_COL])
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("Fetch[ing] data files")
    combined_metric_df = create_combined_hourly_metric_dataset(secrets["minio"]["edge"]["access"],
                                                               secrets["minio"]["edge"]["secret"])
    vodacom_gdf = get_minio_gdf(BUCKET, VODACOM_CT_POLYGONS,
                                secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("Fetch[ed] data files")

    logging.info("Filter[ing] down to Cape Town")
    ct_metric_df = filter_to_ct(combined_metric_df, vodacom_gdf)
    logging.info("Filter[ed] down to Cape Town")

    logging.info("Writ[ing] data to Minio")
    minio_utils.dataframe_to_minio(ct_metric_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=CT_COMBINED_HOURLY_METRICS,
                                   data_versioning=False,
                                   file_format="csv")
    minio_utils.dataframe_to_minio(combined_metric_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=ALL_COMBINED_HOURLY_METRICS,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("Wrot[e] data to Minio")
