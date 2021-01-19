import json
import os
import logging
import sys

from db_utils import minio_utils
import pandas

import hr_data_munge

SERVICE_DELIVERY_SPATIAL_FILE = "data/private/business_continuity_service_delivery_spatial.csv"

DATE_COL = "date"
INDEX_COLS = ["resolution", "index", "feature"]

BACKLOG_COL = "backlog"
SERVICE_STANDARD_COL = "service_standard"
SERVICE_STANDARD_WEIGHTING_COL = "service_standard_weighting"  # delta will be how many have been closed
LONG_BACKLOG_COL = "long_backlog"
LONG_BACKLOG_WEIGHTING_COL = "long_backlog_weighting"  # delta will be how many been opened

METRICS_COLS = [BACKLOG_COL, SERVICE_STANDARD_COL, LONG_BACKLOG_COL, DATE_COL]
DELTA_COLS = [SERVICE_STANDARD_COL, SERVICE_STANDARD_WEIGHTING_COL,
              LONG_BACKLOG_COL, LONG_BACKLOG_WEIGHTING_COL,
              DATE_COL]
RELATIVE_DELTA_COLS = [BACKLOG_COL, ]

DELTA_SUFFIX = "_delta"
RELATIVE_SUFFIX = "_relative"

COVID_BUCKET = "covid"
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE
SERVICE_DELIVERY_SPATIAL_PREFIX = "data/private/business_continuity_service_delivery_spatial_metrics"


def calculate_metrics_by_hex(groupby_df):
    groupby_df = groupby_df.sort_values(by=DATE_COL)
    #logging.debug(f"groupby_df=\n{groupby_df}")

    delta_df = groupby_df[DELTA_COLS].diff(1)  # absolute change
    delta_df[DATE_COL] = delta_df[DATE_COL].dt.days
    delta_relative_df = (
                # relative change - delta divided by previous value
                groupby_df[RELATIVE_DELTA_COLS].diff(1) / groupby_df[RELATIVE_DELTA_COLS].shift(1)
    )

    result_series = pandas.concat([
        groupby_df[METRICS_COLS].iloc[-1],
        delta_df.add_suffix(DELTA_SUFFIX).iloc[-1],
        delta_relative_df.add_suffix(DELTA_SUFFIX).add_suffix(RELATIVE_SUFFIX).iloc[-1]
    ])
    #logging.debug(f"result_series=\n{result_series}")

    return result_series


def calculate_metrics(data_df):
    logging.debug(f"data_df=\n{data_df}")
    grouped_df = data_df.groupby(INDEX_COLS).apply(calculate_metrics_by_hex)
    logging.debug(f"grouped_df=\n{grouped_df}")

    return grouped_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("G[etting] Data")
    spatial_fact_df = hr_data_munge.get_data_df(SERVICE_DELIVERY_SPATIAL_FILE,
                                                secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    spatial_fact_df[DATE_COL] = pandas.to_datetime(spatial_fact_df[DATE_COL], format="%Y-%m-%d")
    spatial_fact_df.drop(columns=["Unnamed: 0"], inplace=True)
    logging.debug(f"spatial_fact_df.shape={spatial_fact_df.shape}")
    logging.debug(f"spatial_fact_df.columns={spatial_fact_df.columns}")
    logging.info("G[ot] Data")

    logging.info("Calculat[ing] metrics per hex")
    calc_df = calculate_metrics(spatial_fact_df)
    logging.info("Calculat[ed] metrics per hex")

    logging.info("Wr[iting] most recent data to Minio")
    minio_utils.dataframe_to_minio(calc_df, COVID_BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   BUCKET_CLASSIFICATION,
                                   filename_prefix_override=SERVICE_DELIVERY_SPATIAL_PREFIX,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("Wr[ote] most recent data to Minio")
