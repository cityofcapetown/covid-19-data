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

DELTA_SUFFIX = "_delta"
RELATIVE_SUFFIX = "_relative"

COVID_BUCKET = "covid"
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE
SERVICE_DELIVERY_SPATIAL_PREFIX = "data/private/business_continuity_service_delivery_spatial_metrics"


def calculate_metrics_by_hex(groupby_df):
    groupby_df = groupby_df.sort_values(by=DATE_COL)
    #logging.debug(f"groupby_df=\n{groupby_df}")

    delta_df = groupby_df.diff(1)  # absolute change
    rel_cols = [col for col in delta_df if col != DATE_COL]
    delta_relative_df = (
                delta_df[rel_cols] / groupby_df[rel_cols].shift(1)  # relative change - delta divided by previous value
    )

    result_series = pandas.concat([
        groupby_df.iloc[-1],
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
