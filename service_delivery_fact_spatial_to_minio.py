import json
import os
import logging
import sys

from db_utils import minio_utils
import pandas

import service_delivery_fact_ts_to_minio
import service_delivery_metrics_munge

INDEX_COLS = ["resolution", "index", "feature"]

HEX_RESOLUTION = 7

REFERENCE_DATE = "2020-10-12"

COVID_BUCKET = "covid"
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE
SERVICE_DELIVERY_SPATIAL_PREFIX = "data/private/business_continuity_service_delivery_spatial"


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
    source_fact_df = pandas.concat((
        service_delivery_fact_ts_to_minio.get_fact_dataset(fact_datatset,
                                                           secrets["minio"]["lake"]["access"],
                                                           secrets["minio"]["lake"]["secret"])
        for fact_datatset in service_delivery_fact_ts_to_minio.FACTS_DATASETS
    ))
    logging.debug(f"source_fact_df.shape={source_fact_df.shape}")
    logging.debug(f"source_fact_df.columns={source_fact_df.columns}")
    logging.info("G[ot] Data")

    logging.info("Filter[ing] by hex resolution")
    filtered_fact_df = service_delivery_fact_ts_to_minio.filter_by_resolution(source_fact_df, HEX_RESOLUTION)
    logging.debug(f"filtered_fact_df.shape={filtered_fact_df.shape}")
    logging.debug(f"filtered_fact_df.columns={filtered_fact_df.columns}")
    logging.info("Filter[ed] by hex resolution")

    logging.info("Pivot[ing] data")
    pivot_df = service_delivery_metrics_munge.pivot_dataframe(filtered_fact_df, INDEX_COLS).reset_index()
    logging.info("Pivot[ed] data")

    logging.info("Select[ing] reference and most recent data")
    filtered_df = pandas.concat([
        service_delivery_metrics_munge.select_latest_value(pivot_df, INDEX_COLS, REFERENCE_DATE),
        service_delivery_metrics_munge.select_latest_value(pivot_df, INDEX_COLS)
    ])
    logging.info("Select[ed] reference and most recent data")

    logging.info("Wr[iting] most recent data to Minio")
    minio_utils.dataframe_to_minio(filtered_df, COVID_BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   BUCKET_CLASSIFICATION,
                                   filename_prefix_override=SERVICE_DELIVERY_SPATIAL_PREFIX,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("Wr[ote] most recent data to Minio")
