import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import pandas

BUCKET = 'covid'
CLASSIFICATION = minio_utils.DataClassification.EDGE
CT_COMBINED_HOURLY_METRICS = "data/private/ct_mobile_device_counts.csv"

TIMESTAMP_COL = "timestamp"
HOURLY_METRIC_POLYGON_ID = "polygon_id"
UNIQUE_COUNT_COL = "unique_count"

UPTIME_COL_PREFIX = "Uptime_"
UPTIME_START = "08:00"
UPTIME_END = "19:00"
DOWNTIME_COL_PREFIX = "Downtime_"

CT_MOBILE_METRICS_TIDY = "data/private/ct_mobile_device_counts_tidy_metrics"
CT_MOBILE_METRICS = "data/private/ct_mobile_device_count_metrics"


def get_combined_metric_file(minio_access, minio_secret):
    logging.debug(f"Fetching {CT_COMBINED_HOURLY_METRICS}...")

    with tempfile.NamedTemporaryFile("r") as temp_file:
        minio_utils.minio_to_file(
            filename=temp_file.name,
            minio_filename_override=CT_COMBINED_HOURLY_METRICS,
            minio_bucket=BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=CLASSIFICATION,
        )
        hm_df = pandas.read_csv(temp_file.name)
        hm_df[TIMESTAMP_COL] = pandas.to_datetime(hm_df[TIMESTAMP_COL])

    return hm_df


def calculate_uptime_and_downtime(combined_df):
    logging.debug("Resampl[ing] Data by day, into uptime and downtime metrics...")
    combined_df.set_index([HOURLY_METRIC_POLYGON_ID, TIMESTAMP_COL], inplace=True)

    tidy_df = combined_df[UNIQUE_COUNT_COL].groupby(HOURLY_METRIC_POLYGON_ID, sort=False).apply(
        lambda groupby_df: (
            groupby_df.resample("1D", level=TIMESTAMP_COL).apply(
                lambda resample_df: (
                    resample_df.iloc[
                        resample_df.index.get_level_values(TIMESTAMP_COL).indexer_between_time(UPTIME_START, UPTIME_END)
                    ].describe().add_prefix(UPTIME_COL_PREFIX).append(
                        resample_df.iloc[
                            resample_df.index.get_level_values(TIMESTAMP_COL).indexer_between_time(UPTIME_END,
                                                                                                   UPTIME_START,
                                                                                                   include_start=False,
                                                                                                   include_end=False)
                        ].describe().add_prefix(DOWNTIME_COL_PREFIX)
                    ))
            )
        )
    ).to_frame()
    logging.debug(f"tidy_df=\n{tidy_df}")
    logging.debug("...Resampl[ed] Data by day, into uptime and downtime metrics")

    return tidy_df


def pivot_data(tidy_df):
    logging.debug("Pivot[ing] data...")

    final_index_name = f"level_{len(tidy_df.index.names) - 1}"
    pivot_df = tidy_df.reset_index(-1).pivot_table(
        columns=final_index_name, values=UNIQUE_COUNT_COL, index=[HOURLY_METRIC_POLYGON_ID, TIMESTAMP_COL]
    )
    logging.debug(f"pivot_df=\n{pivot_df}")
    logging.debug("...Pivot[ed] data")

    return pivot_df


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
    combined_hourly_df = get_combined_metric_file(secrets["minio"]["edge"]["access"],
                                                  secrets["minio"]["edge"]["secret"])
    logging.info("Fetch[ed] data files")

    logging.info("Calculat[ing] Uptime and Downtime Counts")
    combined_tidy_df = calculate_uptime_and_downtime(combined_hourly_df)
    logging.info("Calculat[ed] Uptime and Downtime Counts")

    logging.info("Pivot[ing] counts")
    combined_pivot_df = pivot_data(combined_tidy_df)
    logging.info("Pivot[ed] counts")

    logging.info("Writ[ing] data to Minio")
    minio_utils.dataframe_to_minio(combined_tidy_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=CT_MOBILE_METRICS_TIDY,
                                   data_versioning=False,
                                   file_format="csv")

    minio_utils.dataframe_to_minio(combined_pivot_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=CT_MOBILE_METRICS,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("Wrot[e] data to Minio")
