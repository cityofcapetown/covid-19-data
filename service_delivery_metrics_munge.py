"""
script to calculate backlog % close and total requests opened metrics for city and department over a given period
"""

__author__ = "Colin Anthony"

# base imports
import json
import logging
import os
import pathlib
import sys
# external imports
from db_utils import minio_utils
import pandas as pd
import numpy as np

# set bucket constants
SERVICE_FACTS_BUCKET = "service-standards-tool.sd-request-facts"
SERVICE_ATTRIBUTES = "service-standards-tool.sd-request-fact-attributes"
COVID_BUCKET = "covid"
PRIVATE_PREFIX = "data/private/"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
LAKE_CLASSIFICATION = minio_utils.DataClassification.LAKE

# outfiles
DEPT_SERVICE_METRICS = "business_continuity_service_delivery_department_metrics"

# settings
DATE_FORMAT = "%Y-%m-%d"
START_DATE = pd.to_datetime('2020-10-12')
STALENESS_THRESHOLD = pd.Timedelta('28D')
MIN_PERIODS = 90
RESOLUTION = 3
ROLLLING_WINDOW = '180D'
DEFAULT_GREY = "#bababa"

SERVICE_STD = "service_standard"
BACKLOG = "backlog"
TOTAL_OPEN = "total_opened"
OPEN_COUNT = "opened_count"
CLOSED_COUNT = "closed_count"
CLOSED_IN_TARGET = "closed_within_target_sum"
HEX_INDEX_COL = "index"
DATE_COL = "date"
RES_COL = "resolution"
GEO_COL = "geometry"
CODE = "Code"
CODE_ID = "CodeID"
CODE_GRP = "CodeGroupID"
MEASURE = "measure"
VAL = "value"
FEATURE = "feature"
DIRCT = "directorate"
DEPT = "department"
DEPT_COLOUR = "dept_color"

INDEX_COLS = [DIRCT, DEPT, CODE]

SECRETS_PATH_VAR = "SECRETS_PATH"

DEPARTMENTS_CLR_DICT = {
    "Electricity": '#a6cee3',
    "Water and Sanitation": '#1f78b4',
    "Solid Waste Management": '#b2df8a',
    "Roads Infrastructure and Management": '#33a02c',
    "Public Housing": '#fb9a99',
    "Recreation and Parks": '#e31a1c'
}


def generate_request_code_name(df):
    names_series = df.apply(
        lambda row: f"{row[CODE]} ({row[CODE_GRP]}-{row[CODE_ID]})",
        axis=1
    )

    return names_series


def pivot_dataframe(df, index_cols=INDEX_COLS):
    logging.debug("Pre-Pivot[ing] aggregation on dataframe")
    pre_pivot_df = df.groupby(
        index_cols + [DATE_COL, MEASURE], as_index=False
    ).sum()
    logging.debug("Pre-Pivot[ed] aggregation on dataframe")

    logging.debug("Check[ing] for duplicates across index range")
    duplicate_index_test = pre_pivot_df[pre_pivot_df.duplicated(
        subset=index_cols + [DATE_COL, MEASURE])].copy()
    if not duplicate_index_test.empty:
        logging.error("Danger, duplicate values across the index, this will led to unwanted aggregation")
        sys.exit(-1)
    logging.debug("Check[ed] for duplicates across index range")

    logging.debug("Pivot[ing] dataframe")
    pivoted_df = pre_pivot_df.pivot_table(
        columns=MEASURE,
        values=VAL,
        index=index_cols + [DATE_COL]
    ).fillna(0)
    pivoted_df.sort_values(DATE_COL, inplace=True)
    logging.debug("Pivot[ed] dataframe")

    return pivoted_df


def calculate_metrics_dataframe(df, index_cols=INDEX_COLS):
    logging.debug("Add[ing] backlog calc")
    calc_df = df.copy()
    calc_df[BACKLOG] = calc_df[OPEN_COUNT] - calc_df[CLOSED_COUNT]
    logging.debug("Add[ed] backlog calc")

    logging.debug("Add[ing] backlog rolling sum calc")
    metrics_df = calc_df.reset_index().groupby(index_cols).apply(
        lambda groupby_df: (
            groupby_df.set_index(DATE_COL)
                      .resample(rule="1D").sum()  # Resample to make sure there is a value for each day (even if it's 0)
                      .rolling(ROLLLING_WINDOW, min_periods=MIN_PERIODS).sum()  # Find the rolling sum
        )
    ).reset_index()
    logging.debug("Add[ed] backlog rolling sum calc")

    logging.debug("Add[ing] service standard calc")
    metrics_df[SERVICE_STD] = (metrics_df[CLOSED_IN_TARGET] / metrics_df[CLOSED_COUNT] * 100)
    metrics_df[SERVICE_STD].replace(np.inf, 0, inplace=True)
    logging.debug("Add[ed] service standard calc")

    return metrics_df


def select_latest_value(df, index_cols=INDEX_COLS, cut_off_date=None):
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])

    if cut_off_date:
        df = df.query(f"{DATE_COL} <= @cut_off_date")

    filtered_df = df.copy().sort_values(
        by=DATE_COL
    ).drop_duplicates(
        subset=index_cols, keep='last'
    )

    # Filtering to select request types that are *not too old*
    max_date = filtered_df[DATE_COL].max()
    stale_date = max_date - STALENESS_THRESHOLD
    stale_indices = filtered_df.query(f"{DATE_COL} < @stale_date").index
    if stale_indices.shape[0]:
        logging.warning(
            f"Dropping the following entries because latest val is earlier than '{stale_date.strftime('%Y-%m-%d')}':\n"
            f"{filtered_df.loc[stale_indices]}"
        )
        filtered_df.drop(stale_indices, inplace=True)

    return filtered_df


def calc_total_values(df, pivot_df, index_cols=INDEX_COLS):
    logging.debug("Calculat[ing] total requests to date")
    opened_total = (
        pivot_df.query(f"{DATE_COL} >= @START_DATE")
                .groupby(index_cols)[OPEN_COUNT].sum()
                .rename(TOTAL_OPEN)
                .reset_index()
    )
    logging.debug("Calculat[ed] total requests to date")

    logging.debug("Merg[ing] metrics and total requests")
    combined_df = pd.merge(
        df,
        opened_total,
        on=index_cols,
        how="left",
        validate="1:1"
    )
    logging.debug("Merg[ed] metrics and total requests")

    return combined_df


def drop_nas(df, index_cols=INDEX_COLS):
    logging.debug(f"( pre-NaN drop) res3_combined.shape={df.shape}")
    clean_df = df.dropna(subset=[
        col for col in df.columns if col not in index_cols
    ], how='all')
    logging.debug(f"(post-NaN drop) res3_combined.shape={df.shape}")

    return clean_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')
    # Loading secrets
    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        logging.info("Trying to load from file")
        secrets_file = pathlib.Path("/home/jovyan/secrets/service-turnaround-secrets.json").resolve()
        if not secrets_file.exists():
            print("could not find your secrets")
            sys.exit(-1)
        else:
            secrets = json.load(open(secrets_file))
            logging.info("Loaded secrets from file")
    else:
        logging.info("Setting secrets variables")
        secrets_path = os.environ[SECRETS_PATH_VAR]
        if not secrets_path:
            logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
            sys.exit(-1)
        secrets = json.load(open(secrets_path))

    logging.info(f"Fetch[ing] {SERVICE_FACTS_BUCKET}")
    service_dfs = []
    for df_bucket in [SERVICE_FACTS_BUCKET, SERVICE_ATTRIBUTES]:
        fetched_df = minio_utils.minio_to_dataframe(
            minio_bucket=df_bucket,
            minio_key=secrets["minio"]["lake"]["access"],
            minio_secret=secrets["minio"]["lake"]["secret"],
            data_classification=LAKE_CLASSIFICATION,
        )
        service_dfs.append(fetched_df)
    logging.info(f"Fetch[ed] {SERVICE_FACTS_BUCKET}")

    service_facts = service_dfs[0]
    service_attribs = service_dfs[1]

    for department in service_attribs[DEPT].unique():
        if department not in DEPARTMENTS_CLR_DICT.keys():
            DEPARTMENTS_CLR_DICT[department] = DEFAULT_GREY

    logging.info("Filter[ing] to hex 3 resolution")
    service_facts_hex_3 = service_facts.query("resolution == @RESOLUTION").copy().assign(
        date=lambda df: pd.to_datetime(df.date, format=DATE_FORMAT))
    logging.info("Filter[ed] to hex 3 resolution")

    logging.info("Merg[ing] to annotations")
    res3_facts_annotated = pd.merge(service_facts_hex_3, service_attribs, how="left", on=FEATURE, validate="m:1")
    logging.info("Merg[ed] to annotations")

    logging.info("Generat[ing] request code annotation name")
    res3_facts_annotated.loc[:, CODE] = generate_request_code_name(res3_facts_annotated)
    logging.info("Generat[ed] request code annotation name")

    logging.info("Pivot[ing] dataframe")
    res3_pivot_df = pivot_dataframe(res3_facts_annotated)
    logging.info("Pivot[ed] dataframe")

    logging.info("Calculat[ing] metric values")
    res3_calc_df = calculate_metrics_dataframe(res3_pivot_df)
    logging.info("Calculat[ed] metric values")

    logging.info("Filter[ing] metrics to latest data date")
    res3_filt_df = select_latest_value(res3_calc_df)
    logging.info("Filter[ed] metrics to latest data date")

    logging.info("Calculat[ing] total requests attribute")
    res3_combined = calc_total_values(res3_filt_df, res3_pivot_df)
    res3_combined.drop(columns=[DATE_COL], inplace=True)
    logging.info("Calculat[ed] total requests attribute")

    logging.info("Dropp[ing] any entries where all metrics are NaNs")
    res3_combined = drop_nas(res3_combined)
    logging.info("Dropp[ed] any entries where all metrics are NaNs")

    logging.info("Add[ing] color field")
    res3_combined[DEPT_COLOUR] = res3_combined[DEPT].map(DEPARTMENTS_CLR_DICT)
    logging.info("Add[ed] color field")

    # put the file in minio
    logging.info(f"Push[ing] collected department metrics data to minio")
    result = minio_utils.dataframe_to_minio(
        res3_combined,
        filename_prefix_override=f"{PRIVATE_PREFIX}{DEPT_SERVICE_METRICS}",
        minio_bucket=COVID_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
        data_classification=EDGE_CLASSIFICATION,
        data_versioning=False,
        file_format="csv")

    if not result:
        logging.debug(f"Send[ing] data to minio failed")
    logging.info(f"Push[ed] collected department metrics data to minio")

    logging.info(f"Done")
