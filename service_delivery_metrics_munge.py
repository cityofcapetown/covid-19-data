"""
script to calculate backlog % close and total requests opened metrics for city and department over a given period
"""

__author__ = "Colin Anthony"

# base imports
from datetime import timedelta
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
START_DATE = pd.to_datetime('2020-10-12')
MIN_PERIODS = 90
RESOLUTION = 3
DELTA_DAYS = 2
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
INDEX_COLS = ["directorate", "department", CODE]

SECRETS_PATH_VAR = "SECRETS_PATH"

DEPARTMENTS_CLR_DICT = {
    "Electricity": '#a6cee3',
    "Water and Sanitation": '#1f78b4',
    "Solid Waste Management": '#b2df8a',
    "Roads Infrastructure and Management": '#33a02c',
    "Public Housing": '#fb9a99',
    "Recreation and Parks": '#e31a1c'
}


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

    for department in service_attribs["department"].unique():
        if department not in DEPARTMENTS_CLR_DICT.keys():
            DEPARTMENTS_CLR_DICT[department] = DEFAULT_GREY

    logging.info("Filter[ing] to hex 3 resolution")
    service_facts_hex_3 = service_facts.query("resolution == @RESOLUTION").copy().assign(
        date=lambda df: pd.to_datetime(df.date, format="%Y-%m-%d"))
    logging.info("Filter[ed] to hex 3 resolution")

    logging.info("Merg[ing] to annotations")
    res3_facts_annotated = pd.merge(service_facts_hex_3, service_attribs, how="left", on="feature", validate="m:1")
    logging.info("Merg[ed] to annotations")

    logging.info("Generat[ing] request code annotation name")
    res3_facts_annotated.loc[:, "Code"] = res3_facts_annotated['Code'] + " (" + res3_facts_annotated[
        'CodeGroupID'] + "-" + res3_facts_annotated['CodeID'] + ")"
    logging.info("Generat[ed] request code annotation name")

    logging.info("Pre-Pivot[ing] aggregation on dataframe")
    res3_pre_pivot_agg_df = res3_facts_annotated.groupby(
        INDEX_COLS + [DATE_COL, MEASURE], as_index=False).sum()
    logging.info("Pre-Pivot[ed] aggregation on dataframe")

    logging.info("Check[ing] for duplicates across index range")
    duplicate_index_test = res3_pre_pivot_agg_df[res3_pre_pivot_agg_df.duplicated(
        subset=INDEX_COLS + [DATE_COL, MEASURE])].copy()
    if not duplicate_index_test.empty:
        logging.warning("Danger, duplicate values across the index, this will led to unwanted aggreagation")
        sys.exit(-1)
    logging.info("Check[ed] for duplicates across index range")

    logging.info("Pivot[ing] dataframe")
    res3_pivot_df = res3_pre_pivot_agg_df.pivot_table(
        columns=MEASURE,
        values=VAL,
        index=INDEX_COLS + [DATE_COL]
    ).fillna(0)
    res3_pivot_df.sort_values(DATE_COL, inplace=True)
    logging.info("Pivot[ed] dataframe")

    logging.info("Add[ing] backlog calc")
    res3_pivot_df[BACKLOG] = res3_pivot_df[OPEN_COUNT] - res3_pivot_df[CLOSED_COUNT]
    logging.info("Add[ed] backlog calc")

    logging.info("Add[ing] backlog rolling sum calc")
    backlog_df = res3_pivot_df.groupby(INDEX_COLS).apply(
        lambda df: df.rolling(ROLLLING_WINDOW, min_periods=MIN_PERIODS, on=df.index.get_level_values(DATE_COL)).sum()
    ).reset_index()
    logging.info("Add[ed] backlog rolling sum calc")

    logging.info("Add[ing] service standard calc")
    backlog_df[SERVICE_STD] = (backlog_df[CLOSED_IN_TARGET] / backlog_df[CLOSED_COUNT] * 100)
    backlog_df[SERVICE_STD].replace(np.inf, 0, inplace=True)
    logging.info("Add[ed] service standard calc")

    logging.info("Filter[ing] metrics to latest data date")
    backlog_df[DATE_COL] = pd.to_datetime(backlog_df[DATE_COL])
    latest_date_dept = backlog_df.date.max() - timedelta(days=DELTA_DAYS)
    res3_backlog_df_filt = backlog_df.query(f"{DATE_COL} == @latest_date_dept").copy()
    logging.info("Filter[ed] metrics to latest data date")

    logging.info("Calculat[ing] total requests to date")
    res3_opened_total = res3_pivot_df.query(f"{DATE_COL} >= @START_DATE").groupby(
        INDEX_COLS).agg(
        total_opened=(OPEN_COUNT, "sum")
    ).reset_index()
    logging.info("Calculat[ed] total requests to date")

    logging.info("Merg[ing] metrics and total requests")
    res3_combined = pd.merge(
        res3_backlog_df_filt,
        res3_opened_total,
        on=INDEX_COLS,
        how="left",
        validate="1:1"
    )
    res3_combined.drop(columns=[DATE_COL], inplace=True)
    logging.info("Merg[ed] metrics and total requests")

    logging.info("Add[ing] color field")
    res3_combined["dept_color"] = res3_combined["department"].apply(lambda x: DEPARTMENTS_CLR_DICT[x])
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
