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
import tempfile
# external imports
from db_utils import minio_utils
import pandas as pd
import numpy as np

# set bucket constants
SERVICE_FACTS_BUCKET = "service-standards-tool.sd-request-facts"
SERVICE_ATTRIBUTES = "service-standards-tool.sd-request-fact-attributes"
WIDGETS_RESTRICTED_PREFIX = "widgets/private/business_continuity_service_request_map/"
COVID_BUCKET = "covid"
PRIVATE_PREFIX = "data/private/"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
LAKE_CLASSIFICATION = minio_utils.DataClassification.LAKE

# outfiles
DEPT_SERVICE_METRICS = "business_continuity_service_delivery_department_metrics"

# settings
START_DATE = pd.to_datetime('2020-10-12')

SECRETS_PATH_VAR = "SECRETS_PATH"

DEPARTMENTS_CLR_DICT = {
    "Electricity": '#a6cee3',
    "Water and Sanitation": '#1f78b4',
    "Solid Waste Management": '#b2df8a',
    "Roads Infrastructure and Management": '#33a02c',
    "Public Housing": '#fb9a99',
    "Recreation and Parks": '#e31a1c'
}

def minio_json_to_dict(minio_filename_override, minio_bucket, minio_key, minio_secret, data_classification):
    """
    function to pull minio json file to python dict
    :param minio_filename_override: (str) minio override string (prefix and file name)
    :param minio_bucket: (str) minio bucket name
    :param minio_key: (str) the minio access key
    :param minio_secret: (str) the minio key secret
    :param data_classification: minio classification (edge | lake)
    :return: python dict
    """
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                           minio_filename_override=minio_filename_override,
                                           minio_bucket=minio_bucket,
                                           minio_key=minio_key,
                                           minio_secret=minio_secret,
                                           data_classification=data_classification,
                                           )
        if not result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
            df = json.load(temp_data_file)
            return df


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
        secrets_path = os.environ["SECRETS_PATH"]
        if not pathlib.Path(secrets_path).glob("*.json"):
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
            DEPARTMENTS_CLR_DICT[department] = "#bababa"

    logging.info("Filter[ing] to hex 3 resolution")
    service_facts_hex_3 = service_facts.query("resolution == 3").copy().assign(
        date=lambda df: pd.to_datetime(df.date, format="%Y-%m-%d"))
    logging.info("Filter[ed] to hex 3 resolution")

    logging.info("Merg[ing] to annotations")
    res3_pivot_df_expand = pd.merge(service_facts_hex_3, service_attribs, how="left", on="feature", validate="m:1")
    logging.info("Merg[ed] to annotations")

    logging.info("Generat[ing] request code annotation name")
    res3_pivot_df_expand.loc[:, "Code"] = res3_pivot_df_expand['Code'] + " (" + res3_pivot_df_expand[
        'CodeGroupID'] + "-" + res3_pivot_df_expand['CodeID'] + ")"
    logging.info("Generat[ed] request code annotation name")

    logging.info("Pivot[ing] dataframe")
    res3_pivot_df = res3_pivot_df_expand.groupby(
        ["date", "directorate", "department", "Code", "measure"], as_index=False).sum().pivot_table(
        columns="measure",
        values="value",
        index=["date", "directorate", "department", "Code"]
    )
    res3_pivot_df.sort_values("date", inplace=True)
    logging.info("Pivot[ed] dataframe")

    logging.info("Add[ing] backlog calc")
    res3_pivot_df["backlog"] = res3_pivot_df["opened_count"] - res3_pivot_df["closed_count"]
    logging.info("Add[ed] backlog calc")
    
    logging.info("Add[ing] backlog rolling sum calc")
    backlog_df = res3_pivot_df.fillna(0).groupby(["directorate", "department", "Code"]).apply(
        lambda df: df.rolling("180D", on=df.index.get_level_values('date')).sum()
    ).reset_index()
    logging.info("Add[ed] backlog rolling sum calc")
    
    logging.info("Add[ing] service standard calc")
    backlog_df["service_standard"] = (backlog_df["closed_within_target_sum"] / backlog_df["closed_count"] * 100)
    backlog_df["service_standard"].replace(np.inf, 0, inplace=True)
    logging.info("Add[ed] service standard calc")
    
    logging.info("Filter[ing] metrics to latest data date")
    backlog_df["date"] = pd.to_datetime(backlog_df["date"])
    latest_date_dept = backlog_df.date.max() - timedelta(days=2)
    stats_filt = backlog_df.query("date == @latest_date_dept").copy()
 
    logging.info("Filter[ed] metrics to latest data date")
    logging.info("Calculat[ing] total requests to date")
    opened_total = res3_pivot_df.query("date >= @START_DATE").fillna(0).groupby(["directorate", "department", "Code"]).agg(
        total_opened=("opened_count", "sum")
    ).reset_index()
    
    logging.info("Calculat[ed] total requests to date")
    logging.info("Merg[ing] metrics and total requests")
    combined = pd.merge(stats_filt, opened_total, on=["directorate", "department", "Code"], how="left", validate="1:1")
    logging.info("Merg[ed] metrics and total requests")
    logging.info("Add[ing] color field")
    combined["dept_color"] = combined["department"].apply(lambda x: DEPARTMENTS_CLR_DICT[x])
    logging.info("Add[ed] color field")

    # put the file in minio
    logging.info(f"Push[ing] collected department metrics data to minio")
    result = minio_utils.dataframe_to_minio(
        combined,
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
