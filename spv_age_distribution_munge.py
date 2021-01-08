"""
This script takes the spv linelist and outputs lag adjusted values for covid cases and deaths by age bin
for the CT metro
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
# local imports
from spv_metro_subdistricts_munge import minio_csv_to_df, write_to_minio


# data settings
COVID_BUCKET = "covid"
RESTRICTED_PREFIX = "data/private/"
WC_CASES = "wc_all_cases.csv"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE

CASES_ADJUSTED_MASTER = "spv_cases_age_distribution"
DEATHS_ADJUSTED_MASTER = "spv_deaths_age_distribution"

CT_CITY_METRO = 'City of Cape Town'
EXPORT = "Export.Date"
DIAGNOSIS = "Date.of.Diagnosis"
DEATH = "Date.of.Death"
DATE = "Date"
DISTRICT = "District"
SUBDISTRICT = "Subdistrict"
CT_METRO = "CT_Metro"
AGE_BAND = "Age_Band"
AGE_GRP = "Agegroup"
COUNT = "count"
age_dict = {
    "Not recorded": "Unknown",
    '-460 - -455': "Unknown",
    '-160 - -155': "Unknown",
    '265 - 270': "Unknown",
    '0 - 5': "000 - 010",
    '5 - 10': "000 - 010",
    '10 - 15': "010 - 020",
    '15 - 20': "010 - 020",
    '20 - 25': "020 - 030",
    '25 - 30': "020 - 030",
    '30 - 35': "030 - 040",
    '35 - 40': "030 - 040",
    '40 - 45': "040 - 050",
    '45 - 50': "040 - 050",
    '50 - 55': "050 - 060",
    '55 - 60': "050 - 060",
    '60 - 65': "060 - 070",
    '65 - 70': "060 - 070",
    '70 - 75': "070 - 080",
    '75 - 80': "070 - 080",
    '80 - 85': "080 - 125",
    '90 - 95': "080 - 125",
    '85 - 90': "080 - 125",
    '95 - 100': "080 - 125",
    '100 - 105': "080 - 125",
    '105 - 110': "080 - 125",
    '120 - 125': "080 - 125",
}
COMMON_GROUP_COLS = [EXPORT, AGE_BAND, DISTRICT]

SECRETS_PATH_VAR = "SECRETS_PATH"


def agg_by_age(data_frame: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """
    function to carry out aggregation of df on list of group columns
    Args:
        data_frame (pd.DataFrame): pandas dataframe
        group_cols (list): list of columns to use as the groupby columns

    Returns:
        df_agg (pd.DataFrame): panas dataframe with the aggregations
    """
    if EXPORT not in group_cols:
        logging.error(f"{EXPORT} must be in group columns list")
        sys.exit(-1)
    df_agg = data_frame.groupby(group_cols).agg(
        count=(EXPORT, COUNT)
    ).reset_index()

    return df_agg


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    logging.info(f"Fetch[ing] secrets")
    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        logging.info("Trying to load from file")
        secrets_file = pathlib.Path("/home/jovyan/secrets/secrets.json").resolve()
        if not secrets_file.exists():
            print("could not find your secrets")
            sys.exit(-1)
        else:
            secrets = json.load(open(secrets_file))
            logging.info("Loaded secrets from file")
    else:
        logging.info("Setting secrets variables")
        secrets_file = os.environ[SECRETS_PATH_VAR]
        if not pathlib.Path(secrets_file).exists():
            logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
            sys.exit(-1)
        secrets = json.load(open(secrets_file))
    logging.info(f"Fetch[ed] secrets")

    logging.info(f"Fetch[ing] data from minio")
    spv_latest = minio_csv_to_df(
        minio_filename_override=f"{RESTRICTED_PREFIX}{WC_CASES}",
        minio_bucket=COVID_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    logging.info(f"Fetch[ed] data from minio")

    spv_latest.loc[:, EXPORT] = pd.to_datetime(spv_latest[EXPORT], format='%b  %d %Y %I:%M%p').dt.date
    spv_latest[AGE_BAND] = spv_latest[AGE_GRP].apply(
        lambda val: age_dict[val] if val in age_dict.keys() else age_dict.get(val, "Unknown"))

    for kind, outfile in [
        (DIAGNOSIS, CASES_ADJUSTED_MASTER),
        (DEATH, DEATHS_ADJUSTED_MASTER)
    ]:
        # down select to relevant columns
        df = spv_latest[[EXPORT, kind, DISTRICT, SUBDISTRICT, AGE_BAND]].copy()

        # districts
        logging.info("Calculat[ing] district aggregations")
        district_group_columns = COMMON_GROUP_COLS + [kind]
        district_agg = agg_by_age(df, district_group_columns)
        district_agg[SUBDISTRICT] = np.nan
        logging.info("Calculat[ed] district aggregations")

        # subdistricts
        logging.info("Calculat[ing] metro aggregations")
        subdistrict_group_columns = COMMON_GROUP_COLS + [kind, SUBDISTRICT]
        subdistrict_agg = agg_by_age(df, subdistrict_group_columns)
        logging.info("Calculat[ed] metro aggregations")

        logging.info("Combin[ing] district and subdistrict data")
        combined_df = pd.concat([district_agg, subdistrict_agg])
        logging.info("Combin[ed] district and subdistrict data")

        logging.info("Reorder[ing] columns")
        combined_df = combined_df[[EXPORT, kind, AGE_BAND, DISTRICT, SUBDISTRICT, COUNT]]
        logging.info("Reorder[ed] columns")

        # write to minio
        logging.info("Push[ing] data to minio")
        result = write_to_minio(
            COVID_BUCKET, RESTRICTED_PREFIX,
            secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
            combined_df, outfile
        )
        logging.info("Push[ed] data to minio")

    logging.info(f"Done")
