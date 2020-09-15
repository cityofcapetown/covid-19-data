# base imports
import json
import logging
import os
import sys
import tempfile
# external imports
from db_utils import minio_utils
import geopandas as gpd
import pandas as pd


__author__ = "Colin Anthony"


# set the minio bucket
MINIO_BUCKET = "covid"
DATA_RESTRICTED_PREFIX = "data/private/"
DATA_PUBLIC_PREFIX = "data/public/"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

OFFICIAL_SUBURBS = "official_suburb_labels.geojson"
LATEST_COLLECTED_SPV = "ct_all_cases.csv"
OUTFILE_NAME = "ct-covid-cases-by-suburb"
DIAG_DATE = "Date.of.Diagnosis"
SUBPLACE_COL = "Subplace.name"
MAINPLACE_COL = "Mainplace.Name"
MP_CODE = "Mainplace.Code"
SP_CODE = "Subplace.Code"
CUST_AREA = "Place_Name"
CUST_AREA_CODE = "Place_code"
CUST_AREA_TYPE = "Place_type"

# Mainplaces to report as Subplace
PLACE_KEY = ["Cape Town"]
PLACE_CODE = [199041]


def minio_csv_to_df(minio_filename_override, minio_key, minio_secret):
    """Function to read in the collected covid daily linelist.csv file 

    Args:
        minio_filename_override (str): name of the minio override filepath 
        minio_bucket (str): minio bucket name
        minio_key (str): minio access key
        minio_secret (str): minio secret
        data_classification (str): minio class

    Returns:
        [Object]: Pandas DataFrame
    """
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        try:
            result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                            minio_filename_override=minio_filename_override,
                                            minio_bucket=MINIO_BUCKET,
                                            minio_key=minio_key,
                                            minio_secret=minio_secret,
                                            data_classification=MINIO_CLASSIFICATION,
                                            )
        except Exception as e:
            logging.debug(f"Could not get data from minio bucket for {minio_filename_override}\nReturning empty dataframe")
            sys.exit(-1)
        
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...") 
            df = pd.read_csv(temp_data_file, low_memory=False)
            return df

    
def place_name_fixer(x, y, keys):
    if y in keys and not pd.isna(x):
        return x
    else:
        return y
    
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        sys.exit(-1)

    logging.info("Setting secrets variables")
    secrets_path = os.environ["SECRETS_PATH"]
    if not os.path.exists(secrets_path):
        logging.error(f"Secrets file not found: {secrets_path} ")
        sys.exit(-1)
        
    secrets = json.load(open(secrets_path))
    
    # import the collected spv data
    logging.debug(f"Get the data from minio")
    spv_latest = minio_csv_to_df(
            minio_filename_override=f"{DATA_RESTRICTED_PREFIX}{LATEST_COLLECTED_SPV}",
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )

    # format the date
    spv_latest.loc[:, DIAG_DATE] = pd.to_datetime(spv_latest[DIAG_DATE]).dt.date
    
    # get list of all mainplace codes 
    mainplace_codes = list(spv_latest[MP_CODE].unique())
    
    # Mainplace.name sometimes of "Cape Town" covers a huge area, report as "subplace.name"
    logging.info(f"Fixing suburb names")
    spv_latest.loc[:, CUST_AREA] = spv_latest.apply(lambda df: place_name_fixer(df[SUBPLACE_COL], df[MAINPLACE_COL], PLACE_KEY), axis="columns")
    spv_latest.loc[:, CUST_AREA_CODE] = spv_latest.apply(lambda df: place_name_fixer(df[SP_CODE], df[MP_CODE], PLACE_CODE), axis="columns")
    
    
    # get the aggregated counts
    logging.info(f"Aggregating counts by suburb names")
#     suburb_cases = spv_latest.groupby([DIAG_DATE])[CUST_AREA].value_counts().reset_index(name="count")
    suburb_cases = spv_latest.groupby([DIAG_DATE, CUST_AREA_CODE, CUST_AREA])[CUST_AREA_CODE].agg(
        count = (CUST_AREA, "size"),
    ).reset_index()
    
    # annotate whether the area is a Mainplace or Subplace
    suburb_cases.loc[:, CUST_AREA_TYPE] = suburb_cases[CUST_AREA_CODE].apply(lambda x: "Mainplace" if x in mainplace_codes else "Subplace")
    # convert the float to int
    suburb_cases.loc[:, CUST_AREA_CODE] = suburb_cases[CUST_AREA_CODE].astype(int)
    # give the date col a better name
    suburb_cases.rename(columns={DIAG_DATE: "Diagnosis_Date"}, inplace=True)

    # send data to minio
    logging.info(f"Writing data to minio")
    result = minio_utils.dataframe_to_minio(
        suburb_cases,
        minio_bucket=MINIO_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
        data_classification=MINIO_CLASSIFICATION,
        filename_prefix_override=f"{DATA_RESTRICTED_PREFIX}{OUTFILE_NAME}",
        data_versioning=False,
        file_format="csv",
    )
    
    logging.info(f"Done")
    