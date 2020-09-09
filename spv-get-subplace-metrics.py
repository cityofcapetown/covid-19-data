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
GROUPBY_COL = "Date.of.Diagnosis"
SUBPLACE_COL = "Subplace.name"
MAINPLACE_COL = "Mainplace.Name"

        
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

    
def place_name_fixer(x, y):
    if y == "Cape Town" and not pd.isna(x):
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
    spv_latest.loc[:, GROUPBY_COL] = pd.to_datetime(spv_latest[GROUPBY_COL]).dt.date
    
    # Mainplace.name sometimes has "Cape Town" and the suburb in "subplace.name" - fixing that
    logging.info(f"Fixing suburb names")
    spv_latest.loc[:, "ct_suburb"] = spv_latest.apply(lambda df: place_name_fixer(df[SUBPLACE_COL], df[MAINPLACE_COL]), axis="columns")
    
    # get the aggregated counts
    logging.info(f"Aggregating counts by suburb names")
    suburb_cases = spv_latest.groupby([GROUPBY_COL])["ct_suburb"].value_counts().reset_index(name="count")
 
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
    