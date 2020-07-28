# base imports
from datetime import datetime
import json
import logging
import os
import pathlib
import sys
import tempfile
# external imports
from db_utils import minio_utils
import pandas as pd


__author__ = "Colin Anthony"


BUCKET = "covid"
SPV_PREFIX = "data/staging/wcgh_backup/"
FILE_OVERRIDE = f"data/private/wc_spv_collected_latest"


def minio_txt_to_df(minio_filename_override, minio_bucket, minio_key, minio_secret, data_classification):
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        try:
            result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                               minio_filename_override=minio_filename_override,
                                               minio_bucket=minio_bucket,
                                               minio_key=minio_key,
                                               minio_secret=minio_secret,
                                               data_classification=data_classification,
                                               )
        except Exception as e:
            logging.debug(f"Could not get data from minio bucket for {minio_filename_override}\nReturning empty dataframe")
            cols = ["Export Date", "Date of Diagnosis", "Admission Date", "Date of ICU Admission", "Date of Death"]
            df = pd.DataFrame(columns=cols)
            return df
            
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...") 
            with open(temp_data_file.name,'rb') as source:
                df = pd.read_csv(source, sep="\t", engine='c', encoding='ISO-8859-1')
                return df


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
    secrets = json.load(open(secrets_path))
    if not pathlib.Path(secrets_path).glob("*.json"):
        logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
        sys.exit(-1)

    # collect the wc casees data from minio into a dataframe
    # get the filenames for required window period
    logging.info("get the spv filenames for required window period")
    
    # get the minio client
    logging.debug(f"getting the minio client")
    minio_client = minio_utils._get_minio_client(
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
        data_classification=minio_utils.DataClassification.EDGE,
        minio_override_url=None,
        minio_client_override=None,
    )
    
    # get the list of files in the spv staging bucket
    logging.debug(f"getting the list of files from {SPV_PREFIX}")
    bucket_file_list = minio_utils._list_bucket_objects(minio_client, BUCKET, SPV_PREFIX)
    spv_files = sorted([file for file in bucket_file_list if file.endswith("txt")])
    
    # initialize main df to collect daily dumps
    logging.debug(f"Initializing empty master dataframe")
    cols = ["Export Date", "Date of Diagnosis", "Admission Date", "Date of ICU Admission", "Date of Death"]
    master_df = pd.DataFrame(columns=cols)
    
    logging.debug(f"collect the linelist data from minio")
    for file in spv_files:
        logging.debug(f"Attempting to get spv file: {file}")
        wc_day_cases_df = minio_txt_to_df(
            minio_filename_override=f"{file}",
            minio_bucket="covid",
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=minio_utils.DataClassification.EDGE,
        )
        if wc_day_cases_df.empty:
            logging.error(f"No spv file found for: {file}\ncontinuing")
            continue
            
        logging.debug(f"merging {file} with master dataframe")
        frames = [master_df, wc_day_cases_df]
        master_df = pd.concat([frm[cols] for frm in frames])
        
    # set the date format
    master_df.loc[:, "Export Date"] = pd.to_datetime(master_df["Export Date"]).dt.date
    # sort by export date
    master_df.sort_values("Export Date", ascending=True, inplace=True)
    
    # rename the columns to same format as csv files in minio
    logging.debug(f"renaming columns in final collected spv dataframe")
    rename_cols = {col_name: col_name.replace(" ", ".") for col_name in list(master_df)}
    master_df.rename(columns=rename_cols, inplace=True)
        
    # put the file in minio
    logging.debug(f"Sending collected data to minio")
    result = minio_utils.dataframe_to_minio(
        master_df,
        minio_bucket="covid",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
        data_classification=minio_utils.DataClassification.EDGE,
        filename_prefix_override=FILE_OVERRIDE,
        data_versioning=False,
        file_format="csv")

    if not result:
        logging.debug(f"Sending collected data to minio failed")
        
    logging.info(f"Done")
