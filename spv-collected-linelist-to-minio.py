"""
Script to collect the daily spv dumps
"""

__author__ = "Colin Anthony"

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
import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError
import pyarrow as pa
import pyarrow.parquet as pq


BUCKET = "covid"
SPV_PREFIX = "data/staging/wcgh_backup/"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE

COLS = ["Export Date", "Date of Diagnosis", "Admission Date", "Date of ICU Admission", "Date of Death", "District", "Subdistrict"]
CHECK_COLS = ["hex_l7", "hex_l8"]
WC_FILE_OVERRIDE = f"data/private/spv_collected_latest"
USE_LAST_X_DAYS = 120


def minio_txt_to_df(minio_filename_override, minio_bucket, minio_key, minio_secret, data_classification):
    logging.info("Pulling data from Minio bucket...")

    df = pd.DataFrame(columns=COLS)
    with tempfile.NamedTemporaryFile() as temp_data_file:
        try:
            result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                               minio_filename_override=minio_filename_override,
                                               minio_bucket=minio_bucket,
                                               minio_key=minio_key,
                                               minio_secret=minio_secret,
                                               data_classification=data_classification,
                                               )
            logging.info(f"Reading in raw data from '{temp_data_file.name}'...")
            with open(temp_data_file.name, 'rb') as source:
                df = pd.read_csv(source, sep="\t", engine='c', encoding='ISO-8859-1', usecols=COLS)

        except EmptyDataError as e:
            logging.warning(f"Could not get data from minio bucket for {minio_filename_override}\nReturning empty dataframe")
            yield None
        else:
            yield df


def csv_to_parquet(dfs: iter) -> None:
    """
    adapted from https://stackoverflow.com/questions/44715393/how-to-concatenate-multiple-pandas-dataframes-without-running-into-memoryerror
    """
    start_time = datetime.now()
    pqwriter = None
    with tempfile.NamedTemporaryFile() as temp_data_file:
        for i, df_gen in enumerate(dfs):
            df = next(df_gen)
            if not isinstance(df, pd.DataFrame):
                logging.warning("empty df, skipping")
                continue

            table = pa.Table.from_pandas(df)
            # for the first chunk of records
            if i == 0:
                # create a parquet write object giving it an output file
                pqwriter = pq.ParquetWriter(temp_data_file, table.schema)
            pqwriter.write_table(table)
        if pqwriter:
            pqwriter.close()
        combinded_df = pd.read_parquet(temp_data_file)

    end_time = datetime.now()
    total = end_time - start_time
    logging.info(f'took {total} to concat dfs')
    
    return combinded_df


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
    # get the list of files in the spv staging bucket
    logging.info(f"getting the list of files from {SPV_PREFIX}")
    spv_files = sorted(list(minio_utils.list_objects_in_bucket(
        BUCKET, 
        minio_key=secrets["minio"]["edge"]["access"], 
        minio_secret=secrets["minio"]["edge"]["secret"], 
        data_classification=EDGE_CLASSIFICATION,
        filter_pattern_regex=f"{SPV_PREFIX}covid_sum.*.txt"
    )))
    
    # initialize main df to collect daily dumps
    logging.info(f"Initializing empty master dataframe")
    master_df = pd.DataFrame(columns=COLS)
    
    logging.info(f"collect the linelist data from minio")
    all_dfs_gen = []
    for i, file in enumerate(spv_files[-1 * USE_LAST_X_DAYS:]):
        logging.info(f"Attempting to get spv file: {file}")
        wc_day_cases_df = minio_txt_to_df(
            minio_filename_override=f"{file}",
            minio_bucket="covid",
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=minio_utils.DataClassification.EDGE,
        )

        all_dfs_gen.append(wc_day_cases_df)

    logging.info(f"Combin[ing] collected dfs")    
    comb_df = csv_to_parquet(all_dfs_gen)      
    if comb_df.empty:
        logging.error("failed to generate combined dataframe")
        sys.exit(-1)
    logging.info(f"Combin[ed] collected dfs")    
    
    # set the date format
    comb_df.loc[:, "Export Date"] = pd.to_datetime(comb_df["Export Date"]).dt.date
    # sort by export date
    comb_df.sort_values("Export Date", ascending=True, inplace=True)

    # rename the columns to same format as csv files in minio
    logging.info(f"renaming columns in final collected spv dataframe")
    rename_cols = {col_name: col_name.replace(" ", ".") for col_name in list(comb_df.columns)}
    comb_df.rename(columns=rename_cols, inplace=True)
        
    # put the file in minio
    logging.info(f"Push[ing] collected WC data to minio")
    result = minio_utils.dataframe_to_minio(
        comb_df,
        minio_bucket="covid",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
        data_classification=minio_utils.DataClassification.EDGE,
        filename_prefix_override=WC_FILE_OVERRIDE,
        data_versioning=False,
        file_format="csv")

    if not result:
        logging.info(f"Push[ing] collected WC data to minio failed")
    logging.info(f"Push[ed] collected WC data to minio")
    logging.info(f"Done")
