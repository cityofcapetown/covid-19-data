import json
import logging
import os
import sys
import tempfile
import time

from db_utils import minio_utils
import pandas


def get_raw_finance_file(minio_key, minio_secret, minio_filename, minio_bucket):
    with tempfile.NamedTemporaryFile(suffix=".xlsx") as temp_raw_file:
        minio_utils.minio_to_file(
            filename=temp_raw_file.name,
            minio_filename_override=minio_filename,
            minio_bucket=minio_bucket,
            minio_key=minio_key,
            minio_secret=minio_secret,
            data_classification=minio_utils.DataClassification.EDGE,
        )
        
        yield temp_raw_file
     
    
def get_cleaned_df(datafile):
    raw_df = pandas.read_excel(datafile.name, engine='openpyxl')
    
    # Setting columns
    raw_df.columns = raw_df.iloc[0]
    raw_df.drop(0, inplace=True)

    # Don't ask
    raw_df.Date = raw_df.Date.str.replace('2047', '2017')
    
    # Creating a proper timestamp column
    raw_df["DateTimestamp"] = pandas.to_datetime(raw_df.Date, format="%d.%m.%y", errors='coerce')
    # Date format changed in 2015 - really wanted to get ahead of that 2099 rollover
    raw_df.DateTimestamp = raw_df.DateTimestamp.combine_first(
        pandas.to_datetime(raw_df.Date, format="%d.%m.%Y", errors='coerce')
    )
    
    # Dropping all observations without a date
    raw_df.drop(
        raw_df[raw_df.DateTimestamp.isna()].index, 
        inplace=True
    )
    # Dropping any left over blank lines and columns
    raw_df.dropna(how="all", inplace=True)
    raw_df.dropna(how="all", axis='columns', inplace=True)
    
    # Filling any nas
    raw_df.fillna(0, inplace=True)
    
    return raw_df
  
    
def remap_and_filter_df(raw_df):
    rename_map = {
        'Daily Cash    Takings': "Cash",
        'Bank ': "Bank",
         'ACB          (Debit Orders)': "DebitOrders",
        'E Portal  Daily Run  ': 'EPortal',
        'Unidentified Cash': "UnidentifiedCash",
        'Group   Accounts': "GroupAccounts"
    }
    
    filter_df = raw_df[
        ["DateTimestamp", *list(rename_map.keys())]
    ].copy().rename(
        rename_map, axis='columns'
    ).set_index('DateTimestamp').fillna(0)
    
    return filter_df


def get_csv_file(filter_df, csv_filename):
    with tempfile.TemporaryDirectory() as tempdir:
        temp_path = os.path.join(tempdir, csv_filename)
        filter_df.to_csv(temp_path)
        yield temp_path
    
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')
    
    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))
    
    BUCKET = 'covid'
    RESTRICTED_PREFIX = "data/private/"
    CURRENT_FILENAME = "total_isu_current.xls"
    MINIO_FILENAME = "{}{}".format(RESTRICTED_PREFIX, CURRENT_FILENAME)
    for raw_datafile in get_raw_finance_file(secrets["minio"]["edge"]["access"],
                                             secrets["minio"]["edge"]["secret"],
                                             MINIO_FILENAME, BUCKET):
    
        logging.info("Cleaning...")
        cleaned_df = get_cleaned_df(raw_datafile)

        logging.info("Filtering and renaming...")
        filtered_df = remap_and_filter_df(cleaned_df)

        logging.info("Writing out to CSV...")
        CSV_FILENAME = "income_totals.csv"
        for output_csvfile in get_csv_file(filtered_df, CSV_FILENAME):
            logging.debug("Uploading '{}' to minio://covid/{}".format(output_csvfile, RESTRICTED_PREFIX))

            logging.info("Writing to Minio...")
            minio_utils.file_to_minio(
                filename=output_csvfile,
                filename_prefix_override=RESTRICTED_PREFIX,
                minio_bucket=BUCKET,
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
                data_classification=minio_utils.DataClassification.EDGE,
            )

            logging.info("... Done!")
    