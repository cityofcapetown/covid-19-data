import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import pandas

BUCKET = 'covid'
CLASSIFICATION = minio_utils.DataClassification.EDGE
PAYMENTS_BASELINE_FILENAME_PATH = "data/private/business_continuity_finance_payments_baseline.csv"
PAYMENTS_DIFF_FILENAME_PATH = "data/private/business_continuity_finance_payments_diff.csv"

PAYMENT_DATE_COLUMN = "Date"
PAYMENT_TIMESTAMP_COLUMN = "DateTimestamp"
PAYMENT_DEDUP_COLUMNS = [PAYMENT_DATE_COLUMN, "PaymentType"]
PAYMENT_DATA_COLUMNS = [PAYMENT_DATE_COLUMN, "PaymentType", "DailyTotal", "TimestampAccessed"]

PAYMENTS_FILENAME_PATH = "data/private/business_continuity_finance_payments"


def get_data_df(filename, minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_data_file:
        logging.debug("Pulling diff data from Minio bucket...")
        result = minio_utils.minio_to_file(
            temp_data_file.name,
            BUCKET,
            minio_access,
            minio_secret,
            CLASSIFICATION,
            minio_filename_override=filename
        )
        assert result

        logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
        data_df = pandas.read_csv(temp_data_file)

    data_df[PAYMENT_TIMESTAMP_COLUMN] = pandas.to_datetime(data_df.Date, format="%Y%m%d")
    data_df.set_index(PAYMENT_TIMESTAMP_COLUMN, inplace=True)

    return data_df


def get_data_dfs(minio_access, minio_secret):
    baseline_df = get_data_df(PAYMENTS_BASELINE_FILENAME_PATH, minio_access, minio_secret)
    diff_df = get_data_df(PAYMENTS_DIFF_FILENAME_PATH, minio_access, minio_secret)

    return baseline_df, diff_df


def combine_baseline_diff(baseline_df, diff_df):
    logging.debug(f"baseline_df.tail(10)=\n{baseline_df.tail(10)}")
    logging.debug(f"diff_df.tail(10)=\n{diff_df.tail(10)}")

    combined_df = diff_df.combine_first(baseline_df)
    combined_df = combined_df.drop_duplicates(subset=PAYMENT_DEDUP_COLUMNS)

    # Cleaning up...
    combined_df[PAYMENT_DATE_COLUMN] = combined_df[PAYMENT_DATE_COLUMN].astype(int)
    combined_df = combined_df.reset_index()[PAYMENT_DATA_COLUMNS]

    logging.debug(f"combined_df.tail(10)=\n{combined_df.tail(10)}")

    return combined_df


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

    logging.info("Fetch[ing] baseline and diff data")
    baseline_data_df, diff_data_df = get_data_dfs(secrets["minio"]["edge"]["access"],
                                                  secrets["minio"]["edge"]["secret"])
    logging.info("Fetch[ed] baseline and diff data")

    logging.info("Merg[ing] baseline and diff data")
    combined_data_df = combine_baseline_diff(baseline_data_df, diff_data_df)
    logging.info("Merg[ed] baseline and diff data")

    # Writing result out
    logging.info("Writing full DataFrame to Minio...")
    minio_utils.dataframe_to_minio(combined_data_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=PAYMENTS_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
    

