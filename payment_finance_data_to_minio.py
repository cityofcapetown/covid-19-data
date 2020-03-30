import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils, odbc_utils
import pandas


def obtain_baseline_data(r3_user, r3_password,
                         r3_hostname, r3_hostport, r3_dbname):
    BASELINE_SQL_PATH = "resources/city-payments-daily.sql"

    current_path = os.path.dirname(
        os.path.abspath(__file__)
    )
    payment_query_path = os.path.join(current_path, BASELINE_SQL_PATH)

    with open(payment_query_path, "r") as payment_query_file:
        payment_query = payment_query_file.read()

    baseline_df = odbc_utils.oracle_to_dataframe(
        payment_query,
        r3_user, r3_password,
        r3_hostname, r3_hostport, r3_dbname
    )

    return baseline_df


def obtain_diff_data(minio_access, minio_secret, minio_classification):
    PAYMENT_DATA_FILE = ""
    PAYMENT_DATA_BUCKET = ""
    PAYMENT_DATA_DELIMETER = "~|~"
    PAYMENT_DATA_COLUMNS = ("Date", "PaymentType", "DailyTotal")

    with tempfile.NamedTemporaryFile() as temp_data_file:
        logging.debug("Pulling diff data from Minio bucket...")
        result = minio_utils.minio_to_file(
            temp_data_file.name,
            PAYMENT_DATA_BUCKET,
            minio_access,
            minio_secret,
            minio_classification,
            minio_filename_override=PAYMENT_DATA_FILE
        )
        assert result

        logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
        raw_diff_data = temp_data_file.read()

        logging.debug("Creating Diff DF")
        diff_data_df = pandas.DataFrame((
            map(lambda x: x.strip() if len(x.strip()) > 0 else None,
                diff_data_raw_line.split(PAYMENT_DATA_DELIMETER))
            for diff_data_raw_line in raw_diff_data.split("\n")
            if PAYMENT_DATA_DELIMETER in diff_data_raw_line
        ), columns=PAYMENT_DATA_COLUMNS)

        return diff_data_df


def combine_baseline_diff(baseline_df, diff_df):
    return baseline_df


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

    # Getting baseline data (i.e. everything in QA)
    SAP_R3_BASELINE_SERVER = 'r3q1db01.capetown.gov.za'
    SAP_R3_BASELINE_PORT = '1527'
    SAP_R3_BASELINE_DB_NAME = 'RQ1'

    logging.info("Fetch[ing] baseline data from R3 QA DB")
    baseline_payment_df = obtain_baseline_data(
        secrets["sap-r3"]["user"],
        secrets["sap-r3"]["password"],
        SAP_R3_BASELINE_SERVER, SAP_R3_BASELINE_PORT, SAP_R3_BASELINE_DB_NAME
    )
    logging.info("Fetch[ed] baseline data from R3 QA DB")

    # Topping up with Prod data
    diff_payment_df = obtain_diff_data(secrets["minio"]["edge"]["access"],
                               secrets["minio"]["edge"]["secret"],
                               minio_utils.DataClassification.EDGE)
    combined_df = combine_baseline_diff(baseline_payment_df, diff_payment_df)

    # Writing result out
    logging.info("Writing full DataFrame to Minio...")
    BUCKET = 'covid'
    PAYMENTS_FILENAME_PATH = "data/private/business_continuity_finance_payments"

    minio_utils.dataframe_to_minio(combined_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=PAYMENTS_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
    

