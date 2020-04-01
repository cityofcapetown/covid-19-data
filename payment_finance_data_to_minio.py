import json
import logging
import os
import sys

from db_utils import minio_utils, odbc_utils


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


def obtain_diff_data():
    pass


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
    # ToDo pull file, read in diff df
    # ToDo combine with baseline df

    # Writing result out
    logging.info("Writing full DataFrame to Minio...")
    BUCKET = 'covid'
    PAYMENTS_FILENAME_PATH = "data/private/business_continuity_finance_payments"

    minio_utils.dataframe_to_minio(baseline_payment_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=PAYMENTS_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
    

