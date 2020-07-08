import json
import logging
import os
import sys
import tempfile

import pandas
from db_utils import minio_utils

import exchange_utils

SUBJECT_FILTER = 'OHS Data'
FILENAMES = "ohs_data.xlsx"
BUCKET = 'covid'
STAGING_PREFIX = "data/staging/ohs_backup/"
RESTRICTED_PREFIX = "data/private/"


def convert_xls_to_csv(xls_path, csv_path):
    temp_df = pandas.read_excel(xls_path)

    temp_df.columns = map(str.lower, temp_df.columns)
    temp_df.columns = temp_df.columns.str.replace(" ", "_")
    temp_df.loc[:, "cct_areas_visited"] = temp_df["cct_areas_visited"].replace('\n',' ', regex=True)

    temp_df.to_csv(csv_path, sep="~", index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("Set[ting] up auth")
    # Exchange auth
    account = exchange_utils.setup_exchange_account(secrets["proxy"]["username"],
                                                    secrets["proxy"]["password"])

    logging.info("Gett[ing] OHS Data from Email")
    # Turning down various exchange loggers - they're abit noisy
    exchangelib_loggers = [
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if name.startswith("exchangelib")
    ]
    for logger in exchangelib_loggers:
        logger.setLevel(logging.INFO)

    filtered_items = exchange_utils.filter_account(account, SUBJECT_FILTER)
    for attachment_path in exchange_utils.get_latest_attachment_file(filtered_items):
        logging.debug("Uploading '{}' to minio://covid/{}".format(attachment_path, RESTRICTED_PREFIX))

        # Just back up everything we get
        minio_utils.file_to_minio(
            filename=attachment_path,
            filename_prefix_override=STAGING_PREFIX,
            minio_bucket=BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=minio_utils.DataClassification.EDGE,
        )

        filename = os.path.basename(attachment_path)
        if attachment_path.endswith(".xlsx") and filename in FILENAMES:
            with tempfile.TemporaryDirectory() as tempdir:
                # New name, less shouty
                new_filename = filename.lower().replace(" ", "_").replace(".xlsx", ".csv")
                localpath = os.path.join(tempdir, new_filename)
                # Converting to CSV
                convert_xls_to_csv(attachment_path, localpath)

                # Writing the master data files to private data prefix
                minio_utils.file_to_minio(
                    filename=localpath,
                    filename_prefix_override=RESTRICTED_PREFIX,
                    minio_bucket=BUCKET,
                    minio_key=secrets["minio"]["edge"]["access"],
                    minio_secret=secrets["minio"]["edge"]["secret"],
                    data_classification=minio_utils.DataClassification.EDGE,
                )

    logging.info("G[ot] OHS Data from Emails")
