import json
import logging
import os
import re
import sys
import tempfile

import pandas
from db_utils import minio_utils

import exchange_utils
import hr_data_to_minio
import sharepoint_utils

SUBJECT_FILTER = 'CITY ORG UNIT MASTER DATA'
FILENAMES = {"CITY ORG UNIT MASTER DATA.xlsx"}
BUCKET = 'covid'
STAGING_PREFIX = "data/staging/"
RESTRICTED_PREFIX = "data/private/"

MASTER_SHEET_LIST_NAME = 'Master Sheets'
MASTER_STAFF_FILE_PATTERN = 'staff uploaded detail'

ALL_STAFF_FILENAME_PATH = "data/private/hr_data_all_staff"


def convert_xls_to_csv(xls_path, csv_path):
    temp_df = pandas.read_excel(xls_path)
    temp_df.to_csv(csv_path, index=False)


def get_most_recent_sharepoint_item(site, file_name_pattern):
    file_list_dicts = site.List(MASTER_SHEET_LIST_NAME).GetListItems()

    file_list = [
        file_dict for file_dict in file_list_dicts
        if file_name_pattern in file_dict["Name"].lower()
    ]
    logging.debug(f"Found {len(file_list)} items in '{MASTER_SHEET_LIST_NAME}' list")

    created_regex = re.compile(hr_data_to_minio.SP_REGEX)
    file_list.sort(
        key=lambda file_dict: created_regex.search(file_dict["Created"]).group(1)
    )
    most_recent = file_list[-1]

    return most_recent


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
    # Sharepoint auth
    sp_auth, city_proxy_string, city_proxy_dict = sharepoint_utils.get_auth_objects(
        secrets["proxy"]["username"],
        secrets["proxy"]["password"]
    )
    sharepoint_utils.set_env_proxy(city_proxy_string)
    logging.info("Set[up] auth")

    logging.info("Gett[ing] Master Data from Emails")
    exchange_utils.set_exchange_loglevel(logging.INFO)

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

    logging.info("G[ot] Master Data from Emails")

    logging.info("Gett[ing] Master Data from Sharepoint")
    sp_site = sharepoint_utils.get_sp_site(
        hr_data_to_minio.SP_DOMAIN,
        hr_data_to_minio.SP_SITE,
        sp_auth
    )
    for filename_path, file_pattern in ((ALL_STAFF_FILENAME_PATH, MASTER_STAFF_FILE_PATTERN),):
        logging.debug(f"Fetching {filename_path}")
        staff_dict = get_most_recent_sharepoint_item(sp_site, file_pattern)
        for essential_staff_df in hr_data_to_minio.get_excel_list_dfs([staff_dict],
                                                                      sp_auth, city_proxy_dict,
                                                                      secrets["minio"]["edge"]["access"],
                                                                      secrets["minio"]["edge"]["secret"]):
            minio_utils.dataframe_to_minio(essential_staff_df, BUCKET,
                                           secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                           minio_utils.DataClassification.EDGE,
                                           filename_prefix_override=filename_path,
                                           data_versioning=False,
                                           file_format="csv",
                                           index=False)
