import hashlib
import json
import logging
import os
import sys
import zipfile

from db_utils import minio_utils
import pandas

import exchange_utils

CITY_DOMAIN = "CAPETOWN"
CITY_PROXY = "internet.capetown.gov.za:8080"
PROXY_ENV_VARS = ["http_proxy", "https_proxy"]
PROXY_ENV_VARS = PROXY_ENV_VARS + list(map(lambda x: x.upper(), PROXY_ENV_VARS))

SENDER_FILTER = "VanRooyen"
DAYS_LOOKBACK = 7
FILES_PER_DAY = 7
DATA_FILE_LOOKUPS = {
    "Documents/ZEMP_Q0001_V7_BICS_COVID_AUTO_00000.htm": ("ANALYSIS_ia_pt_a", "zemp_q0001_v7_bics_covid_auto"),
    "Documents/ZEMP_Q0001_V7_BICS_COVID_AUTO1_00000.htm": ("ANALYSIS_ia_pt_a", "zemp_q0001_v7_bics_covid_auto"),
    'Documents/ZHR_TIME_ATTEND_EMP_3_AUTO_00000.htm': ("ANALYSIS_ia_pt_a", "zhr_time_attend_emp_3_auto"),
    "Documents/ZATT_2002_AUTO_00000.htm": ("ANALYSIS_ia_pt_a", "zatt_2002_auto"),
    "Documents/ZATT_2002_AUTO1_00000.htm": ("ANALYSIS_ia_pt_a", "zatt_2002_auto"),
    'Documents/ZEMP_Q0001_V7_BICS_DASH_AUTO_00000.htm': ("ANALYSIS_ia_pt_a", "zemp_q0001_v7_bics_dash_auto"),
    'Documents/ZEMP_Q0001_V7_BICS_DASH_AUTO1_00000.htm': ("ANALYSIS_ia_pt_a", "zemp_q0001_v7_bics_dash_auto"),
}

BUCKET = 'covid'
SAP_HR_BACKUP_PREFIX = "data/staging/hr_sap_data_backup/"
SAP_RAW_FILENAME_PREFIX = "data/private/hr_sap_data_"


def light_clean(data_df):
    hr_data_df = data_df.iloc[5:-1].copy()
    hr_data_df.columns = data_df.iloc[1]

    return hr_data_df


def get_attachment_file_df(data_file):
    """Extracts any data files in the config"""
    with zipfile.ZipFile(data_file, "r") as zip_data_file:
        zip_file_list = zip_data_file.namelist()

        data_file_path, data_div_id, data_file_prefix = None, None, None
        for file_path, (table_id, file_prefix) in DATA_FILE_LOOKUPS.items():
            if file_path in zip_file_list:
                logging.debug(f"'{file_path}' in list!")
                data_file_path = file_path
                data_div_id = table_id
                data_file_prefix = file_prefix
                logging.debug(
                    f"data_file_path='{data_file_path}', data_div_id='{data_div_id}', data_file_prefix='{file_prefix}'"
                )

        if data_file_path is None:
            logging.warning(
                f"I don't couldn't find a file I know about in the {data_file}'s list"
            )
            logging.debug(
                f"len(zip_file_list)={len(zip_file_list)}"
            )
            # logging.debug(
            #     f"zip_file_list=\n{pprint.pformat(zip_file_list)}"
            # )
            return data_file_path, None

        with zip_data_file.open(data_file_path) as html_data_file:
            try:
                zip_data_df = light_clean(
                    pandas.read_html(html_data_file, attrs={"id": data_div_id})[0]
                )
                logging.debug(f"zip_data_df.shape={zip_data_df.shape}")
            except Exception as e:
                logging.error(f"Parsing failed! {e.__class__}: '{e}'")
                zip_data_df = None

    return data_file_prefix, zip_data_df


def generate_raw_doc_dfs(items) -> dict:
    """Generates a dictionary of different file types -> dataframes of data"""
    email_data_dfs = {}

    most_recent_file_numbers = FILES_PER_DAY * DAYS_LOOKBACK
    logging.debug(f"Looking back at {most_recent_file_numbers} files")
    for data_file in exchange_utils.get_attachment_files(items, most_recent_count=most_recent_file_numbers):
        # backing up data file
        # Calculating checksum
        with open(data_file, "rb") as raw_data_file:
            file_data = raw_data_file.read()
        file_checksum = hashlib.md5(file_data).hexdigest()
        logging.debug(f"file_checksum={file_checksum}")

        minio_utils.file_to_minio(
            filename=data_file,
            filename_prefix_override=SAP_HR_BACKUP_PREFIX + file_checksum + "_",
            minio_bucket=BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=minio_utils.DataClassification.EDGE,
        )

        # if is a zip file, might be a file we're interested in
        if zipfile.is_zipfile(data_file):
            data_file_prefix, data_df = get_attachment_file_df(data_file)

            # If this is a datafile we don't know how to handle, we skip
            if data_file_prefix is None:
                logging.warning(f"No data extracted from {data_file}! Moving on...")
                continue

            dfs = email_data_dfs.get(data_file_prefix, [])
            email_data_dfs[data_file_prefix] = dfs + [data_df]

    data_dfs = {
        data_file_prefix: pandas.concat([df for df in data_dfs if df is not None])
        for data_file_prefix, data_dfs in email_data_dfs.items()
    }

    return data_dfs


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')
    exchange_utils.set_exchange_loglevel(logging.INFO)

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("Setting up auth...")
    exchange_account = exchange_utils.setup_exchange_account(
        secrets["proxy"]["username"],
        secrets["proxy"]["password"]
    )
    logging.info("Getting account filtered items")
    account_filtered_items = exchange_utils.filter_account(exchange_account, sender_filter=SENDER_FILTER)

    logging.info("Getting raw dfs...")
    raw_doc_dfs = generate_raw_doc_dfs(account_filtered_items)

    logging.info("Writing to Minio...")
    for raw_df_name, df in raw_doc_dfs.items():
        filename = f"{SAP_RAW_FILENAME_PREFIX}{raw_df_name}"
        minio_utils.dataframe_to_minio(df, BUCKET,
                                       secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                       minio_utils.DataClassification.EDGE,
                                       filename_prefix_override=filename,
                                       data_versioning=False,
                                       file_format="csv",
                                       index=False)
    logging.info("...Done!")
