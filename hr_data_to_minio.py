import json
import logging
import os
import re
import sys
import tempfile
import urllib.parse

from db_utils import minio_utils
import pandas
import requests

import sharepoint_utils

SP_DOMAIN = 'http://ctapps.capetown.gov.za'
SP_SITE = '/sites/HRCovidCapacity/'
SP_EXCEL_LIST_NAME = 'EXCEL FORM DATA'
DATA_SHEET_NAMES = ['owssvr', 'DATASHEET']
SP_REGEX = r'^\d+;#(.+)$'
SOURCE_COL_NAME = "SourceUrl"
ACCESS_COL_NAME = "AccessTimestamp"

SP_XML_LIST_NAME = 'HR COVID Capacity Online Form'
XML_URL_COL_NAME = 'URL Path'
XML_ID_COL_NAME = 'Unique Id'
XML_DATE_COL_NAME = 'Date'
XML_FIELD_NAMES = [
    'Manager', 'Manager Staff No', 'Designation', 'Department', 'Evaluation',
    'Employee Name', 'Employee No', 'Categories', XML_DATE_COL_NAME, SOURCE_COL_NAME, ACCESS_COL_NAME
]
ISO8601_FORMAT = "%Y-%m-%d %H:%M:%S"

SP_BATCH_LIST_NAMES = ['SWM Batch Submissions', 'WS Batch Submissions']
BATCH_FILE_PATTERN_FILTER = {
    'SWM Batch Submissions': "new swm staff capacity",
    'WS Batch Submissions': "ws staffing capacity report"
}
BATCH_COLUMN_MAP = {
    'Date': 'Date',
    'Evaluation': 'Evaluation',
    'Staff Number': 'Employee No',
    'Status': 'Categories'
}

BUCKET = 'covid'
HR_BACKUP_PREFIX = "data/staging/hr_data_backup/"
FILENAME_PATH = "data/private/hr_data_complete"


def get_xml_list_dfs(site, list_name):
    access_timestamp = pandas.Timestamp.now(tz="Africa/Johannesburg")
    xml_list = site.List(list_name).GetListItems()
    xml_df = pandas.DataFrame(xml_list)

    if xml_df.shape[0] == 0:
        logging.warning(f"XML list is empty, returning None")
        return None
    else:
        logging.debug(f"Got {xml_df.shape[0]} XML entries")

    url_pattern = re.compile(SP_REGEX)
    logging.debug(f"Setting '{SOURCE_COL_NAME}'='URL Path', '{ACCESS_COL_NAME}'={access_timestamp}")

    xml_df[SOURCE_COL_NAME] = xml_df[XML_URL_COL_NAME].str.extract(
        url_pattern, expand=False
    ).apply(
        lambda file_uri: urllib.parse.urljoin(SP_DOMAIN, file_uri)
    )
    xml_df[ACCESS_COL_NAME] = access_timestamp

    # Backing up the XML rows into Minio
    with tempfile.TemporaryDirectory() as tempdir:
        for row in xml_df.itertuples(index=False):
            row_series = pandas.DataFrame({
                col: [val]
                for col, val in zip(xml_df.columns, row)
            }).iloc[0]

            id_search = url_pattern.search(row_series['Unique Id'])
            unique_id = id_search.group(1)
            logging.debug(f"Backing up '{unique_id}.json' to Minio...")
            local_path = os.path.join(tempdir, unique_id)
            with open(local_path, "w") as json_file:
                row_series.to_json(json_file)

            minio_utils.file_to_minio(
                filename=local_path,
                filename_prefix_override=HR_BACKUP_PREFIX,
                minio_bucket=BUCKET,
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
                data_classification=minio_utils.DataClassification.EDGE,
            )
    # Making the XML file more like the others
    xml_df[XML_DATE_COL_NAME] = xml_df[XML_DATE_COL_NAME].dt.strftime(ISO8601_FORMAT)

    return xml_df[XML_FIELD_NAMES]


def get_excel_list_dfs(site_list, auth, proxy_dict, minio_access, minio_secret):
    url_pattern = re.compile(SP_REGEX)
    http_session = requests.Session()
    http_session.proxies = proxy_dict
    http_session.auth = auth

    with tempfile.TemporaryDirectory() as tempdir:
        for file_dict in site_list:
            file_regex_result = url_pattern.search(file_dict["URL Path"])
            file_uri = file_regex_result.group(1)
            file_url = urllib.parse.urljoin(SP_DOMAIN, file_uri)
            logging.debug(f"Fetching '{file_url}'...")

            resp = http_session.get(file_url)
            assert resp.status_code == 200
            access_timestamp = pandas.Timestamp.now(tz="Africa/Johannesburg")

            local_filename = file_uri.replace("/", "_")
            local_path = os.path.join(tempdir, local_filename)
            with open(local_path, "wb") as name_temp_file:
                name_temp_file.write(resp.content)

            logging.debug("Backing up HR data file to Minio")
            minio_utils.file_to_minio(
                filename=local_path,
                filename_prefix_override=HR_BACKUP_PREFIX,
                minio_bucket=BUCKET,
                minio_key=minio_access,
                minio_secret=minio_secret,
                data_classification=minio_utils.DataClassification.EDGE,
            )

            is_excel_file = any(map(
                lambda ext: local_path.endswith(ext),
                ("xlsx", "xls", "XLSX", "XLS")
            ))
            if is_excel_file:
                logging.debug(f"Generating df from downloaded file")
                for data_sheet_name, raw_df in pandas.read_excel(local_path, sheet_name=None, dtype="object").items():
                    logging.debug(f"Reading sheet'{data_sheet_name}'")

                    logging.debug(f"Setting '{SOURCE_COL_NAME}'='{file_url}', '{ACCESS_COL_NAME}'={access_timestamp}")
                    raw_df[SOURCE_COL_NAME] = file_url
                    raw_df[ACCESS_COL_NAME] = access_timestamp

                    logging.debug(f"raw_df.head(10)=\n{raw_df.head(10)}")

                    yield raw_df
            else:
                logging.debug("Not an Excel file, continuing..")
                continue


def get_combined_list_df(site, auth, proxy_dict, minio_access, minio_secret):
    # Get XML files
    xml_list_df = get_xml_list_dfs(site, SP_XML_LIST_NAME)

    # setup file generator
    # Extract files
    # This approach has been deprecated - the entries should be in the cache.
    # site_list = site.List(SP_EXCEL_LIST_NAME).GetListItems()
    # logging.debug(f"Got '{len(site_list)}' item(s) from '{SP_EXCEL_LIST_NAME}'")
    # site_list_dfs = get_excel_list_dfs(site_list, auth, proxy_dict, minio_access, minio_secret)

    # Batch files
    batch_list_dfs = (
        batch_df[BATCH_COLUMN_MAP.keys()].rename(BATCH_COLUMN_MAP, axis='columns')
        for batch_list in SP_BATCH_LIST_NAMES
        for batch_df in get_excel_list_dfs(sharepoint_utils.filter_site_list(site.List(batch_list),
                                                                             BATCH_FILE_PATTERN_FILTER[batch_list]),
                                           auth, proxy_dict, minio_access, minio_secret)
    )

    # concat
    combined_df = pandas.concat([
        # df for df in [xml_list_df, *site_list_dfs, *batch_list_dfs]
        df for df in [xml_list_df, *batch_list_dfs]
        if df is not None
    ])

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

    logging.info("Setting up auth...")
    sp_auth, city_proxy_string, city_proxy_dict = sharepoint_utils.get_auth_objects(
        secrets["proxy"]["username"],
        secrets["proxy"]["password"]
    )
    sharepoint_utils.set_env_proxy(city_proxy_string)

    logging.info("Getting combined df...")
    sp_site = sharepoint_utils.get_sp_site(SP_DOMAIN, SP_SITE, sp_auth)
    combined_df = get_combined_list_df(sp_site, sp_auth, city_proxy_dict,
                                       secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])

    logging.info("Writing to Minio...")
    minio_utils.dataframe_to_minio(combined_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv",
                                   index=False)
    logging.info("...Done!")
