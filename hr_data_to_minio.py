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
from requests_ntlm import HttpNtlmAuth
from shareplum import Site


CITY_DOMAIN = "CAPETOWN"
CITY_PROXY = "internet.capetown.gov.za:8080"
PROXY_ENV_VARS = ["http_proxy", "https_proxy"]
PROXY_ENV_VARS = PROXY_ENV_VARS + list(map(lambda x: x.upper(), PROXY_ENV_VARS))

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

BUCKET = 'covid'
HR_BACKUP_PREFIX = "data/staging/hr_data_backup/"
FILENAME_PATH = "data/private/hr_data_complete"


def get_auth_objects(username, password):
    auth = HttpNtlmAuth(f'{CITY_DOMAIN}\\{username}', password)
    proxy_string = f'http://{secrets["proxy"]["username"]}:{secrets["proxy"]["password"]}@{CITY_PROXY}'
    proxy_dict = {
        "http": proxy_string,
        "https": proxy_string
    }

    return auth, proxy_string, proxy_dict


def set_env_proxy(proxy_string):
    for proxy_env_var in PROXY_ENV_VARS:
        logging.debug(f"Setting '{proxy_env_var}'")
        os.environ[proxy_env_var] = proxy_string


def get_sp_site(sp_domain, sp_site, auth):
    site_string = urllib.parse.urljoin(sp_domain, sp_site)
    site = Site(site_string, auth=auth)

    return site


def get_xml_list_dfs(site, list_name):
    access_timestamp = pandas.Timestamp.now(tz="Africa/Johannesburg")
    xml_list = site.List(list_name).GetListItems()
    xml_df = pandas.DataFrame(xml_list)
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


def get_excel_list_dfs(site, list_name, auth, proxy_dict):
    site_list = site.List(list_name).GetListItems()
    logging.debug(f"Got '{len(site_list)}' item(s) from '{list_name}'")
    url_pattern = re.compile(SP_REGEX)

    with tempfile.TemporaryDirectory() as tempdir:
        for file_dict in site_list:
            file_regex_result = url_pattern.search(file_dict["URL Path"])
            file_uri = file_regex_result.group(1)
            file_url = urllib.parse.urljoin(SP_DOMAIN, file_uri)
            logging.debug(f"Fetching '{file_url}'...")

            resp = requests.get(file_url, auth=auth, proxies=proxy_dict)
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
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
                data_classification=minio_utils.DataClassification.EDGE,
            )

            logging.debug(f"Generating df from downloaded file")
            for data_sheet_name, raw_df in pandas.read_excel(local_path, sheet_name=None).items():
                logging.debug(f"Reading sheet'{data_sheet_name}'")

                logging.debug(f"Setting '{SOURCE_COL_NAME}'='{file_url}', '{ACCESS_COL_NAME}'={access_timestamp}")
                raw_df[SOURCE_COL_NAME] = file_url
                raw_df[ACCESS_COL_NAME] = access_timestamp

                yield raw_df


def get_combined_list_df(site, auth, proxy_dict):
    # Get XML files
    xml_list_df = get_xml_list_dfs(site, SP_XML_LIST_NAME)

    # setup file generator
    site_list_dfs = get_excel_list_dfs(site, SP_EXCEL_LIST_NAME, auth, proxy_dict)

    # concat
    combined_df = pandas.concat([xml_list_df, *site_list_dfs])

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
    sp_auth, city_proxy_string, city_proxy_dict = get_auth_objects(
        secrets["proxy"]["username"],
        secrets["proxy"]["password"]
    )
    set_env_proxy(city_proxy_string)

    logging.info("Getting combined df...")
    sp_site = get_sp_site(SP_DOMAIN, SP_SITE, sp_auth)
    combined_df = get_combined_list_df(sp_site, sp_auth, city_proxy_dict)

    logging.info("Writing to Minio...")
    minio_utils.dataframe_to_minio(combined_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv",
                                   index=False)
    logging.info("...Done!")
