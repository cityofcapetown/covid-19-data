import json
import logging
import os
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
SP_LIST_NAME = 'EXCEL FORM DATA'
DATA_SHEET_NAME = 'DATASHEET'

SOURCE_COL_NAME = "SourceUrl"
ACCESS_COL_NAME = "AccessTimestamp"

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


def get_list_dfs(site, list_name, auth, proxy_dict):
    site_list = site.List(list_name).GetListItems()
    logging.debug(f"Got '{len(site_list)}' item(s) from '{list_name}'")

    with tempfile.TemporaryDirectory() as tempdir:
        for file_dict in site_list:
            file_uri = file_dict["URL Path"][3:]
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

            try:
                logging.debug(f"Generating df from downloaded file")
                raw_df = pandas.read_excel(local_path, sheet_name=DATA_SHEET_NAME)

                logging.debug(f"Setting '{SOURCE_COL_NAME}'='{file_url}', '{ACCESS_COL_NAME}'={access_timestamp}")
                raw_df[SOURCE_COL_NAME] = file_url
                raw_df[ACCESS_COL_NAME] = access_timestamp
            except Exception as e:
                logging.error(f"{e.__class__}:'{repr(e)}'")
                logging.warning("Moving on...")
                continue

            yield raw_df


def get_combined_list_df(site, list_name, auth, proxy_dict):
    # setup file generator
    site_list_dfs = get_list_dfs(site, list_name, auth, proxy_dict)

    # concat
    combined_df = pandas.concat(site_list_dfs)

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
    combined_df = get_combined_list_df(sp_site, SP_LIST_NAME, sp_auth, city_proxy_dict)

    logging.info("Writing to Minio...")
    minio_utils.dataframe_to_minio(combined_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv",
                                   index=False)

    logging.info("...Done!")
