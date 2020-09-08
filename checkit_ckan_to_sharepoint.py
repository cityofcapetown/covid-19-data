import json
import logging
import os
import sys

from shareplum import site

import ckan_utils
import sharepoint_utils

CKAN_DATASET = "checkit-ctis-c3-submissions"
CKAN_FILENAME = 'checkit-c3-data-04-09-2020.xlsx'

SP_DOMAIN = 'http://teamsites.capetown.gov.za'
SP_SITE = 'sites/IDPOPMts'
CHECKIT_FOLDER = 'Data Science/CheckIT_C3_Submissions'


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("G[etting] data from CKAN...")
    http_session = ckan_utils.setup_http_session(secrets['proxy']['username'], secrets['proxy']['password'])
    for filename, file_data in ckan_utils.download_data_from_ckan(
            CKAN_DATASET, secrets["ocl-ckan"]["ckan-api-key"], http_session, CKAN_FILENAME
        ):
        logging.info("...G[ot] data from CKAN")

        logging.info("Setting up sharepoint auth...")
        sp_auth, city_proxy_string, city_proxy_dict = sharepoint_utils.get_auth_objects(
            secrets["proxy"]["username"],
            secrets["proxy"]["password"]
        )
        sharepoint_utils.set_env_proxy(city_proxy_string)

        logging.info("Wr[iting] data to sharepoint...")
        sp_site = sharepoint_utils.get_sp_site(SP_DOMAIN, SP_SITE, sp_auth, site.Version.v2016)
        sharepoint_utils.put_sp_file(sp_site, CHECKIT_FOLDER, CKAN_FILENAME, file_data)
        logging.info("Wr[ote] data to sharepoint...")
