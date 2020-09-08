import json
import logging
import os
import sys
import tempfile

from shareplum import site

import checkit_ckan_to_sharepoint
import ckan_utils
import sharepoint_utils

CHECKIT_OUTPUT_FILE = 'c3-checkit-data-07-09-2020.xlsx'


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

    logging.info("Setting up sharepoint auth...")
    sp_auth, city_proxy_string, city_proxy_dict = sharepoint_utils.get_auth_objects(
        secrets["proxy"]["username"],
        secrets["proxy"]["password"]
    )
    sharepoint_utils.set_env_proxy(city_proxy_string)

    logging.info("G[etting] data from sharepoint...")
    sp_site = sharepoint_utils.get_sp_site(checkit_ckan_to_sharepoint.SP_DOMAIN,
                                           checkit_ckan_to_sharepoint.SP_SITE,
                                           sp_auth, site.Version.v2016)
    sp_data = sharepoint_utils.get_sp_file(sp_site, checkit_ckan_to_sharepoint.CHECKIT_FOLDER, CHECKIT_OUTPUT_FILE)
    logging.info("G[ot] data from sharepoint...")

    logging.info("Wr[iting] data to CKAN...")
    with tempfile.NamedTemporaryFile("r+b") as temp_data_file:
        temp_data_file.write(sp_data)
        temp_data_file.flush()
        temp_data_file.seek(0)

        http_session = ckan_utils.setup_http_session(secrets['proxy']['username'], secrets['proxy']['password'])
        ckan_utils.upload_data_to_ckan(
            temp_data_file.name, temp_data_file,
            checkit_ckan_to_sharepoint.CKAN_DATASET, CHECKIT_OUTPUT_FILE,
            secrets["ocl-ckan"]["ckan-api-key"], http_session
        )
    logging.info("...Wr[ote] data to CKAN")
