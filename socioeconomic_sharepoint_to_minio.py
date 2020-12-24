import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
from shareplum import site

import sharepoint_utils

SP_DOMAIN = 'http://teamsites.capetown.gov.za'
SP_SITE = 'sites/IDPOPMts'

SE_DATA_FOLDER = 'Data Science/SocioEconomicData'
SE_PROJECTIONS_FILE = 'SEProjections_without graphs.xlsx'

COVID_DATA_FILE = "socioeconomic_data.xlsx"
COVID_BUCKET = "covid"
RESTRICTED_PREFIX = "data/private/"

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
    sp_site = sharepoint_utils.get_sp_site(SP_DOMAIN,
                                           SP_SITE,
                                           sp_auth, site.Version.v2016)
    sp_data = sharepoint_utils.get_sp_file(sp_site, SE_DATA_FOLDER, SE_PROJECTIONS_FILE)
    logging.info("G[ot] data from sharepoint...")

    logging.info("Wr[iting] data to Minio...")
    with tempfile.TemporaryDirectory() as tempdir, \
            open(os.path.join(tempdir, COVID_DATA_FILE), "wb") as temp_data_file:
        temp_data_file.write(sp_data)
        temp_data_file.flush()
        temp_data_file.seek(0)

        result = minio_utils.file_to_minio(
                filename=temp_data_file.name,
                filename_prefix_override=RESTRICTED_PREFIX,
                minio_bucket=COVID_BUCKET,
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
                data_classification=minio_utils.DataClassification.EDGE,
            )
        assert result

    logging.info("...Wr[ote] data to Minio")
