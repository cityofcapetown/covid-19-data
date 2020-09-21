import json
import logging
import os
import pathlib
import sys
import tempfile

from db_utils import minio_utils

import ckan_utils


__author__ = "Colin Anthony"


MINIO_BUCKET = "covid"
DATA_RESTRICTED_PREFIX = "data/private/"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

SUBURB_FILE = "ct-covid-cases-by-suburb.csv"
DATASET_NAME = "cape-town-covid-cases-by-suburb" 
RESOURCE_NAME = "Cape Town Covid Cases by Suburb"

CHECKSUM_FIELD = "checksum"

OCL_CKAN_DOMAIN = 'https://cct.opencitieslab.org'
RESOURCE_CREATE_PATH = 'api/action/resource_create'
RESOURCE_UPDATE_PATH = 'api/action/resource_update'
PACKAGE_LOOKUP_PATH = 'api/action/package_show'


def get_data_file(filename, minio_access, minio_secret):
    
    with tempfile.NamedTemporaryFile(mode="rb") as temp_data_file:
        logging.debug("Pulling data from Minio bucket...")
        minio_path = os.path.join(DATA_RESTRICTED_PREFIX, filename)

        result = minio_utils.minio_to_file(
            temp_data_file.name,
            MINIO_BUCKET,
            minio_access,
            minio_secret,
            MINIO_CLASSIFICATION,
            minio_filename_override=minio_path,
        )
        assert result

        yield temp_data_file


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        sys.exit(-1)

    logging.info("Setting secrets variables")
    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))
    if not pathlib.Path(secrets_path).glob("*.json"):
        logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
        sys.exit(-1)
    
    # import the collected spv data
    logging.info(f"Get the data from minio")
    covid_suburb_cases_file = get_data_file(
        SUBURB_FILE, 
        minio_access=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    
    # set up the http session
    http_session = ckan_utils.setup_http_session(
    secrets["proxy"]["username"],
    secrets["proxy"]["password"],
    )
    
    # push the data to ckan
    logging.info(f"Upload[ing] {SUBURB_FILE} to {DATASET_NAME}")
    for data_file in covid_suburb_cases_file:
        result = ckan_utils.upload_data_to_ckan(
            SUBURB_FILE,
            data_file, 
            DATASET_NAME, 
            RESOURCE_NAME, 
            secrets["ocl-ckan"]["ckan-api-key"],
            http_session
        )
    
        assert isinstance(result, ckan_utils.CkanUploadResult), f"Upload of {RESOURCE_NAME} to {DATASET_NAME} failed!"
    
    logging.info(f"Upload[ed] {RESOURCE_NAME} to {DATASET_NAME}")

    logging.info("...Done")
