import json
import os
import logging
import sys
import tempfile

from db_utils import minio_utils

import ckan_utils

COVID_BUCKET = "covid"
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE
RESTRICTED_DATA_PREFIX = "data/private"

RETRIES = 10

DATASET_MAP = (
    # ((CKAN dataset name, CKAN resource name), Minio filename)
    (("western-cape-case-data", "Western Cape Case Data"), "wc_all_cases.csv"),
    (("cape-town-case-data", "Cape Town Case Data"), "ct_all_cases.csv"),
    (("western-cape-model-data", "Western Cape Model Data"), "wc_model_data.csv"),
    (("cape-town-model-data", "Cape Town Model Data"), "ct_model_data.csv"),
)


def get_data_files(filenames, minio_access, minio_secret):
    for filename in filenames:
        with tempfile.NamedTemporaryFile(mode="rb") as temp_data_file:
            logging.debug("Pulling data from Minio bucket...")
            minio_path = os.path.join(RESTRICTED_DATA_PREFIX, filename)

            result = minio_utils.minio_to_file(
                temp_data_file.name,
                COVID_BUCKET,
                minio_access,
                minio_secret,
                BUCKET_CLASSIFICATION,
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
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    # Generator for flattening everything into a file
    logging.info("Sett[ing up] data generator")
    data_filenames = [filename for _, filename in DATASET_MAP]
    data_files = get_data_files(data_filenames,
                                secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    data_generator = (
        (dataset_name, resource_name, filename, data_file)
        for ((dataset_name, resource_name), filename), data_file in zip(DATASET_MAP, data_files)
    )
    logging.info("Set[up] data generator")

    http_session = ckan_utils.setup_http_session(secrets['proxy']['username'], secrets['proxy']['password'])
    for dataset_name, resource_name, filename, data_file in data_generator:
        logging.info(f"Upload[ing] {resource_name} to {dataset_name}")
        result = ckan_utils.upload_data_to_ckan(filename, data_file, dataset_name, resource_name,
                                                secrets["ocl-ckan"]["ckan-api-key"], http_session)
        assert isinstance(result, ckan_utils.CkanUploadResult), f"Upload of {resource_name} to {dataset_name} failed!"
        logging.info(f"Upload[ed] {resource_name} to {dataset_name}")

    logging.info("...Done")
