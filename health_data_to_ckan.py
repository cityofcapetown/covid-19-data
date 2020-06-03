import hashlib
import json
import mimetypes
import os
import logging
import sys
import requests
from requests.adapters import HTTPAdapter
import tempfile

from db_utils import minio_utils

COVID_BUCKET = "covid"
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE
RESTRICTED_DATA_PREFIX = "data/private"

RETRIES = 10

DATASET_MAP = (
    # ((CKAN dataset name, CKAN resource name), Minio filename)
    (("western-cape-case-data", "Western Cape Case Data"), "wc_all_cases.csv"),
    (("cape-town-case-data", "Cape Town Case Data"), "ct_all_cases.csv"),
    (("western-cape-model-data", "Western Cape Model Data"), "wc_model_data.csv")
)

CHECKSUM_FIELD = "checksum"

OCL_CKAN_DOMAIN = 'https://cct.opencitieslab.org'
RESOURCE_CREATE_PATH = 'api/action/resource_create'
RESOURCE_UPDATE_PATH = 'api/action/resource_update'
PACKAGE_LOOKUP_PATH = 'api/action/package_show'


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


def setup_http_session(proxy_username, proxy_password):
    http = requests.Session()

    # Setting proxy on session
    proxy_string = f"http://{proxy_username}:{proxy_password}@internet.capetown.gov.za:8080/"
    http.proxies = {
        "http": proxy_string,
        "https": proxy_string
    }

    return http


def _get_dataset_metadata(dataset_name, ckan_api_key, session):
    resp = session.get(
        f'{OCL_CKAN_DOMAIN}/{PACKAGE_LOOKUP_PATH}',
        params={"id": dataset_name},
        headers={"X-CKAN-API-Key": ckan_api_key},
    )

    if resp.status_code == 200:
        body = resp.json()['result']

        return body
    elif resp.status_code == 404:
        raise RuntimeError(f"'{dataset_name}' doesn't exist on {OCL_CKAN_DOMAIN}!")
    else:
        logging.warning(f"Got unexpected status code on {dataset_name} - {resp.status_code}")
        logging.debug(f"response text: {resp.text}")

        return None


def _form_dataset_resources_lookup(dataset_metadata):
    dataset_resource_lookup = {
        resource['name']: {
            "id": resource["id"],
            CHECKSUM_FIELD: resource.get(CHECKSUM_FIELD, None)
        }
        for resource in dataset_metadata['resources']
    }

    return dataset_resource_lookup


def _generate_checksum(datafile):
    data = datafile.read()
    datafile.seek(0)

    md5sum = hashlib.md5(data).hexdigest()

    return md5sum


def upload_data_to_ckan(filename, datafile, dataset_name, resource_name, ckan_api_key, session):
    # Getting the dataset's metatadata
    dataset_metadata = _get_dataset_metadata(dataset_name, ckan_api_key, session)

    if dataset_metadata is None:
        raise RuntimeError(f"I don't know what to do with '{dataset_name}'")

    # Getting the data's resource information
    dataset_resource_lookup = _form_dataset_resources_lookup(dataset_metadata)
    logging.debug(f"{dataset_name}'s resources: {', '.join(dataset_resource_lookup.keys())}")

    # Generating the checksum for the data in Minio
    checksum = _generate_checksum(data_file)
    logging.debug(f"Resource checksum: '{checksum}'")

    if resource_name not in dataset_resource_lookup:
        logging.debug(f"Resource '{resource_name}' doesn't exist in '{dataset_name}', creating it...")
        mimetype, _ = mimetypes.guess_type(filename)
        _, ext = os.path.splitext(filename)
        logging.debug(f"Guessing that '{resource_name}' is '{ext[1:]}' with '{mimetype}' mimetype")

        resource_call_path = f"{OCL_CKAN_DOMAIN}/{RESOURCE_CREATE_PATH}"
        data = {
            "package_id": dataset_name,
            "name": resource_name,
            "resource_type": "file",
            "format": ext[1:],
            "mimetype": mimetype,
            CHECKSUM_FIELD: checksum
        }
    elif checksum != dataset_resource_lookup[resource_name][CHECKSUM_FIELD]:
        logging.debug(f"Resource '{resource_name}' exists in '{dataset_name}', "
                      f"but the checksum is different, so updating it...")
        resource_id = dataset_resource_lookup[resource_name]['id']
        logging.debug(f"resource id: '{resource_id}'")

        resource_call_path = f"{OCL_CKAN_DOMAIN}/{RESOURCE_UPDATE_PATH}"
        data = {
            "id": resource_id,
            CHECKSUM_FIELD: checksum
        }
    else:
        resource_id = dataset_resource_lookup[resource_name]['id']
        logging.debug(f"Skipping call to CKAN, as there isn't new data (checksum is '{checksum}', "
                      f"id is '{resource_id}')")
        return True

    try:
        # Uploading the resource
        resp = session.post(
            resource_call_path,
            data=data,
            headers={"X-CKAN-API-Key": ckan_api_key},
            files={
                'upload': (filename, datafile)
            },
        )
        assert resp.status_code == 200
    except requests.exceptions.ProxyError as e:
        logging.error(f"Received proxy error when uploading data: '{e}'")
        logging.warning(f"Assuming this is a graceful shutdown!")

    return True


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

    http_session = setup_http_session(secrets['proxy']['username'], secrets['proxy']['password'])
    for dataset_name, resource_name, filename, data_file in data_generator:
        logging.info(f"Upload[ing] {resource_name} to {dataset_name}")
        result = upload_data_to_ckan(filename, data_file, dataset_name, resource_name,
                                     secrets["ocl-ckan"]["ckan-api-key"], http_session)
        assert result, f"Upload of {resource_name} to {dataset_name} failed!"
        logging.info(f"Upload[ed] {resource_name} to {dataset_name}")

    logging.info("...Done")
