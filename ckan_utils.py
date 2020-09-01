import hashlib
import mimetypes
import os
import logging
import requests

CITY_PROXY_HOST = "internet.capetown.gov.za:8080"

CHECKSUM_FIELD = "checksum"

OCL_CKAN_DOMAIN = 'https://cct.opencitieslab.org'
RESOURCE_CREATE_PATH = 'api/action/resource_create'
RESOURCE_UPDATE_PATH = 'api/action/resource_update'
PACKAGE_LOOKUP_PATH = 'api/action/package_show'


def setup_http_session(proxy_username, proxy_password) -> requests.Session:
    """Sets up a HTTP session with the appropriate City proxy set

    :param proxy_username: City LDAP user for proxy
    :param proxy_password: City LDAP user for password
    :return:
    """
    http = requests.Session()

    # Setting proxy on session
    proxy_string = f"http://{proxy_username}:{proxy_password}@{CITY_PROXY_HOST}/"
    http.proxies = {
        "http": proxy_string,
        "https": proxy_string
    }

    return http


def _get_dataset_metadata(dataset_name, ckan_api_key, session) -> dict or None:
    """Utility function wrapping call to get metadata

    :param dataset_name: Name of CKAN dataset
    :param ckan_api_key: CKAN secret used to authenticate user
    :param session: HTTP session to use to make call
    :return:
    """
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


def _form_dataset_resources_lookup(dataset_metadata) -> dict:
    """Utility function for flattening dataset's resources

    :param dataset_metadata: CKAN dataset metadata dictionary
    :return: Dictionary for each resource within a dataset
    """
    dataset_resource_lookup = {
        resource['name']: {
            "id": resource["id"],
            CHECKSUM_FIELD: resource.get(CHECKSUM_FIELD, None)
        }
        for resource in dataset_metadata['resources']
    }

    return dataset_resource_lookup


def _generate_checksum(datafile) -> str:
    """Utility function to generate MD5 checksum

    :param datafile: File object to generate checksum
    :return: MD5 checksum string
    """
    datafile.seek(0)
    data = datafile.read()
    datafile.seek(0)

    md5sum = hashlib.md5(data).hexdigest()

    return md5sum


def upload_data_to_ckan(filename, data_file, dataset_name, resource_name, ckan_api_key, session) -> bool:
    """Uploads data as a resource to a CKAN dataset. Does so opportunistically
    Assumes that the dataset exists

    :param filename: Name of source data file
    :param data_file: Source data file object
    :param dataset_name: Name to use for CKAN dataset (**assumed to exist**)
    :param resource_name: Name to use for CKAN resource
    :param ckan_api_key: CKAN API key for user
    :param session: HTTP session to use
    :return: Boolean flag to indicate that the upload has been successful
    """
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

    # Creating the resource for the first time
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
    # Updating the resource, because the checksum is different
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
    # Do nothing
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
                'upload': (filename, data_file)
            },
        )
        assert resp.status_code == 200
    except requests.exceptions.ProxyError as e:
        logging.error(f"Received proxy error when uploading data: '{e}'")
        logging.warning(f"Assuming this is a graceful shutdown!")

    return True
