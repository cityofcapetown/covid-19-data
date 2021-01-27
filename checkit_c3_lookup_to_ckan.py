import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import pandas
from shareplum import site

import checkit_ckan_to_sharepoint
import checkit_sharepoint_to_ckan
import ckan_utils
import sharepoint_utils

CHECKIT_C3_COL = "c3_tracking_number"

C3_DATASET_NAME = "sap-r3-connector.augmented-service-notifications"

CHECKIT_C3_DATA_OUTPUT_FILE = 'c3-checkit-lookup-21-09-2020.csv'


def get_output_df(sharepoint_data):
    with tempfile.NamedTemporaryFile("r+b") as temp_data_file:
        temp_data_file.write(sharepoint_data)
        temp_data_file.flush()

        checkit_df = pandas.read_excel(temp_data_file.name)
        logging.debug(f"checkit_df.shape={checkit_df}")

    # Coercing checkit C3 ids into expected form - 12, zero-padded digits
    checkit_df[CHECKIT_C3_COL] = checkit_df[CHECKIT_C3_COL].dropna().astype(int).apply(
        lambda no: f"{no:012d}"
    )

    return checkit_df


def get_c3_data(minio_access, minio_secret):
    return minio_utils.minio_to_dataframe(
        minio_bucket=C3_DATASET_NAME,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=minio_utils.DataClassification.LAKE,
    )


def merge_data(checkit_df, c3_df):
    logging.debug(f"checkit_df.shape={checkit_df.shape}")
    logging.debug(f"c3_df.shape={c3_df.shape}")
    merge_df = c3_df.merge(
        checkit_df.set_index(CHECKIT_C3_COL),
        left_index=True,
        right_index=True,
        how="inner",
        validate="one_to_many"
    )
    logging.debug(f"merge_df.shape={merge_df.shape}")
    logging.debug(f"merge_df.columns={merge_df.columns}")

    return merge_df


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

    logging.info("G[etting] data from sharepoint")
    sp_site = sharepoint_utils.get_sp_site(checkit_ckan_to_sharepoint.SP_DOMAIN,
                                           checkit_ckan_to_sharepoint.SP_SITE,
                                           sp_auth, site.Version.v2016)
    sp_data = sharepoint_utils.get_sp_file(sp_site,
                                           checkit_ckan_to_sharepoint.CHECKIT_FOLDER,
                                           checkit_sharepoint_to_ckan.CHECKIT_OUTPUT_FILE)
    logging.info("G[ot] data from sharepoint")

    logging.info("Convert[ing] data from sharepoint")
    checkit_data_df = get_output_df(sp_data)
    logging.info("Convert[ed] sharepoint data")

    logging.info("G[etting] C3 data")
    service_notification_df = get_c3_data(
        secrets["minio"]["lake"]["access"], secrets["minio"]["lake"]["secret"]
    )
    logging.info("G[ot] C3 data")

    logging.info("Merg[ing] C3 and Checkit data")
    merged_df = merge_data(checkit_data_df, service_notification_df)
    logging.info("Merg[ed] C3 and Checkit data")

    logging.info("Wr[iting] data to CKAN...")
    with tempfile.NamedTemporaryFile("rb", suffix=".csv") as temp_data_file:
        merged_df.to_csv(temp_data_file.name)

        http_session = ckan_utils.setup_http_session(secrets['proxy']['username'], secrets['proxy']['password'])
        result = ckan_utils.upload_data_to_ckan(
            CHECKIT_C3_DATA_OUTPUT_FILE, temp_data_file,
            checkit_ckan_to_sharepoint.CKAN_DATASET, CHECKIT_C3_DATA_OUTPUT_FILE,
            secrets["ocl-ckan"]["ckan-api-key"], http_session
        )
    logging.info("...Wr[ote] data to CKAN")
