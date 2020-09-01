import fnmatch
import json
import os
import pathlib
import logging
import sys
import tempfile
import zipfile

from db_utils import minio_utils

import ckan_utils

BUCKET = 'covid'
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE

VULNERABILITY_VIEWER_SOURCE = "*-hotspots.html"
DASHBOARD_ASSETS = "assets/*"
PUBLIC_WIDGETS_PREFIX = "widgets/public/*"
PRIVATE_WIDGETS_LIBDIR_PREFIX = "widgets/private/libdir/*"
PRIVATE_WIDGETS_JS_PREFIX = "widgets/private/*.js"
PRIVATE_WIDGETS_CSS_PREFIX = "widgets/private/*.css"
CITY_MAP_WIDGETS_PREFIX = "widgets/private/case_count_maps/*"
STATS_TABLE_WIDGETS_PREFIX = "widgets/private/subdistrict_stats_table_widgets/*"
CT_EPI_WIDGETS = "widgets/private/ct_*"
CCT_EPI_WIDGETS = "widgets/private/cct_*"
MODEL_WIDGETS = "widgets/private/wc_model_*"
LATEST_VALUES = "widgets/private/latest_values.json"

VV_SHARE_PATTERNS = (
    VULNERABILITY_VIEWER_SOURCE,
    DASHBOARD_ASSETS,
    PUBLIC_WIDGETS_PREFIX,
    PRIVATE_WIDGETS_LIBDIR_PREFIX,
    PRIVATE_WIDGETS_JS_PREFIX,
    PRIVATE_WIDGETS_CSS_PREFIX,
    CITY_MAP_WIDGETS_PREFIX,
    STATS_TABLE_WIDGETS_PREFIX,
    CT_EPI_WIDGETS,
    CCT_EPI_WIDGETS,
    MODEL_WIDGETS,
    LATEST_VALUES
)
VV_EXCLUDE_LIST = (
    "cpop_gt55.geojson",
)

CASE_MAPS_SHARE_PATTERN = (
    CITY_MAP_WIDGETS_PREFIX,
)

SHARE_CONFIG = (
    # Dest Dataset, Dest resource name, Resource filename, Patterns to match, Patterns to exclude
    ("vulnerability-viewer", "Vulnerability Viewer", "vulnerability_viewer.zip", VV_SHARE_PATTERNS, VV_EXCLUDE_LIST),
)


def pull_down_covid_bucket_files(minio_access, minio_secret, patterns, exclude_patterns):
    with tempfile.TemporaryDirectory() as tempdir:
        logging.debug("Sync[ing] data from COVID bucket")

        def _list_bucket_objects(minio_client, minio_bucket, prefix=None):
            object_set = set([obj.object_name
                              for obj in minio_client.list_objects_v2(minio_bucket, recursive=True)
                              if (
                                      any(map(lambda p: fnmatch.fnmatch(obj.object_name, p), patterns)) and not
                                      any(map(lambda p: fnmatch.fnmatch(obj.object_name, p), exclude_patterns))
                              )])
            logging.debug(f"object_set={', '.join(object_set)}")

            return object_set

        minio_utils._list_bucket_objects = _list_bucket_objects

        minio_utils.bucket_to_dir(
            tempdir, BUCKET,
            minio_access, minio_secret, BUCKET_CLASSIFICATION
        )
        logging.debug("Sync[ed] data from COVID bucket")

        for dirpath, _, filenames in os.walk(tempdir):
            for filename in filenames:
                local_path = os.path.join(dirpath, filename)
                remote_path = pathlib.Path(os.path.relpath(local_path, tempdir))

                yield local_path, remote_path


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

    # Getting SFTP client
    logging.info("G[etting] HTTP session")
    http_session = ckan_utils.setup_http_session(secrets['proxy']['username'], secrets['proxy']['password'])
    logging.info("G[ot] HTTP session")

    for dest_dataset, dest_resource, resource_filename, patterns, exclude_patterns in SHARE_CONFIG:
        logging.info(f"looking for matches for '{dest_dataset}':'{dest_resource}'")

        with tempfile.NamedTemporaryFile("rb+", suffix=".zip") as zipped_data_file:
            # Creating Zip archive
            logging.info(f"Creat[ing] zip archive for '{resource_filename}'")
            with zipfile.ZipFile(zipped_data_file.name, "w") as zipped_data:
                for local_path, remote_path in pull_down_covid_bucket_files(secrets["minio"]["edge"]["access"],
                                                               secrets["minio"]["edge"]["secret"],
                                                               patterns, exclude_patterns):
                    logging.debug(f"Adding '{local_path}' to zip archive")
                    zipped_data.write(local_path, arcname=remote_path)
            logging.info(f"Creat[ed] zip archive for '{resource_filename}'")

            logging.info("Upload[ing] to CKAN")
            ckan_utils.upload_data_to_ckan(resource_filename, zipped_data_file,
                                           dest_dataset, dest_resource,
                                           secrets["ocl-ckan"]["ckan-api-key"], http_session)
            logging.info("Upload[ed] to CKAN")
