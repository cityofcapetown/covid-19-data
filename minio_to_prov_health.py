import fnmatch
import json
import os
import pathlib
import logging
import sys
import tempfile

from db_utils import minio_utils
import paramiko
import prov_health_data_to_minio

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
CASE_MAPS_SHARE_PATTERN = (
    CITY_MAP_WIDGETS_PREFIX,
)

FTP_SYNC_DIR_NAME = 'COCT_WCGH'
SHARE_CONFIG = (
    # Dest Dir, Patterns to match
    (os.path.join(FTP_SYNC_DIR_NAME, "vulnerability_viewer"), VV_SHARE_PATTERNS),
    (os.path.join(FTP_SYNC_DIR_NAME, "case_maps"), CASE_MAPS_SHARE_PATTERN),
)


def pull_down_covid_bucket_files(minio_access, minio_secret, patterns):
    with tempfile.TemporaryDirectory() as tempdir:
        logging.debug("Sync[ing] data from COVID bucket")

        def _list_bucket_objects(minio_client, minio_bucket, prefix=None):
            object_set = set([obj.object_name
                              for obj in minio_client.list_objects_v2(minio_bucket, recursive=True)
                              if any(map(lambda p: fnmatch.fnmatch(obj.object_name, p), patterns))])
            logging.debug(f"object_set={', '.join(object_set)}")

            return object_set

        minio_utils._list_bucket_objects = _list_bucket_objects

        minio_utils.bucket_to_dir(
            tempdir, prov_health_data_to_minio.BUCKET,
            minio_access, minio_secret, prov_health_data_to_minio.BUCKET_CLASSIFICATION
        )
        logging.debug("Sync[ed] data from COVID bucket")

        for dirpath, _, filenames in os.walk(tempdir):
            for filename in filenames:
                local_path = os.path.join(dirpath, filename)
                remote_path = pathlib.Path(os.path.relpath(local_path, tempdir))

                yield local_path, remote_path


def ftp_mkdir_p(sftp, dir_path):
    parent_dirs = list(reversed(dir_path.parents))[1:]
    for path_dir in parent_dirs:
        child_dirs = sftp.listdir(str(path_dir.parent))
        dir_name = str(path_dir.name)
        if dir_name not in child_dirs:
            logging.debug(f"Creating '{dir_name}' in {path_dir.parent}")
            dir_full_path = os.path.join(path_dir.parent, dir_name)
            sftp_client.mkdir(dir_full_path)


def check_update(sftp, remote_path, local_path):
    file_list = sftp.listdir(str(remote_path.parent))
    if str(remote_path.name) in file_list:
        sftp_attr = sftp.stat(str(remote_path))
        logging.debug(f"sftp_attr={sftp_attr.st_size}")
        local_attr = paramiko.sftp_attr.SFTPAttributes.from_stat(os.stat(local_path))
        logging.debug(f"local_attrs={local_attr.st_size}")

        return sftp_attr.st_size != local_attr.st_size
    else:
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

    # Getting SFTP client
    logging.info("Auth[ing] with FTP server")
    sftp_client = prov_health_data_to_minio.get_sftp_client(
        secrets["proxy"]["username"], secrets["proxy"]["password"],
        secrets["ftp"]["wcgh"]["username"], secrets["ftp"]["wcgh"]["password"],
    )
    logging.info("Auth[ed] with FTP server")

    for dest_dir, patterns in SHARE_CONFIG:
        logging.info(f"looking for matches for '{dest_dir}'")
        for local_path, remote_path in pull_down_covid_bucket_files(secrets["minio"]["edge"]["access"],
                                                                    secrets["minio"]["edge"]["secret"],
                                                                    patterns):
            remote_path = pathlib.Path(os.path.join(dest_dir, remote_path))
            logging.debug(f"{local_path} -> {remote_path}")

            # Creating the dir, if it doesn't exist
            ftp_mkdir_p(sftp_client, remote_path)

            # listing the file to see if it already exists
            should_update = check_update(sftp_client, remote_path, local_path)

            # Finally, updating the file, if necessary
            if should_update:
                sftp_client.put(local_path, str(remote_path))
            else:
                logging.warning(f"Not updating '{remote_path}' - sizes are the same")
