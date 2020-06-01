import json
import os
import logging
import re
import sys
import stat
import tempfile
import zipfile

import paramiko

from db_utils import minio_utils

CITY_PROXY_HOSTNAME = "internet.capetown.gov.za"
CITY_PROXY_PORT = "8080"

FTP_HOSTNAME = "164.151.8.14"
FTP_PORT = "5022"
FTP_SYNC_DIR_NAME = 'WCGH_COCT'

PROV_HEALTH_BACKUP_PREFIX = "data/staging/wcgh_backup/"
RESTRICTED_PREFIX = "data/private/"
BUCKET = 'covid'
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE

COVID_SUM_FILENAME_REGEX = "^covid_sum.*txt$"
COVID_SUM_FILENAME = "covid_sum_latest.txt"


def get_sftp_client(proxy_username, proxy_password, ftp_username, ftp_password):
    proxy = paramiko.proxy.ProxyCommand(
        (f'/usr/bin/ncat --proxy {CITY_PROXY_HOSTNAME}:{CITY_PROXY_PORT} '
         f'--proxy-type http '
         f'--proxy-auth {proxy_username}:{proxy_password} '
         f'{FTP_HOSTNAME} {FTP_PORT}')
    )
    transport = paramiko.Transport(sock=proxy)
    transport.connect(username=ftp_username, password=ftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    return sftp


def sftp_flatten(parent_path, sftp):
    for sftp_fileattr in sftp.listdir_attr(parent_path):
        remote_path = os.path.join(parent_path, sftp_fileattr.filename)

        logging.debug(f"remote_path={remote_path}, "
                      f"stat.S_ISDIR(sftp_fileattr.st_mode)={stat.S_ISDIR(sftp_fileattr.st_mode)}")

        if stat.S_ISDIR(sftp_fileattr.st_mode):
            for child_fileattr, child_path in sftp_flatten(remote_path, sftp):
                yield child_fileattr, child_path
        else:
            yield sftp_fileattr, remote_path


def get_prov_files(sftp):
    # Recursive fetch of files and their paths
    list_of_files = list(sftp_flatten(FTP_SYNC_DIR_NAME, sftp))

    # Sorting by modification time
    list_of_files.sort(key=lambda sftp_file_tuple: sftp_file_tuple[0].st_mtime)

    filename_list = ', '.join(map(
        lambda sftp_file_tuple: sftp_file_tuple[1], list_of_files
    ))
    logging.debug(f"Got the following list of files from FTP server: '{filename_list}'")

    with tempfile.TemporaryDirectory() as tempdir:
        # Getting the files from the FTP server
        for sftp_file, sftp_file_remote_path in list_of_files:
            filename = sftp_file.filename
            logging.debug(f"Getting '{sftp_file_remote_path}'...")
            local_path = os.path.join(tempdir, filename)

            sftp.get(sftp_file_remote_path, local_path)

        # Still doing this within the tempdir context manager
        for sftp_file, _ in list_of_files:
            filename = sftp_file.filename
            local_path = os.path.join(tempdir, filename)
            # This is reliant on the file's modified time, hence it's probably the latest
            last_sftp_file, _ = list_of_files[-1]
            probably_latest = sftp_file is last_sftp_file
            logging.debug(f"local_path={local_path}, probably_latest={probably_latest}")

            yield local_path, probably_latest


def get_zipfile_contents(zfilename, zfile_password, latest):
    with tempfile.TemporaryDirectory() as tempdir, zipfile.ZipFile(zfilename) as zfile:
        zfile_contents = zfile.namelist()
        logging.debug(f"Found the following files listed in '{zfilename}': {', '.join(zfile_contents)}")
        for zcontent_filename in zfile_contents:
            logging.debug(f"Extracting '{zcontent_filename}'")
            zfile.extract(zcontent_filename, path=tempdir, pwd=zfile_password.encode())

            local_path = os.path.join(tempdir, zcontent_filename)
            yield PROV_HEALTH_BACKUP_PREFIX, local_path

            # Additionally, if this looks like a covid sum file, and its the latest
            # then make a generic latest file symlink for it
            if re.match(COVID_SUM_FILENAME_REGEX, zcontent_filename) and latest:
                latest_file_local_path = os.path.join(tempdir, COVID_SUM_FILENAME)
                os.link(local_path, latest_file_local_path)

                yield RESTRICTED_PREFIX, latest_file_local_path


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
    sftp_client = get_sftp_client(
        secrets["proxy"]["username"], secrets["proxy"]["password"],
        secrets["ftp"]["wcgh"]["username"], secrets["ftp"]["wcgh"]["password"],
    )
    logging.info("Auth[ed] with FTP server")

    # Getting files from provincial server
    logging.info("Get[ing] files from FTP server...")
    seen_the_latest_covid_sum_file = False
    for ftp_file_path, probably_latest_file in get_prov_files(sftp_client):
        logging.debug(f"Backing up {ftp_file_path}...")
        minio_utils.file_to_minio(
            filename=ftp_file_path,
            filename_prefix_override=PROV_HEALTH_BACKUP_PREFIX,
            minio_bucket=BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=BUCKET_CLASSIFICATION,
        )

        if zipfile.is_zipfile(ftp_file_path):
            logging.debug(f"{ftp_file_path} appears to be a zip file, attempting to decompress...")
            for file_path_prefix, zcontent_file_path in get_zipfile_contents(ftp_file_path,
                                                           secrets["ftp"]["wcgh"]["password"],
                                                           probably_latest_file):
                if COVID_SUM_FILENAME in zcontent_file_path:
                    seen_the_latest_covid_sum_file = True

                logging.debug(f"...extracted {zcontent_file_path}")
                minio_utils.file_to_minio(
                    filename=zcontent_file_path,
                    filename_prefix_override=file_path_prefix,
                    minio_bucket=BUCKET,
                    minio_key=secrets["minio"]["edge"]["access"],
                    minio_secret=secrets["minio"]["edge"]["secret"],
                    data_classification=BUCKET_CLASSIFICATION,
                )

    assert seen_the_latest_covid_sum_file, "Did *not* copy a latest file"

    logging.info("G[ot] files from FTP server")
