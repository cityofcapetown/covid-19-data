import json
import os
import logging
import sys
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
RESTRICTED_PREFIX = "data/staging/"
BUCKET = 'covid'
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE


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


def get_prov_files(sftp):
    list_of_files = sftp.listdir(FTP_SYNC_DIR_NAME)
    logging.debug(f"Got the following list of files from FTP server: '{', '.join(list_of_files)}'")

    with tempfile.TemporaryDirectory() as tempdir:
        # Getting the files from the FTP server
        for filename in list_of_files:
            logging.debug(f"Getting {filename}...")
            local_path = os.path.join(tempdir, filename)
            ftp_path = os.path.join(FTP_SYNC_DIR_NAME, filename)

            sftp.get(ftp_path, local_path)

        # Still doing this within the tempdir context manager
        for filename in list_of_files:
            local_path = os.path.join(tempdir, filename)
            yield local_path


def get_zipfile_contents(zfilename, zfile_password):
    with tempfile.TemporaryDirectory as tempdir:
        with zipfile.ZipFile(zfilename) as zfile:
            zfile_contents = zfile.namelist()
            logging.debug(f"Found the following files listed in '{zfilename}': {', '.join(zfile_contents)}")
            for zcontent_filename in zfile_contents:
                logging.debug(f"Extracting '{zcontent_filename}'")
                local_path = os.path.join(tempdir, zcontent_filename)
                zfile.extract(zcontent_filename, path=local_path, pwd=zfile_password.encode())

                yield local_path


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
    for ftp_file_path in get_prov_files(sftp_client):
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
            for zcontent_file_path in get_zipfile_contents(ftp_file_path):
                logging.debug(f"...extracted {zcontent_file_path}")
                minio_utils.file_to_minio(
                    filename=zcontent_file_path,
                    filename_prefix_override=RESTRICTED_PREFIX,
                    minio_bucket=BUCKET,
                    minio_key=secrets["minio"]["edge"]["access"],
                    minio_secret=secrets["minio"]["edge"]["secret"],
                    data_classification=BUCKET_CLASSIFICATION,
                )
    logging.info("G[ot] files from FTP server")
