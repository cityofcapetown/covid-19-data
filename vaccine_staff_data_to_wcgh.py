import json
import logging
import os
import pathlib
import sys

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import pandas
from db_utils import minio_utils

from hr_data_munge import HR_MASTER_FILENAME_PATH, BUCKET, HR_MASTER_STAFFNUMBER
from prov_health_data_to_minio import get_sftp_client

ID_COL = "ID number"
ID_ENCRYPED_COL = "id_encrypted"
STAFFNUMBER_ENCRYPTED_COL = "persno_encrypted"

ENCRYPTION_PADDING = padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
    )
PUBLIC_KEY_FILE = 'resources/cct-covid-vaccine-staff-pub.pem'

FTP_WRITE_DIR_NAME = 'COCT_WCGH'
FTP_WRITE_FILE_NAME = 'cct_staff_id_encrypted.csv.gz'


def _encrypt_data(hr_df):
    public_key_path = pathlib.Path(__file__).parent.absolute() / PUBLIC_KEY_FILE

    logging.debug(f"Reading Public key from {public_key_path.name}")
    with open(public_key_path, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
    logging.debug(f"Read Public key from {public_key_path.name}")

    logging.debug("Encrypting data")
    encrypted_df = pandas.DataFrame({
        encrypted_col: hr_df[col].apply(
            lambda id_val: public_key.encrypt(
                f"{float(id_val):.0f}".encode(), ENCRYPTION_PADDING
            ).hex() if pandas.notna(id_val) else None
        )
        for col, encrypted_col in ((HR_MASTER_STAFFNUMBER, STAFFNUMBER_ENCRYPTED_COL),
                                   (ID_COL, ID_ENCRYPED_COL))
    })
    logging.debug("Encrypted data")

    logging.debug("Shuffling data")
    shuffle_df = encrypted_df.sample(frac=1).reset_index(drop=True)
    logging.debug("Shuffling data")

    logging.debug(f"{shuffle_df.shape=}")
    logging.debug(f"shuffle_df=\n{shuffle_df.head(10)}")

    return shuffle_df


def _write_data_to_prov(encrypted_df, sftp):
    sftp_path = pathlib.Path(FTP_WRITE_DIR_NAME) / FTP_WRITE_FILE_NAME
    logging.debug(f"Open[ing] remote file '{sftp_path.name}'")
    with sftp.file(str(sftp_path), "wb") as output_file:
        logging.debug("Open[ed] remote file")

        logging.debug("Writ[ing] data")
        encrypted_df.to_csv(output_file, index=False)
        logging.debug("Wr[ote] data")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    SECRETS_PATH_VAR = "SECRETS_PATH"
    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("G[etting] Staff Master Data")
    hr_master_df = minio_utils.minio_to_dataframe(
        BUCKET, filename_prefix_override=HR_MASTER_FILENAME_PATH,
        data_classification=minio_utils.DataClassification.EDGE
    )
    logging.info("G[ot] Staff Master Data")

    logging.info("Form[ing] Encrypted Data")
    encrypted_hr_df = _encrypt_data(hr_master_df)
    logging.info("Form[ed] Encrypted Data")

    # Getting SFTP client
    logging.info("Auth[ing] with FTP server")
    sftp_client = get_sftp_client(
        secrets["proxy"]["username"], secrets["proxy"]["password"],
        secrets["ftp"]["wcgh"]["username"], secrets["ftp"]["wcgh"]["password"],
    )
    logging.info("Auth[ed] with FTP server")

    logging.info("Wr[iting] data file to server")
    _write_data_to_prov(encrypted_hr_df, sftp_client)
    logging.info("Wr[ote] data file to server")
