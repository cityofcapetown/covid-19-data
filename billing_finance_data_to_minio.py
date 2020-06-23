import json
import logging
import os
import sys

from db_utils import minio_utils

import exchange_utils

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_FILE"

    if SECRETS_PATH_VAR not in os.environ:
        print("Secrets path not found!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_FILE"]
    # secrets_path = "../secrets.json"
    secrets = json.load(open(secrets_path))
    account = exchange_utils.setup_exchange_account(secrets["proxy"]["username"],
                                                    secrets["proxy"]["password"])

    # Turning down various exchange loggers - they're abit noisy
    exchangelib_loggers = [
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if name.startswith("exchangelib")
    ]
    for logger in exchangelib_loggers:
        logger.setLevel(logging.INFO)

    SUBJECT_FILTER = 'MCSH-Billing_Invoicing'
    filtered_items = exchange_utils.filter_account(account, SUBJECT_FILTER)

    CURRENT_FILENAME = "MCSH-Billing_Invoicing.xlsx"
    BUCKET = 'covid'
    BUCKET_PREFIX = "data/staging/"
    for attachment_path in exchange_utils.get_latest_attachment_file(filtered_items):
        logging.debug("Uploading '{}' to minio://covid/{}".format(attachment_path, BUCKET_PREFIX))
        print(attachment_path)
        new_path = os.path.join(os.path.dirname(attachment_path), CURRENT_FILENAME)
        print(new_path)

        if attachment_path.endswith(".xlsx"):
            os.rename(attachment_path, new_path)
            minio_utils.file_to_minio(
                filename=new_path,
                filename_prefix_override=BUCKET_PREFIX,
                minio_bucket=BUCKET,
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
                data_classification=minio_utils.DataClassification.EDGE,
            )
    # Delete filtered items because we feel sorry for our poor inbox        
    filtered_items.delete()
