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
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        print("Secrets path not found!")
        sys.exit(-1)
        

    secrets_path = os.environ["SECRETS_PATH"]
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
    
    SUBJECT_FILTER = 'Covid-19 Announcements'
    filtered_items = exchange_utils.filter_account(account, SUBJECT_FILTER)

    CURRENT_FILENAME = "covid_19 announcements.xlsx"
    BUCKET = 'covid'
    PUBLIC_PREFIX = "data/staging/"
    RESTRICTED_PREFIX = "data/private/"
    for attachment_path in exchange_utils.get_latest_attachment_file(filtered_items):
        logging.debug("Uploading '{}' to minio://covid/{}".format(attachment_path, PUBLIC_PREFIX))
        
        minio_utils.file_to_minio(
            filename=attachment_path,
            filename_prefix_override=PUBLIC_PREFIX,
            minio_bucket=BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=minio_utils.DataClassification.EDGE,
        )

        if attachment_path.endswith(".xls") and not os.path.exists(CURRENT_FILENAME):
            os.link(attachment_path, CURRENT_FILENAME)
            minio_utils.file_to_minio(
                filename=CURRENT_FILENAME,
                filename_prefix_override=RESTRICTED_PREFIX,
                minio_bucket=BUCKET,
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
                data_classification=minio_utils.DataClassification.EDGE,
            )
