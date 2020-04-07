import json
import logging
import os
import sys
import tempfile
import time

from db_utils import minio_utils
from exchangelib import DELEGATE, NTLM, Account, Credentials, Configuration, NTLM, FileAttachment, Build, Version

def setup_exchange_account(username, password):
    logging.debug("Creating config for account with username '{}'".format(username))
    credentials = Credentials(username=username, password=password)
    config = Configuration(
        server="webmail.capetown.gov.za", 
        credentials=credentials,
        auth_type=NTLM
    )
    
    # Seems to help if you have a pause before trying to log in
    time.sleep(1)
    
    logging.debug("Logging into account")
    account = Account(
        primary_smtp_address="opm.data@capetown.gov.za", 
        config=config, autodiscover=False, 
        access_type=DELEGATE
    )
    
    return account


def filter_account(account, subject_line_content):
    logging.debug("Filtering Inbox")
    
    filtered_items = account.inbox.filter(subject__contains=SUBJECT_FILTER)
    
    return filtered_items


def get_attachment_file(filtered_items):
    with tempfile.TemporaryDirectory() as tempdir:
        logging.debug("Created temp dir '{}'".format(tempdir))
        for item in filtered_items.order_by('-datetime_received')[:1]:
            logging.debug(
                "Received match: '{} {} {}''".format(item.subject, item.sender, item.datetime_received)
            )
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    logging.debug("Downloading '{}'".format(attachment.name))
                    local_path = os.path.join(tempdir, attachment.name)
                    
                    with open(local_path, 'wb') as f, attachment.fp as fp:
                        buffer = fp.read(1024)
                        while buffer:
                            f.write(buffer)
                            buffer = fp.read(1024)
                            
                    yield local_path

    
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
    
    account = setup_exchange_account(secrets["proxy"]["username"], 
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
    filtered_items = filter_account(account, SUBJECT_FILTER)

    CURRENT_FILENAME = "covid_19 announcements.xlsx"
    BUCKET = 'covid'
    PUBLIC_PREFIX = "data/staging/"
    for attachment_path in get_attachment_file(filtered_items):
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
