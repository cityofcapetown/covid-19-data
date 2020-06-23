import logging
import os
import tempfile
import time

from exchangelib import DELEGATE, Account, Credentials, Configuration, NTLM, FileAttachment

EXCHANGE_SERVER = "webmail.capetown.gov.za"
EXCHANGE_EMAIL = "opm.data@capetown.gov.za"


def setup_exchange_account(username, password,
                           exchange_server=EXCHANGE_SERVER, exchange_email=EXCHANGE_EMAIL):
    logging.debug("Creating config for account with username '{}'".format(username))
    credentials = Credentials(username=username, password=password)
    config = Configuration(
        server=exchange_server,
        credentials=credentials,
        auth_type=NTLM
    )

    # Seems to help if you have a pause before trying to log in
    time.sleep(1)

    logging.debug("Logging into account")
    account = Account(
        primary_smtp_address=exchange_email,
        config=config, autodiscover=False,
        access_type=DELEGATE
    )

    return account


def filter_account(account, subject_filter):
    logging.debug("Filtering Inbox")

    filtered_items = account.inbox.filter(subject__contains=subject_filter)

    return filtered_items


def get_latest_attachment_file(filtered_items):
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
