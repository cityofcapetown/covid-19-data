import logging
import os
import tempfile
import time

from exchangelib import DELEGATE, Account, Credentials, Configuration, NTLM, FileAttachment

EXCHANGE_SERVER = "webmail.capetown.gov.za"
EXCHANGE_EMAIL = "opm.data@capetown.gov.za"


def set_exchange_loglevel(level):
    # Turning down various exchange loggers - they're abit noisy
    exchangelib_loggers = [
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if name.startswith("exchangelib")
    ]
    for logger in exchangelib_loggers:
        logger.setLevel(level)


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
        access_type=DELEGATE,
    )

    return account


def filter_account(account, subject_filter=None, sender_filter=None):
    logging.debug("Filtering Inbox")

    filters = {}
    if subject_filter:
        filters["subject__contains"] = subject_filter
    if sender_filter:
        filters["sender__icontains"] = sender_filter

    filtered_items = account.inbox.filter(**filters)

    return filtered_items


def get_attachment_files(filtered_items, most_recent_count=None):
    with tempfile.TemporaryDirectory() as tempdir:
        logging.debug("Created temp dir '{}'".format(tempdir))
        items = filtered_items.order_by('-datetime_received')[
                :most_recent_count] if most_recent_count else filtered_items

        for item in items:
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


def get_latest_attachment_file(filtered_items):
    return get_attachment_files(filtered_items, 1)
