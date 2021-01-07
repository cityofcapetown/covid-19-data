import itertools
import json
import logging
import os
import sys

from db_utils import minio_utils

from media_data_to_minio import BRANDSEYE_CACHE_PATH
import media_data_to_minio

BUCKET = 'covid'
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE
FULL_FILENAME_PATH = "data/private/sd_media_complete"
FILTER_FILENAME_PATH = "data/public/sd_media_filtered"

SD_TAG_IDS = ("80989",)
SD_CONTENT_TERMS = (
    # General names
    "service*", "#service*",
    "basicservice*", "#basicservice*",
    # Based-off dept Names
    "water and sanitation",
    "electricity",
    "roads",
    "solid waste", "rubbish", "garbage", "dirt",
    "housing", "council housing", "public housing",
    "recreation", "parks",
    # Based-off top 10 issues
    "sewer*", "#sewer*", "drain*", "#drain*",
    "power", "#power",
    "street light*", "#streetlight*",
    "pothole*", "#pothole*",
    "bin*", "#bin*",
    "water*", "#water*",)

SEARCH_START_DATE = '2015-12-31'
SEARCH_INTERVAL = "14D"
REFRESH_INTERVAL = 2

CACHED_DATA_PREFIX = f"{BRANDSEYE_CACHE_PATH}/service_delivery_requests/"

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info('Assembl[ing] search parameters')
    requests_params = media_data_to_minio.assemble_request_params_list(SEARCH_START_DATE, SEARCH_INTERVAL,
                                                                       SD_TAG_IDS, SD_CONTENT_TERMS)

    logging.info("Making Call to Brandseye's API...")
    mentions = list(itertools.chain((
        mention
        for request_params in requests_params[:-REFRESH_INTERVAL]
        for mention in media_data_to_minio.make_cached_request(request_params,
                                                               secrets["brandseye"]["username"],
                                                               secrets["brandseye"]["password"],
                                                               secrets['proxy']['username'],
                                                               secrets['proxy']['password'],
                                                               secrets["minio"]["edge"]["access"],
                                                               secrets["minio"]["edge"]["secret"],
                                                               cache_prefix=CACHED_DATA_PREFIX)
    ), (
        mention
        for request_params in requests_params[-REFRESH_INTERVAL:]
        for mention in media_data_to_minio.make_cached_request(request_params,
                                                               secrets["brandseye"]["username"],
                                                               secrets["brandseye"]["password"],
                                                               secrets['proxy']['username'],
                                                               secrets['proxy']['password'],
                                                               secrets["minio"]["edge"]["access"],
                                                               secrets["minio"]["edge"]["secret"],
                                                               flush_cache=True,
                                                               cache_prefix=CACHED_DATA_PREFIX)
    )))

    logging.info("Creating Mentions DataFrame...")
    mentions_df = media_data_to_minio.create_mentions_df(mentions)

    logging.info("Writing full DataFrame to Minio...")
    minio_utils.dataframe_to_minio(mentions_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   MINIO_CLASSIFICATION,
                                   filename_prefix_override=FULL_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")

    logging.info("Filtering DataFrame...")
    FILTER_COLS = ("id", "categoryId", "socialNetworkId",
                   "published", "title", "link",
                   "sentiment", "engagement")
    filtered_df = media_data_to_minio.filter_mentions_df(mentions_df)

    logging.info("Writing Filtered DataFrame to Minio...")
    minio_utils.dataframe_to_minio(filtered_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=FILTER_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")

    logging.info("...Done!")
