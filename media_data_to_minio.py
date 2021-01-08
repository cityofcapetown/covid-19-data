import functools
import hashlib
import itertools
import json
import math
import logging
import os
import pprint
import sys
import tempfile

from db_utils import minio_utils
import pandas
import requests

# Parameter Components
BRAND_ID = "95745"
# Supplied by Lara Basson, Brandseye on 2019-03-19
CITY_AUTHOR_IDS = ('Fx70a1921b8483bebf',
                   'Fx853d9b920b39998f',  # MyCiti Bus
                   'Ixcityofct',
                   'Tx132437983',
                   'Tx2645400613')
COVID_TAG_IDS = ("120314",)
COVID_CONTENT_TERMS = ('covid*',
                       '#covid*',
                       'corona*',
                       '#corona*',
                       '#corina*',
                       'corina*')
SEARCH_START_DATE = '2019-12-01'
SEARCH_INTERVAL = "7D"
REFRESH_INTERVAL = 2

# tags col
NESTED_COLS_WITH_IDS = ("socialNetwork", "category")
TAGS_COL = "tags"
TAG_IDS_COL = "tagIds"
TOPIC_NAMESPACE_VALUES = {"topic", "topic_tree"}

BRANDSEYE_HOST = "api.brandseye.com"
BRANDSEYE_PATH = "v4/accounts/BECI15AA/mentions"

BUCKET = 'covid'
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE

BRANDSEYE_CACHE_PATH = "data/staging/brandseye_cache"
CACHED_DATA_PREFIX = f"{BRANDSEYE_CACHE_PATH}/covid_requests/"

FILTER_COLS = ("id", "categoryId", "socialNetworkId",
               "published", "title", "link",
               "sentiment", "engagement")

FULL_FILENAME_PATH = "data/private/media_complete"
FILTER_FILENAME_PATH = "data/private/media_filtered"


def assemble_request_params(search_start_date, search_end_date, tag_ids, content_terms,
                            brand_id=BRAND_ID, author_ids=CITY_AUTHOR_IDS, limit=100000):
    # Filtering out all of the City posts
    author_exclude_str = " AND ".join(
        map(lambda author: f"AuthorId isnt '{author}'", author_ids)
    )
    # logging.debug(f"author_exclude_str='{author_exclude_str}'")
    # Looking for anything that matches the search terms
    content_terms_str = " OR ".join(
        map(lambda content: f"Content matches '{content}'", content_terms)
    )
    # Looking for anything that matches the tags
    tag_ids_str = " OR ".join(
        map(lambda tag_id: f"tag is {tag_id}", tag_ids)
    )
    content_tags_combined = f"{content_terms_str} OR {tag_ids_str}"
    # logging.debug(f"content_tags_combined='{content_tags_combined}'")

    # Selecting any novel posts, or any that are in reply to relevant posts
    subquery_str = (f"conversationid in "
                    f"(replyto is unknown AND "
                    f"published after '{search_start_date}' AND published before '{search_end_date}' AND "
                    f"({content_tags_combined}))")
    # logging.debug(f"subquery_str={subquery_str}")

    # We're going to do an AND match on this
    filter_conditions = (
        f"published after '{search_start_date}' AND published before '{search_end_date}'",
        f"brand isorchildof {brand_id}",
        "media isnt enterprise",
        author_exclude_str,
        "Relevancy isnt IRRELEVANT",
        f"({subquery_str} OR ({content_tags_combined}))"
    )

    params = {
        "select": "*",  # suck in everything we can...
        "filter": " AND ".join(filter_conditions),
        "limit": limit
    }
    # logging.debug(f"params=\n{pprint.pformat(params)}")

    return params


def assemble_request_params_list(search_start_date=SEARCH_START_DATE, search_interval=SEARCH_INTERVAL,
                                 tag_ids=COVID_TAG_IDS, content_terms=COVID_CONTENT_TERMS):
    tomorrow = pandas.Timestamp.now() + pandas.Timedelta(days=1)
    periods = math.ceil(
        (tomorrow - pandas.Timestamp(search_start_date)) / pandas.Timedelta(search_interval)
    ) + 1
    logging.debug(f"periods={periods}")
    request_date_ranges = [
        date.strftime("%Y/%m/%d")
        for date in pandas.date_range(search_start_date, periods=periods, freq=search_interval)
    ]

    requests_params_list = [
        assemble_request_params(start_date, end_date, tag_ids, content_terms)
        for start_date, end_date in zip(
            request_date_ranges[:-1], request_date_ranges[1:]
        )
    ]

    return requests_params_list


def make_request(params, username, password, proxy_username, proxy_password, host=BRANDSEYE_HOST, path=BRANDSEYE_PATH):
    proxy_url_with_auth = f"http://{proxy_username}:{proxy_password}@internet.capetown.gov.za:8080"
    proxies = {
        "http": proxy_url_with_auth,
        "https": proxy_url_with_auth,
    }

    auth = requests.auth.HTTPBasicAuth(username, password)
    resp = requests.get("https://{}/{}".format(host, path),
                        params=params, proxies=proxies, auth=auth)

    assert resp.ok, f"Request failed: '{resp.status_code}' - '{resp.text}'"

    mentions_dicts = resp.json()
    assert len(mentions_dicts) < params['limit'], "Request limit reached, decrease search interval!"

    logging.debug("Retrieved '{}' mentions_dicts".format(len(mentions_dicts)))

    return mentions_dicts


@functools.lru_cache()
def _get_cached_files(minio_access, minio_secret, cache_prefix):
    cached_files_list = set(minio_utils.list_objects_in_bucket(BUCKET,
                                                               minio_access, minio_secret, MINIO_CLASSIFICATION,
                                                               minio_prefix_override=cache_prefix))
    logging.debug(f"cached_files_list=\n{pprint.pformat(cached_files_list)}")
    return cached_files_list


def make_cached_request(params, username, password, proxy_username, proxy_password,
                        minio_access, minio_secret, cache_prefix=CACHED_DATA_PREFIX, flush_cache=False):
    cached_files_list = _get_cached_files(minio_access, minio_secret, cache_prefix)

    # Getting cache lookup
    params_dump = json.dumps(params)
    params_checksum = hashlib.md5(params_dump.encode()).hexdigest()
    cached_params_filename = f"{params_checksum}.json"
    cached_params_minio_path = os.path.join(cache_prefix, cached_params_filename)
    logging.debug(f"cached_params_minio_path='{cached_params_minio_path}'")

    if cached_params_minio_path in cached_files_list and not flush_cache:
        logging.debug("Data in Minio cache, fetching it from there rather...")
        with tempfile.NamedTemporaryFile("r") as cache_file:
            result = minio_utils.minio_to_file(cache_file.name,
                                               BUCKET, minio_access, minio_secret, MINIO_CLASSIFICATION,
                                               minio_filename_override=cached_params_minio_path)

            assert result, f"Failed to fetch cache file '{cached_params_minio_path}'!"
            cached_data = json.load(cache_file)

        return cached_data
    else:
        logging.debug("Making request to Brandseye API")
        logging.debug(f"params=\n{pprint.pformat(params)}")
        request_data = make_request(params, username, password, proxy_username, proxy_password)

        with tempfile.TemporaryDirectory() as tempdir:
            local_path = os.path.join(tempdir, cached_params_filename)

            with open(local_path, "w") as cache_file:
                json.dump(request_data, cache_file)

            result = minio_utils.file_to_minio(local_path,
                                               BUCKET, minio_access, minio_secret, MINIO_CLASSIFICATION,
                                               filename_prefix_override=cache_prefix)
            assert result, f"Failed to write cache file '{cached_params_minio_path}'!"

        return request_data


def create_mentions_df(mention_dicts, flatten_cols=NESTED_COLS_WITH_IDS):
    # Create the dataframe
    logging.debug(f"len(mentions_dicts)={len(mention_dicts)}")
    data_df = pandas.DataFrame.from_dict(mention_dicts)
    logging.debug(f"data_df.shape={data_df.shape}")

    # Flattening the IDs
    for nested_col in flatten_cols:
        data_df[f"{nested_col}Id"] = data_df[nested_col].apply(
            lambda nested_dict: nested_dict["id"] if pandas.notna(nested_dict) else "NA"
        )

    # Creating column of flattened tag IDs
    if TAGS_COL in data_df.columns:
        data_df[TAG_IDS_COL] = data_df[TAGS_COL].apply(
            lambda tag_dicts: ";".join(sorted([
                str(tag_dict["id"])
                for tag_dict in tag_dicts
                if tag_dict and tag_dict['namespace'] in TOPIC_NAMESPACE_VALUES
            ])) if isinstance(tag_dicts, list) else None
        )

    # Converting the published column to datetime, setting timezone to SAST
    data_df["published"] = pandas.to_datetime(data_df.published).dt.tz_convert("Africa/Johannesburg")

    # Sanitising bad unicode characters
    for col in data_df.columns:
        if data_df[col].dtype == object:
            logging.debug(f"Sanitising '{col}' values")
            data_df[col] = data_df[col].apply(
                lambda val: str(val).encode('utf-8', 'replace').decode('utf-8') if isinstance(val, str) else val
            )

    return data_df


def filter_mentions_df(complete_mentions_df, columns_to_select=FILTER_COLS):
    # Removing those which are *not* public - public have visibility=NaN
    filtered_df = complete_mentions_df.drop(
        complete_mentions_df[complete_mentions_df.visibility.notna()].index
    ).copy()

    # Selecting columns
    filtered_df = filtered_df[list(columns_to_select)]

    return filtered_df


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
    requests_params = assemble_request_params_list()

    logging.info("Making Call to Brandseye's API...")
    mentions = list(itertools.chain((
        mention
        for request_params in requests_params[:-REFRESH_INTERVAL]
        for mention in make_cached_request(request_params,
                                           secrets["brandseye"]["username"],
                                           secrets["brandseye"]["password"],
                                           secrets['proxy']['username'], secrets['proxy']['password'],
                                           secrets["minio"]["edge"]["access"],
                                           secrets["minio"]["edge"]["secret"])
    ), (
        mention
        for request_params in requests_params[-REFRESH_INTERVAL:]
        for mention in make_cached_request(request_params,
                                           secrets["brandseye"]["username"],
                                           secrets["brandseye"]["password"],
                                           secrets['proxy']['username'], secrets['proxy']['password'],
                                           secrets["minio"]["edge"]["access"],
                                           secrets["minio"]["edge"]["secret"],
                                           flush_cache=True)
    )))

    logging.info("Creating Mentions DataFrame...")
    mentions_df = create_mentions_df(mentions)

    logging.info("Writing full DataFrame to Minio...")

    minio_utils.dataframe_to_minio(mentions_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   MINIO_CLASSIFICATION,
                                   filename_prefix_override=FULL_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")

    logging.info("Filtering DataFrame...")
    filtered_df = filter_mentions_df(mentions_df, FILTER_COLS)

    logging.info("Writing Filtered DataFrame to Minio...")
    minio_utils.dataframe_to_minio(filtered_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=FILTER_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")

    logging.info("...Done!")
