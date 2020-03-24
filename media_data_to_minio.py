import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import pandas
import requests


def assemble_request_params(search_start_date, brand_id, author_ids, tag_ids, content_terms, limit=100000):
    # Filtering out all of the City posts
    author_exclude_str = " AND ".join(
        map(lambda author: "AuthorId isnt '{}'".format(author), author_ids)
    )
    logging.debug("author_exclude_str='{}'".format(author_exclude_str))
    # Looking for anything that matches the search terms
    content_terms_str = " OR ".join(
        map(lambda content: "Content matches '{}'".format(content), content_terms)
    )
    # Looking for anything that matches the tag
    tag_ids_str = " OR ".join(
        map(lambda tag_id: "tag is {}".format(tag_id), tag_ids)
    )
    content_tags_combined = "{} OR {}".format(content_terms_str, tag_ids_str)
    logging.debug("content_tags_combined='{}'".format(content_tags_combined))

    subquery_str = "conversationid in (replyto is unknown AND published after '{}' AND ({}))".format(search_start_date,
                                                                                                     content_tags_combined)
    logging.debug("subquery_str={}".format(subquery_str))

    # We're going to do an AND match on this
    filter_conditions = (
        "published after '{}'".format(search_start_date),
        "brand isorchildof {}".format(brand_id),
        "media isnt enterprise",
        author_exclude_str,
        "Relevancy isnt IRRELEVANT",
        "({} OR ({}))".format(subquery_str, content_tags_combined)
    )

    params = {
        "select": "*",  # suck in everything we can...
        "filter": " AND ".join(filter_conditions),
        "limit": limit
    }

    return params


def make_request(host, path, params, username, password, proxy_username, proxy_password):
    proxy_url_with_auth = "http://{}:{}@internet.capetown.gov.za:8080".format(proxy_username, proxy_password)
    proxies = {
        "http": proxy_url_with_auth,
        "https": proxy_url_with_auth,
    }

    auth = requests.auth.HTTPBasicAuth(username, password)
    resp = requests.get("https://{}/{}".format(host, path),
                        params=params, proxies=proxies, auth=auth)

    assert resp.ok, "Request failed: '{}' - '{}'".format(resp.status_code, resp.text)

    mentions = resp.json()
    logging.debug("Retrieved '{}' mentions".format(len(mentions)))

    return mentions


def create_mentions_df(mentions, flatten_cols):
    # Create the dataframe
    mentions_df = pandas.DataFrame.from_dict(mentions)

    # Flattening the IDs
    for nested_col in flatten_cols:
        mentions_df[f"{nested_col}Id"] = mentions_df[nested_col].apply(
            lambda nested_dict: nested_dict["id"] if pandas.notna(nested_dict) else "NA"
        )

    # Converting the published column to datetime, setting timezone to SAST
    mentions_df["published"] = pandas.to_datetime(mentions_df.published).dt.tz_convert("Africa/Johannesburg")

    return mentions_df


def filter_mentions_df(mention_df, columns_to_select):
    # Removing those which are *not* public - public have visibility=NaN
    filtered_df = mentions_df.drop(
        mentions_df[mentions_df.visibility.notna()].index
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

    # Parameter Components
    BRAND_ID = "95745"
    # Supplied by Lara Basson, Brandseye on 2019-03-19
    AUTHOR_IDS = ('Fx70a1921b8483bebf',
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
    SEARCH_START_DATE = '2019/12/01'

    logging.info("Making Call to Brandseye's API...")
    request_params = assemble_request_params(SEARCH_START_DATE,
                                             BRAND_ID, AUTHOR_IDS,
                                             COVID_TAG_IDS, COVID_CONTENT_TERMS)

    BRANDSEYE_HOST = "api.brandseye.com"
    BRANDSEYE_PATH = "v4/accounts/BECI15AA/mentions"
    mentions = make_request(BRANDSEYE_HOST, BRANDSEYE_PATH, request_params,
                            secrets["brandseye"]["username"], secrets["brandseye"]["password"],
                            secrets['proxy']['username'], secrets['proxy']['password'])

    logging.info("Creating Mentions DataFrame...")
    NESTED_COLS_WITH_IDS = ("socialNetwork", "category")
    mentions_df = create_mentions_df(mentions, NESTED_COLS_WITH_IDS)

    logging.info("Writing full DataFrame to Minio...")
    BUCKET = 'covid'
    FULL_FILENAME_PATH = "data/private/media_complete"

    minio_utils.dataframe_to_minio(mentions_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=FULL_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")

    logging.info("Filtering DataFrame...")
    FILTER_COLS = ("id", "categoryId", "socialNetworkId",
                   "published", "title", "link",
                   "sentiment", "engagement")
    filtered_df = filter_mentions_df(mentions_df, FILTER_COLS)

    logging.info("Writing Filtered DataFrame to Minio...")
    FILTER_FILENAME_PATH = "data/public/media_filtered"
    minio_utils.dataframe_to_minio(filtered_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=FILTER_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")

    logging.info("...Done!")
