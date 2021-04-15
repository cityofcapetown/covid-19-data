# base imports
import json
import logging
import os
import re
import sys
import tempfile
import urllib.parse
# external imports
from db_utils import minio_utils
import pandas
import sharepoint_utils


# minio constants
COVID_BUCKET = 'covid'
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
VACCINE_PREFIX_RAW = "data/private/staff_vaccine/raw/"
STAFF_LIST_PREFIX = f"staff_list_cleaned"
VAX_REGISTER_PREFIX = f"staff_vaccination_register"
RAW_VAX_REGISTER_PREFIX = f"staff_vaccination_register"
SEQ_SURVEY_PREFIX = "staff_sequencing_survey"
VACCIE_BACKUP_PREFIX = "data/private/staff_vaccine/backup/"
STAFF_LIST_BAK = f"{VACCIE_BACKUP_PREFIX}staff/"
SEQ_LIST_BAK = f"{VACCIE_BACKUP_PREFIX}sequencing/"
VACC_LIST_BAK = f"{VACCIE_BACKUP_PREFIX}vaccine/"

# sharepoint paths
SP_DOMAIN = 'http://teamsites.capetown.gov.za'
SP_SITE = '/sites/hshealth/C19/sv'

# sharepoint xml lists
SP_STAFF_LIST_NAME = 'staff_list_cleaned'
SP_VACCINE_REGISTER = 'Staff vaccination register'
SP_SEQUENCING_SURVEY = "Staff vaccination sequencing"

VAX_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
# fitler variables
SP_REGEX = re.compile(r'^\d+;#(.+)$')
ID_REGEX = re.compile(r'^\d+;#\{(.+)\}$')
STAFF_REGEX = re.compile(f"^string;#(.+)$")

# std columns
ACCESS_COL_NAME = "AccessTimestamp"
XML_URL_COL_NAME = 'URL Path'
SOURCE_COL_NAME = "SourceUrl"
ID_COL_NAME = 'Unique Id'

STAFF_MERGE_STR = "concatenated_details"
STAFF_LIST_EDIT_COLS = [STAFF_MERGE_STR]

# # vaccine register list cols
VAX_DATE = "Vaccination date"
MODIFIED = "Modified"
CREATED_DATE = "Created"
VAX_MERGE_STR = "Staff member"
DATE_COLS = [VAX_DATE, ACCESS_COL_NAME, MODIFIED, CREATED_DATE]


def minio_json_to_dict(minio_filename_override, minio_bucket, data_classification):
    """
    function to pull minio json file to python dict
    :param minio_filename_override: (str) minio override string (prefix and file name)
    :param minio_bucket: (str) minio bucket name
    :param minio_key: (str) the minio access key
    :param minio_secret: (str) the minio key secret
    :param data_classification: minio classification (edge | lake)
    :return: python dict
    """
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                           minio_filename_override=minio_filename_override,
                                           minio_bucket=minio_bucket,
                                           data_classification=data_classification,
                                           )
        if not result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
            json_dict = json.load(open(temp_data_file.name, "r"))

            return json_dict


def get_xml_list_dfs(site, list_name, list_backup_suffix, data_classification):
    access_timestamp = pandas.Timestamp.now(tz="Africa/Johannesburg")
    xml_list = site.List(list_name).GetListItems()
    xml_df = pandas.DataFrame(xml_list)

    if xml_df.shape[0] == 0:
        logging.warning(f"XML list is empty, returning None")
        return None
    else:
        logging.debug(f"Got {xml_df.shape[0]} XML entries")

    logging.debug(f"Setting '{SOURCE_COL_NAME}'='URL Path', '{ACCESS_COL_NAME}'={access_timestamp}")

    xml_df[SOURCE_COL_NAME] = xml_df[XML_URL_COL_NAME].str.extract(
        SP_REGEX, expand=False
    ).apply(
        lambda file_uri: urllib.parse.urljoin(SP_DOMAIN, file_uri)
    )

    # fix created date formatting
    xml_df[CREATED_DATE] = xml_df[CREATED_DATE].astype(str)
    xml_df[CREATED_DATE] = xml_df[CREATED_DATE].str.extract(SP_REGEX, expand=False)
    xml_df[ID_COL_NAME] = xml_df[ID_COL_NAME].str.extract(ID_REGEX, expand=False)

    # add accessed timestamp
    xml_df[ACCESS_COL_NAME] = access_timestamp

    # stop to_json changing date format
    for col in DATE_COLS:
        if col in xml_df.columns:
            xml_df[col] = pandas.to_datetime(xml_df[col], format=VAX_DATE_FORMAT)
            xml_df[col] = xml_df[col].dt.strftime(VAX_DATE_FORMAT)

    # Backing up the sharepoint list into Minio
    logging.info(f"back[ing] up {list_name} to minio")
    out_list_name = list_name.replace(" ", "_")

    # Backing up the XML rows into Minio
    with tempfile.TemporaryDirectory() as tempdir:
        xml_df_dict_list = xml_df.to_dict(orient='records')
        for xml_row_dict in xml_df_dict_list:
            unique_id = xml_row_dict[ID_COL_NAME]

            logging.debug(f"Backing up '{unique_id}.json' to Minio...")
            local_path = os.path.join(tempdir, unique_id)
            with open(local_path, "w") as json_file:
                json.dump(xml_row_dict, json_file)

            result = minio_utils.file_to_minio(
                filename=local_path,
                filename_prefix_override=list_backup_suffix,
                minio_bucket=COVID_BUCKET,
                data_classification=data_classification,
            )
            if not result:
                logging.error(f"Failed to backup row {unique_id} as json to minio")
                sys.exit(-1)

    logging.info(f"back[ed] up {list_name} to minio")

    return True


def collect_json_blobs(bucket, prefix, data_classification):
    """Get a list of all the json blov files in the bucket"""
    bucket_file_list = minio_utils.list_objects_in_bucket(
        minio_bucket=bucket,
        minio_prefix_override=prefix,
        data_classification=data_classification,
    )
    return bucket_file_list


def backup_to_full_df(bucket, prefix, data_classification):
    """
    Get all json backup entries and populate the full dataframe
    """
    json_blob_list = collect_json_blobs(
        bucket=bucket,
        prefix=prefix,
        data_classification=data_classification
    )

    part_dict_gen = (
        minio_json_to_dict(
            minio_filename_override=filename,
            minio_bucket=bucket,
            data_classification=EDGE_CLASSIFICATION,
        )
        for filename in json_blob_list
    )

    full_df = pandas.DataFrame(part_dict_gen)

    return full_df.reset_index()


def fix_sp_formatted_cols(df, columns_list, regex_pattern):
    df_copy = df.copy()
    re_pattern = re.compile(regex_pattern)
    for col_name in columns_list:
        df_copy[col_name] = df_copy[col_name].str.extract(re_pattern, expand=False)

    return df_copy


def unset_proxy():
    for proxy in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
        os.environ.pop(proxy)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # secrets env
    SECRETS_PATH_VAR = "SECRETS_PATH"

    # Loading secrets
    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ[SECRETS_PATH_VAR]
    secrets = json.load(open(secrets_path))

    # ----------------------------------
    logging.info("Fetch[ing] in auths")
    # get auths
    sp_auth, city_proxy_string, city_proxy_dict = sharepoint_utils.get_auth_objects(
        secrets["proxy"]["username"],
        secrets["proxy"]["password"]
    )

    # set proxy
    sharepoint_utils.set_env_proxy(city_proxy_string)
    logging.info("Fetch[ed] in auths")

    logging.info("Connect[ing] to SharePoint site")
    sp_site = sharepoint_utils.get_sp_site(SP_DOMAIN, SP_SITE, sp_auth)
    logging.info("Connect[ed] to SharePoint site")

    # ----------------------------------
    # get staff list df
    logging.info(f"Fetch[ing] {SP_STAFF_LIST_NAME}")
    staff_list_backup = get_xml_list_dfs(
        sp_site, SP_STAFF_LIST_NAME, STAFF_LIST_BAK,
        data_classification=EDGE_CLASSIFICATION
    )
    logging.info(f"Fetch[ed] {SP_STAFF_LIST_NAME}")
    logging.info(f"Back[ing] up the full df for {SP_STAFF_LIST_NAME}")
    staff_list_df = backup_to_full_df(
        bucket=COVID_BUCKET, prefix=STAFF_LIST_BAK,
        data_classification=EDGE_CLASSIFICATION
    )
    if staff_list_df.empty:
        logging.error(f"empty dataframe for sharepoint list {SP_STAFF_LIST_NAME}")
        sys.exit(-1)
    logging.info(f"Back[ed] up the full df for {SP_STAFF_LIST_NAME}")

    # fix sharepoint formatting
    logging.info(f"Formatt[ing] columns")
    staff_list_df_fixed = fix_sp_formatted_cols(staff_list_df, [STAFF_MERGE_STR], STAFF_REGEX)
    staff_list_df_fixed = fix_sp_formatted_cols(staff_list_df_fixed, [ID_COL_NAME], SP_REGEX)

    # ----------------------------------
    # get vaccine register df
    logging.info(f"Fetch[ing] {SP_VACCINE_REGISTER}")
    staff_vaccine_backup = get_xml_list_dfs(
        sp_site, SP_VACCINE_REGISTER, VACC_LIST_BAK,
        data_classification=EDGE_CLASSIFICATION
    )
    staff_vaccine_register_df = backup_to_full_df(
        bucket=COVID_BUCKET, prefix=VACC_LIST_BAK,
        data_classification=EDGE_CLASSIFICATION
    )
    if staff_vaccine_register_df.empty:
        logging.error(f"empty dataframe for sharepoint list {SP_VACCINE_REGISTER}")
        sys.exit(-1)
    logging.info(f"Fetch[ed] {SP_VACCINE_REGISTER}")

    # fix sharepoint formatting
    logging.info(f"Formatt[ing] columns")
    staff_vaccine_register_df_fixed = fix_sp_formatted_cols(staff_vaccine_register_df, [VAX_MERGE_STR], SP_REGEX)
    logging.info(f"Formatt[ed] columns")

    # ----------------------------------
    # get sequencing survey
    logging.info(f"Fetch[ing] {SP_SEQUENCING_SURVEY}")
    staff_seq_survey_backckup = get_xml_list_dfs(
        sp_site, SP_SEQUENCING_SURVEY, SEQ_LIST_BAK,
        data_classification=EDGE_CLASSIFICATION
    )
    staff_seq_survey_df = backup_to_full_df(
        bucket=COVID_BUCKET, prefix=SEQ_LIST_BAK,
        data_classification=EDGE_CLASSIFICATION
    )
    if staff_seq_survey_df.empty:
        logging.error(f"empty dataframe for sharepoint list {SP_SEQUENCING_SURVEY}")
        sys.exit(-1)

    # fix sharepoint formatting
    logging.info(f"Formatt[ing] columns")
    staff_seq_survey_df_fixed = fix_sp_formatted_cols(staff_seq_survey_df, [VAX_MERGE_STR], SP_REGEX)
    logging.info(f"Formatt[ed] columns")

    # unset the proxies set by Sharepoint utils
    unset_proxy()

    # write to minio
    logging.info(f"Push[ing] data to mino")
    for filename, df in [
        (STAFF_LIST_PREFIX, staff_list_df_fixed),
        (RAW_VAX_REGISTER_PREFIX, staff_vaccine_register_df_fixed),
        (SEQ_SURVEY_PREFIX, staff_seq_survey_df_fixed)
    ]:

        minio_utils.dataframe_to_minio(
            df,
            COVID_BUCKET,
            secrets["minio"]["edge"]["access"],
            secrets["minio"]["edge"]["secret"],
            EDGE_CLASSIFICATION,
            filename_prefix_override=f"{VACCINE_PREFIX_RAW}{filename}",
            file_format="parquet",
            data_versioning=False

        )
    logging.info(f"Push[ed] data to mino")

    logging.info("...Done!")
