"""
Script to annotate vaccination register with details for non-metro staff
"""

# base imports
import json
import logging
import os
import sys
# external imports
from db_utils import minio_utils
import pandas as pd
# local imports
from vaccine_data_to_minio import COVID_BUCKET, EDGE_CLASSIFICATION, STAFF_MERGE_STR, VAX_MERGE_STR
from vaccine_sequencing_munge import (OUTPUT_PREFIX_ANN, UNIQUE_STAFF_LIST, FIX_HR_ATRIBUTE_COLS, HR_DATA_TAG,
                                      STAFF_LIST_TAG, STAFF_NO, minio_to_df, PARQUET_READER, fix_attribute_cols)
from vaccine_annotate_HR_munge import VACCINE_REGISTER_ANNOTATED_HR

# input settings
UNIQUE_STAFF_LIST = f"{UNIQUE_STAFF_LIST}.parquet"
CITY_VAX_REGISTER = f"{VACCINE_REGISTER_ANNOTATED_HR}.parquet"

# outfile
VACCINE_REGISTER_ANNOT_NON_STAFF = "staff-vaccination-register-annotated-plus-non-staff"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # secrets env
    SECRETS_PATH_VAR = "SECRETS_PATH"

    # Loading secrets
    logging.info("Fetch[ing] secrets")
    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ[SECRETS_PATH_VAR]
    secrets = json.load(open(secrets_path))
    logging.info("Fetch[ed] secrets")

    # ----------------------------------
    logging.info("Fetch[ing] source dataframes")
    vax_register_dfs = []
    for filename_override in [
        f"{OUTPUT_PREFIX_ANN}{CITY_VAX_REGISTER}",
        f"{OUTPUT_PREFIX_ANN}{UNIQUE_STAFF_LIST}"
    ]:
        df = minio_to_df(
            minio_filename_override=filename_override,
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=PARQUET_READER
        )
        vax_register_dfs.append(df)

    # dataframes
    staff_vaccine_register_df, staff_list_df = vax_register_dfs
    logging.info("Fetch[ed] source dataframes")

    # ----------------------------------
    # annotate vaccine register with staff details
    logging.info(f"Merg[ing] Staff annotations onto vaccine register")
    register_annotated = pd.merge(
        staff_list_df,
        staff_vaccine_register_df,
        how="right",
        left_on=STAFF_MERGE_STR,
        right_on=VAX_MERGE_STR,
        suffixes=[STAFF_LIST_TAG, HR_DATA_TAG],
        validate="1:m"
    )
    logging.info(f"Merg[ed] Staff annotations onto vaccine register")

    # ----------------------------------
    logging.info("Merg[ing] health staff list attributes where existing ones are null")
    register_annotated_cleaned = fix_attribute_cols(
        register_annotated, FIX_HR_ATRIBUTE_COLS + [STAFF_NO], STAFF_LIST_TAG, HR_DATA_TAG
    )
    logging.info("Merg[ed]health staff list attributes where existing ones are null")

    # ----------------------------------
    logging.info(f"Push[ing] data to mino")
    minio_utils.dataframe_to_minio(
        register_annotated_cleaned,
        COVID_BUCKET,
        secrets["minio"]["edge"]["access"],
        secrets["minio"]["edge"]["secret"],
        EDGE_CLASSIFICATION,
        filename_prefix_override=f"{OUTPUT_PREFIX_ANN}{VACCINE_REGISTER_ANNOT_NON_STAFF}",
        file_format="parquet",
        data_versioning=False

    )
    logging.info(f"Push[ed] data to mino")

    logging.info("...Done!")
