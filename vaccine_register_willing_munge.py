"""
Aggregate the vaccine register using only sequenced and willing staff
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
from vaccine_data_to_minio import (COVID_BUCKET, EDGE_CLASSIFICATION, VAX_DATE, VAX_MERGE_STR, CREATED_DATE)
from vaccine_sequencing_munge import (OUTPUT_PREFIX_ANN, OUTPUT_PREFIX_AGG, VACCINE_SEQ_ANN, DIRECTORATE_COL,
                                      DEPARTMENT_COL, SUBDISTRICT, STAFF_TYPE, BRANCH, SECTION, ORG_UNIT, FACILITY,
                                      POSITION, GENDER, Q1, Q2, RISK_SCORE, minio_to_df, PARQUET_READER)
from vaccine_annotate_non_HR_munge import VACCINE_REGISTER_ANNOT_NON_STAFF
from vaccine_annotate_HR_munge import SIDE_EFFECT, VAX_TYPE, DOSE_NO
from vaccine_register_munge import COUNT

# input settings
CITY_VAX_REGISTER = f"{OUTPUT_PREFIX_ANN}{VACCINE_REGISTER_ANNOT_NON_STAFF}.parquet"
SEQUENCING_ANNOTATED = f"{OUTPUT_PREFIX_ANN}{VACCINE_SEQ_ANN}.parquet"

# outfile
VACCINE_REGISTER_AGG_WILLING = "staff-vaccination-register-aggregated-willing"

# vaccine register list cols
NOT_SEQ = "Not_sequenced"

PRIMARY_GRP_COLS = [
    VAX_DATE, VAX_TYPE, DOSE_NO, STAFF_TYPE, DIRECTORATE_COL, DEPARTMENT_COL, BRANCH, SUBDISTRICT, SECTION, ORG_UNIT, FACILITY,
    POSITION, GENDER, Q1, Q2, RISK_SCORE
]


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
        SEQUENCING_ANNOTATED,
        CITY_VAX_REGISTER,
    ]:
        df = minio_to_df(
            minio_filename_override=filename_override,
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=PARQUET_READER
        )
        vax_register_dfs.append(df)

    # dataframes
    annotated_sequencing_df, staff_vaccine_register_df = vax_register_dfs

    # ----------------------------------
    # annotate vaccine register with seq survey
    logging.info(f"Merg[ing] Staff annotations onto vaccine register")
    register_annotated_risk = pd.merge(
        staff_vaccine_register_df,
        annotated_sequencing_df[[VAX_MERGE_STR, Q1, Q2, RISK_SCORE]],
        how="left",
        on=VAX_MERGE_STR,
        validate="m:1"
    )
    logging.info(f"Merg[ed] Staff annotations onto vaccine register")

    # ----------------------------------
    logging.info("Aggregat[ing] master vaccine register data")
    vaccine_register_agg_df = register_annotated_risk.groupby(PRIMARY_GRP_COLS, dropna=False).agg(
        **{COUNT: (CREATED_DATE, "count")},
    ).reset_index()

    for col in [RISK_SCORE, Q1, Q2]:
        vaccine_register_agg_df[col] = vaccine_register_agg_df[col].astype(str)
    logging.info("Aggregat[ing] master vaccine register data")


    # ----------------------------------
    logging.info(f"Push[ing] data to mino")
    minio_utils.dataframe_to_minio(
        vaccine_register_agg_df,
        COVID_BUCKET,
        secrets["minio"]["edge"]["access"],
        secrets["minio"]["edge"]["secret"],
        EDGE_CLASSIFICATION,
        filename_prefix_override=f"{OUTPUT_PREFIX_AGG}{VACCINE_REGISTER_AGG_WILLING}",
        file_format="parquet",
        data_versioning=False
    )
    logging.info(f"Push[ed] data to mino")

    logging.info("...Done!")
