# base imports

import json
import logging
import os
import sys
# external imports
from db_utils import minio_utils
from vaccine_data_to_minio import COVID_BUCKET, EDGE_CLASSIFICATION, CREATED_DATE, VAX_DATE
from vaccine_sequencing_munge import (OUTPUT_PREFIX_ANN, OUTPUT_PREFIX_AGG, DIRECTORATE_COL, DEPARTMENT_COL, STAFF_NO,
                                      SUBDISTRICT, STAFF_TYPE, BRANCH, SECTION, ORG_UNIT, FACILITY, POSITION, GENDER,
                                      minio_to_df, PARQUET_READER)
from vaccine_annotate_non_HR_munge import VACCINE_REGISTER_ANNOT_NON_STAFF
from vaccine_annotate_HR_munge import SIDE_EFFECT, VAX_TYPE, DOSE_NO, J_AND_J_VACC


# input settings
CITY_VAX_REGISTER = f"{OUTPUT_PREFIX_ANN}{VACCINE_REGISTER_ANNOT_NON_STAFF}.parquet"

# outfile
VACCINE_REGISTER_AGG = "staff-vaccination-register-aggregated"

# vaccine register list cols
COUNT = "count"

PRIMARY_GRP_COLS = [
    VAX_DATE, VAX_TYPE, DOSE_NO,
    STAFF_TYPE, DIRECTORATE_COL, DEPARTMENT_COL, BRANCH, SUBDISTRICT, SECTION, ORG_UNIT, FACILITY, POSITION, GENDER,
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
    logging.info("Fetch[ing] source dataframe")
    staff_vaccine_register_df = minio_to_df(
        minio_filename_override=CITY_VAX_REGISTER,
        minio_bucket=COVID_BUCKET,
        data_classification=EDGE_CLASSIFICATION,
        reader=PARQUET_READER
    )

    # ----------------------------------
    logging.info("Aggregat[ing] master vaccine register data")
    vaccine_register_agg_df = staff_vaccine_register_df.groupby(PRIMARY_GRP_COLS, dropna=False).agg(
            **{COUNT: (CREATED_DATE, "count")},
        ).reset_index()
    logging.info("Aggregat[ing] master vaccine register data")
     
    # ----------------------------------
    logging.info(f"Push[ing] data to mino")
    minio_utils.dataframe_to_minio(
            vaccine_register_agg_df, 
            COVID_BUCKET,
            secrets["minio"]["edge"]["access"], 
            secrets["minio"]["edge"]["secret"],
            EDGE_CLASSIFICATION,
            filename_prefix_override=f"{OUTPUT_PREFIX_AGG}{VACCINE_REGISTER_AGG}",
            file_format="parquet",
            data_versioning=False
        )
    logging.info(f"Push[ed] data to mino")
    
    logging.info("...Done!")
