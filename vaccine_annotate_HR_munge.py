"""
Script to aggregate on the staff vaccination register
"""

# base imports
from datetime import datetime
from collections import defaultdict
import json
import logging
import os
import re
import sys
# external imports
from db_utils import minio_utils
import numpy as np
import pandas as pd
# local imports
from vaccine_data_to_minio import (COVID_BUCKET, EDGE_CLASSIFICATION, VACCINE_PREFIX_RAW, VAX_MERGE_STR, CREATED_DATE,
                                   VAX_DATE, RAW_VAX_REGISTER_PREFIX)
from vaccine_sequencing_munge import (OUTPUT_PREFIX_ANN, STAFF_NO, EMPTY_VAX_MERGE_STR, NULL_MERGE_VAL, ID_COL_NAME,
                                      HR_MASTER_DATA, minio_to_df, CSV_READER, PARQUET_READER, HR_KEEP_COLS)

# input settings
CITY_VAX_REGISTER = f"{RAW_VAX_REGISTER_PREFIX}.parquet"

# outfile
VACCINE_REGISTER_ANNOTATED_HR = "staff-vaccination-register-annotated"

VAX_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
START_DATE_FILT = '2021-02-23'
TODAY = str(datetime.today())
STAFF_ID_REGEX = re.compile(r"^\w.*; (\d+|NA), .*$")

# vaccine register list cols
ACCESS_COL_NAME = "AccessTimestamp"
CREATED_BY = "Created By"

SIDE_EFFECT = "Side Effects"
J_AND_J_VACC = 'Johnson & Johnson'

VAX_TYPE = "Vaccine type"
DOSE_NO = "Dose received"
EMPLOYEE_NAME = "Employee"
HR_KEEP_COLS_EXTRA = HR_KEEP_COLS + [EMPLOYEE_NAME]
VAX_KEEP_COLS = [
    VAX_DATE, VAX_TYPE, DOSE_NO, SIDE_EFFECT, CREATED_BY, CREATED_DATE, ACCESS_COL_NAME, VAX_MERGE_STR, ID_COL_NAME
]


def validate_vaccines(vacc_df):
    vacc_df[STAFF_NO].replace("NA", np.nan, inplace=True)
    vacc_df[STAFF_NO].replace("nan", np.nan, inplace=True)
    personno = vacc_df[STAFF_NO].to_list()
    vacc_type = vacc_df[VAX_TYPE].to_list()
    dose_no = vacc_df[DOSE_NO].to_list()

    dose_validateion_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    for (staffno, vacc_ty, dose) in zip(personno, vacc_type, dose_no):
        dose_validateion_dict[staffno][vacc_ty][dose] += 1

    duplicate_staff_dose = []
    for staff, vacc_type_dict in dose_validateion_dict.items():
        if len(vacc_type_dict) > 1:
            logging.error(f"multiple vaccine types detected for person no {staff}")
            sys.exit(-1)
        for vacc_type, dose_dict in vacc_type_dict.items():
            if vacc_type == J_AND_J_VACC and len(dose_dict) > 1:
                logging.error(f"multiple doses for J&J, expected only one dose")
                sys.exit(-1)
            for dose, count in dose_dict.items():
                if count > 1 and pd.isna(staff):
                    logging.warning(f"{count} counts for {vacc_type} dose {dose} with no staff ID")
                elif count > 1 and pd.notna(staff):
                    logging.warning(f"multiple entries ({count}) for the same dose ({dose}) for StaffNo {staff}")
                    duplicate_staff_dose.append(staff)

                if dose == "2nd Dose" and "1st Dose" not in dose_dict.keys():
                    logging.error(f"2nd Dose recorded without 1st Dose")
                    sys.exit(-1)

    return duplicate_staff_dose


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
    for (filename_override, reader) in [
        (f"{VACCINE_PREFIX_RAW}{CITY_VAX_REGISTER}", PARQUET_READER),
        (HR_MASTER_DATA, CSV_READER)
    ]:
        df = minio_to_df(
            minio_filename_override=filename_override,
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=reader
        )
        vax_register_dfs.append(df)

    # unpack dataframes
    staff_vaccine_register_df_fixed, staff_list_df = vax_register_dfs
    logging.info("Fetch[ed] source dataframes")

    # process vaccine register
    # drop empty rows
    logging.info(f"Dropp[ing] empty rows")
    staff_vaccine_register_df_fixed_filt = staff_vaccine_register_df_fixed.query(
        f"`{VAX_MERGE_STR}` != @EMPTY_VAX_MERGE_STR "
        f"and `{VAX_MERGE_STR}` != @NULL_MERGE_VAL "
        f"and `{VAX_MERGE_STR}`.notna()"
    ).copy()
    logging.warning(
        f"{len(staff_vaccine_register_df_fixed) - len(staff_vaccine_register_df_fixed_filt)} "
        f"rows dropped as null for the {VAX_MERGE_STR} merge column\n{len(staff_vaccine_register_df_fixed_filt)} records remaining")
    logging.info(f"Dropp[ed] empty rows")

    # filter to desired columns
    logging.info(f"Filter[ing] to selected columns")
    staff_vaccine_register_df_cols_filt = staff_vaccine_register_df_fixed_filt[VAX_KEEP_COLS].copy()
    logging.info(f"Filter[ed] to selected columns")

    # remove wrong dates
    logging.info("Dropp[ing] bad dates")
    staff_vaccine_register_df_date_filt = staff_vaccine_register_df_cols_filt.query(
        f"@START_DATE_FILT <= `{VAX_DATE}` <= @TODAY").copy()
    if staff_vaccine_register_df_date_filt.empty:
        logging.error("Empty dataframe for vaccine register after date filter")
        sys.exit(-1)

    bad_dates = sorted(
        staff_vaccine_register_df_cols_filt.query(f"`{VAX_DATE}` < @START_DATE_FILT or `{VAX_DATE}` > @TODAY")[
            VAX_DATE].unique())
    logging.warning(
        f"{len(staff_vaccine_register_df_cols_filt) - len(staff_vaccine_register_df_date_filt)} "
        f"rows dropped as out of date range")
    logging.warning(f"Bad dates identified {bad_dates}\n{len(staff_vaccine_register_df_date_filt)} records remaining")
    logging.info("Dropp[ing] bad dates")

    # dedup
    logging.info(f"Filter[ing] out duplicate staff entries")
    staff_vaccine_register_df_date_filt_unique = staff_vaccine_register_df_date_filt.sort_values(
        CREATED_DATE, ascending=True).drop_duplicates(
        subset=[VAX_MERGE_STR, VAX_TYPE, DOSE_NO], keep="last"
    )
    logging.warning(
        f"{len(staff_vaccine_register_df_date_filt) - len(staff_vaccine_register_df_date_filt_unique)} "
        f"rows dropped as duplicate staff entries\n{len(staff_vaccine_register_df_date_filt_unique)} records remaining")
    logging.info(f"Filter[ing] out duplicate staff entries")

    # ----------------------------------
    # get staff number
    logging.info("Extract[ing] staff number from vaccine register")
    staff_vaccine_register_df_date_filt_unique[STAFF_NO] = (
        staff_vaccine_register_df_date_filt_unique[VAX_MERGE_STR].
        str.extract(STAFF_ID_REGEX, expand=False)
    )
    logging.info("Extract[ed] staff number from vaccine register")

    # convert both dfs staff no col to str for merge
    logging.info("Convert[ing] staff number to str for merge")
    staff_vaccine_register_df_date_filt_unique[STAFF_NO] = staff_vaccine_register_df_date_filt_unique[STAFF_NO].astype(
        str)
    staff_list_df[STAFF_NO] = staff_list_df[STAFF_NO].astype(str)
    logging.info("Convert[ed] staff number to str for merge")

    # ----------------------------------
    # annotate vaccine register with staff details
    logging.info(f"Merg[ing] Staff annotations onto vaccine register")
    register_annotated = pd.merge(
        staff_list_df[HR_KEEP_COLS_EXTRA],
        staff_vaccine_register_df_date_filt_unique,
        how="right",
        on=STAFF_NO,
        validate="1:m"
    )
    logging.info(f"Merg[ed] Staff annotations onto vaccine register")

    # ----------------------------------
    # validate the vaccine entries
    logging.info("Validat[ing] vaccination entries")
    staff_duplicates = validate_vaccines(register_annotated)
    if staff_duplicates:
        logging.warning(f"{len(staff_duplicates)} Duplicate entries for [staff vaccine type and dose]\nDropping them")
        # remove the staff with duplicates
        register_annotated_dedup = register_annotated.query(f"{STAFF_NO} not in (@staff_duplicates)").copy()
        # select only staff with duplicates (to prevent removal of non-staff members
        staff_duplicates_df = register_annotated.query(f"{STAFF_NO}.isin(@staff_duplicates)").copy()
        # drop the duplicates
        staff_duplicates_df.drop_duplicates(
            subset=[STAFF_NO, VAX_TYPE, DOSE_NO], keep="last", inplace=True)
        # recombine the two dataframes
        register_annotated_cln = pd.concat([register_annotated_dedup, staff_duplicates_df])
    else:
        register_annotated_cln = register_annotated.copy()
    logging.info("Validat[ed] vaccination entries")

    # ----------------------------------
    logging.info(f"Push[ing] data to mino")
    minio_utils.dataframe_to_minio(
        register_annotated_cln,
        COVID_BUCKET,
        secrets["minio"]["edge"]["access"],
        secrets["minio"]["edge"]["secret"],
        EDGE_CLASSIFICATION,
        filename_prefix_override=f"{OUTPUT_PREFIX_ANN}{VACCINE_REGISTER_ANNOTATED_HR}",
        file_format="parquet",
        data_versioning=False

    )
    logging.info(f"Push[ed] data to mino")

    logging.info("...Done!")
