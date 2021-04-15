# base imports
import json
import logging
import os
import sys
import tempfile
# external imports
from db_utils import minio_utils
import pandas as pd
import numpy as np

from hr_master_data_munge import HR_MASTER_FILENAME_PATH, DIRECTORATE_COL, DEPARTMENT_COL
from vaccine_data_to_minio import (COVID_BUCKET, EDGE_CLASSIFICATION, VACCINE_PREFIX_RAW, SEQ_SURVEY_PREFIX,
                                   SP_STAFF_LIST_NAME, STAFF_MERGE_STR, VAX_MERGE_STR)

# input settings
CSV_READER = "csv"
PARQUET_READER = "parquet"
PRIVATE_PREFIX = "data/private/"

OUTPUT_PREFIX_ANN = f"{PRIVATE_PREFIX}staff_vaccine/annotations/"
OUTPUT_PREFIX_AGG = f"{PRIVATE_PREFIX}staff_vaccine/aggregations/"

STAFF_LIST = f"{VACCINE_PREFIX_RAW}{SP_STAFF_LIST_NAME}.parquet"
CITY_SEQ_SURVEY = f"{VACCINE_PREFIX_RAW}{SEQ_SURVEY_PREFIX}.parquet"
HR_MASTER_DATA = f"{HR_MASTER_FILENAME_PATH}.csv"

# outfile
VACCINE_SEQ_ANN = "staff-sequencing-annotation"

UNIQUE_STAFF_LIST = "staff-list-unique"
UNIQUE_STAFF_LIST_ANN = f"{UNIQUE_STAFF_LIST}-annotation"
EMPTY_VAX_MERGE_STR = ", ; , , , "
NULL_MERGE_VAL = ", SELECT STAFF MEMBER FROM THE DROPDOWN MENU; , , , "
ID_COL_NAME = 'Unique Id'

# hr cols
HR_DATA_TAG = "_HR"
STAFF_LIST_TAG = "_staff"

# staff list cols 
SUBDISTRICT = "subdistrict"
ALT_GENDER = "Unspecified"
GENDER_DICT = {
    "Female": "Female",
    "Make": "Male",
    "Male": "Male",
    "N/A": ALT_GENDER,
    "NA": ALT_GENDER,
    None: ALT_GENDER,
    np.nan: ALT_GENDER
}


STAFF_TYPE = "staff_type"
BRANCH = "Branch"
SECTION = "Section"
ORG_UNIT = "Org Unit Name"
ORG_UNIT_NO = "Orgunit"
FACILITY = "facility"
POSITION = "Position"
STAFF_NO = "Persno"
ID_NO = "ID number"
FIRST_NAME = "First name"
LAST_NAME = "Last name"
GENDER = "Gender Key"

STAFF_LIST_EDIT_COLS = [STAFF_MERGE_STR]
STAFF_LIST_KEEP_COLS = [
    STAFF_TYPE, BRANCH, SECTION, ORG_UNIT, FACILITY, POSITION, STAFF_NO, FIRST_NAME, LAST_NAME, GENDER, SUBDISTRICT,
    STAFF_MERGE_STR
]
HR_KEEP_COLS = [STAFF_NO, DIRECTORATE_COL, DEPARTMENT_COL, BRANCH, SECTION, ORG_UNIT, ORG_UNIT_NO, POSITION]
FIX_HR_ATRIBUTE_COLS = [BRANCH, SECTION, ORG_UNIT, POSITION]

# vaccine sequencing list cols
MODIFIED_DATE = "Modified"
MODIFIED_BY = "Modified By"

CREATED_BY = "Created By"
RISK_SCORE = "risk_score"

Q1 = "1. Is the staff member willing to be vaccinated against COVID-19?"
Q2 = "2. Is the staff member willing to take part in the Sisonke vaccination program?"
Q3 = "3. Is the staff member more than 45 years old?"
Q4 = "4. Does the staff member have medical co-morbidities?"
Q5 = "5. Is the staff member patient-facing more than 50% of the time?"
Q6 = "6. Is the staff member working with COVID PUIs? (PUIs = persons under investigation)"
Q7 = "7. Has the staff member received a voucher code from the EVDS/Sisonke system?"

YES = "Yes"
NO = "No"

SCORE_Q = [Q3, Q4, Q5, Q6]
SEQ_KEEP_COLS = [MODIFIED_DATE, VAX_MERGE_STR, Q1, Q2, Q3, Q4, Q5, Q6, Q7, ID_COL_NAME]

STATUS_MAP = {
    YES: 1,
    NO: 0
}


def minio_to_df(minio_filename_override, minio_bucket, data_classification, reader="csv"):
    logging.debug("Pulling data from Minio bucket...")
    if reader == "csv":
        file_reader = pd.read_csv
    elif reader == "parquet":
        file_reader = pd.read_parquet
    else:
        logging.error("reader is not 'csv' or 'parquet")
        sys.exit(-1)

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
            df = file_reader(temp_data_file.name)
            
            return df


def fix_attribute_cols(df, columns_to_fix, left_tag=STAFF_LIST_TAG, right_tag=HR_DATA_TAG):
    """
    Replace values in right merge column with values from left merge column if right == nan
    Drops the left merge columns and Renames the right merge columns to remove the merge suffix
    Args:
        df (pd.DataFrame): the merge df with suffix's appended to overlapping cols
        columns_to_fix (list): the list of overlapping column names (base name only) to merge values on
        left_tag (str): the suffix appended to the left df column name after merge
        right_tag (str): the suffix appended to the right df column name after merge

    Returns:
        (pd.DataFrame): edited copy of the original dataframe
    """
    edit_df = df.copy()
    for column in columns_to_fix:
        edit_df[column + right_tag] = np.where(
                (edit_df[column + right_tag].isna()),  # Identifies the conditional
                edit_df[column + left_tag],  # Set the column value to be inserted
                edit_df[column + right_tag]  # Set the column to be affected
            )
        edit_df.drop(columns=[column + left_tag], inplace=True)
        edit_df.rename(columns={column + right_tag: column}, inplace=True)

    return edit_df


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
    vaccine_dfs = []
    for (filename, reader) in [(HR_MASTER_DATA, CSV_READER),
                           (STAFF_LIST, PARQUET_READER),
                           (CITY_SEQ_SURVEY, PARQUET_READER)
                           ]:
        
        df = minio_to_df(
            minio_filename_override=filename,
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=reader
        )
        vaccine_dfs.append(df)

    # unpack the dfs
    hr_data, staff_list_df, staff_vaccine_seq_df = vaccine_dfs
    
    # ----------------------------------
    # clean up the staff list
    staff_list_df_fixed = staff_list_df.query(
        f"`{STAFF_MERGE_STR}` != @EMPTY_VAX_MERGE_STR "
        f"and `{STAFF_MERGE_STR}` != @NULL_MERGE_VAL "
        f"and `{STAFF_MERGE_STR}`.notna()"
    ).copy()
    # filter to desired columns
    logging.info(f"Filter[ing] to selected columns")
    staff_list_df_fixed_filt = staff_list_df_fixed[STAFF_LIST_KEEP_COLS].dropna(
        subset=["Last name", "First name"]
    ).copy()
    logging.info(f"Filter[ed] to selected columns")
    # dedup
    logging.info(f"Filter[ing] out duplicate staff entries")
    staff_list_df_fixed_filt_unique = staff_list_df_fixed_filt.drop_duplicates(
        subset=[STAFF_MERGE_STR], keep="last"
    ).copy()
    logging.info(f"Filter[ed] out duplicate staff entries")
    # fix gender col values
    if not all([entry in GENDER_DICT.keys() for entry in staff_list_df_fixed_filt_unique[GENDER].unique()]):
        logging.error("Detected gender entry that is not in the mapping dict\n"
                      f"{staff_list_df_fixed_filt_unique[GENDER].unique()}")
        sys.exit(-1)
    staff_list_df_fixed_filt_unique[GENDER] = staff_list_df_fixed_filt_unique[GENDER].map(GENDER_DICT)
    logging.warning(f"{len(staff_list_df) - len(staff_list_df_fixed_filt_unique)} "
                    f"rows dropped from staff list\n{len(staff_list_df_fixed_filt_unique)} records remaining")
    
    # ----------------------------------
    logging.info(f"Convert[ing] staff numbers to string for merge")
    staff_list_df_fixed_filt_unique[STAFF_NO] = staff_list_df_fixed_filt_unique[STAFF_NO].astype(str)
    hr_data[STAFF_NO] = hr_data[STAFF_NO].astype(str)
    logging.info(f"Convert[ed] staff numbers to string for merge")
    
    # ----------------------------------
    # annotate staff list with HR staff details
    logging.info(f"Merg[ing] HR master data onto Staff annotations")
    annotated_staff_list_df = pd.merge(
        staff_list_df_fixed_filt_unique, 
        hr_data[HR_KEEP_COLS], 
        how="left",
        on=STAFF_NO,
        suffixes=[STAFF_LIST_TAG, HR_DATA_TAG],
        validate="m:1",
    )
    logging.info(f"Merg[ed] HR master data onto Staff annotations")
    
    # Use HR annotations unless they are missing (not CoCT staff), then use staff list annotations
    logging.info(f"Keep[ing] HR master annotations unless null (not CoCT staff), then revert to staff list annotations")
    annotated_staff_list_df = fix_attribute_cols(
        annotated_staff_list_df,
        FIX_HR_ATRIBUTE_COLS,
        STAFF_LIST_TAG,
        HR_DATA_TAG
    )
    logging.info(f"Kep[t] HR master annotations unless null (not CoCT staff), then revert to staff list annotations")

    # ----------------------------------
    # clean up the sequencing form
    logging.info(f"Filter[ing] out null entries")
    staff_vaccine_seq_df_filt = staff_vaccine_seq_df.query(
        f"`{VAX_MERGE_STR}` != @EMPTY_VAX_MERGE_STR and "
        f"`{VAX_MERGE_STR}` != @NULL_MERGE_VAL and "
        f"`{VAX_MERGE_STR}`.notna()"
    ).copy()
    logging.info(f"Filter[ed] out null entries")

    # filter to desired columns
    logging.info(f"Filter[ing] to selected columns")
    staff_vaccine_seq_df_filt_cols = staff_vaccine_seq_df_filt[SEQ_KEEP_COLS].copy()
    logging.info(f"Filter[ed] to selected columns")

    # dedup
    logging.info(f"Filter[ing] out duplicate staff entries")
    staff_vaccine_seq_df_unique = staff_vaccine_seq_df_filt_cols.sort_values(
        MODIFIED_DATE, ascending=True
    ).drop_duplicates(
        subset=[VAX_MERGE_STR], keep="last"
    )
    logging.warning(f"{len(staff_vaccine_seq_df_filt_cols) - len(staff_vaccine_seq_df_unique)} "
                    f"rows dropped as duplicate staff sequencing entries\n{len(staff_vaccine_seq_df_unique)} records remaining")
    logging.info(f"Filter[ed] out duplicate staff entries")
    
    # ----------------------------------
    for col in [Q1, Q2, Q3, Q4, Q5, Q6]:
        query_df = staff_vaccine_seq_df_unique.query(f"`{col}` not in(@STATUS_MAP)").copy()
        if not query_df.empty:
            logging.error(f"Unexpected value for question {col}")
            logging.debug(query_df[[VAX_MERGE_STR, col]])
            sys.exit(-1)
        logging.info(f"Reassign[ing] answers to numerical for {col}")
        staff_vaccine_seq_df_unique[col] = staff_vaccine_seq_df_unique[col].map(STATUS_MAP) 
        logging.info(f"Reassign[ed] answers to numerical for {col}")
    
    # ----------------------------------
    # annotate vaccine register with staff details
    logging.info(f"Merg[ing] Staff annotations onto vaccine register")
    annotated_sequencing_df = pd.merge(
        annotated_staff_list_df, 
        staff_vaccine_seq_df_unique, 
        how="right",
        left_on=STAFF_MERGE_STR,
        right_on=VAX_MERGE_STR,
        validate="1:1"
    )
    logging.info(f"Merg[ed] Staff annotations onto vaccine register")

    # ----------------------------------
    # calculate risk score
    logging.info("Calulat[ing] risk score")
    annotated_sequencing_df[RISK_SCORE] = annotated_sequencing_df[SCORE_Q].sum(axis=1)
    logging.info("Calulat[ed] risk score")

    # ----------------------------------
    logging.info(f"Push[ing] data to mino")
    for outfile, out_df, prefix in [
        (UNIQUE_STAFF_LIST, staff_list_df_fixed_filt_unique, OUTPUT_PREFIX_ANN),
        (UNIQUE_STAFF_LIST_ANN, annotated_staff_list_df, OUTPUT_PREFIX_ANN),
        (VACCINE_SEQ_ANN, annotated_sequencing_df, OUTPUT_PREFIX_ANN),
    ]:

        minio_utils.dataframe_to_minio(
                out_df, 
                COVID_BUCKET,
                secrets["minio"]["edge"]["access"], 
                secrets["minio"]["edge"]["secret"],
                EDGE_CLASSIFICATION,
                filename_prefix_override=f"{prefix}{outfile}",
                file_format="parquet",
                data_versioning=False

            )
    logging.info(f"Push[ing] data to mino")
    
    logging.info("...Done!")
