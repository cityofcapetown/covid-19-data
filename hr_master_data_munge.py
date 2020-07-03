import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import pandas

BUCKET = 'covid'
CLASSIFICATION = minio_utils.DataClassification.EDGE
HR_FORM_FILENAME_PATH = "data/private/hr_data_complete.csv"

HR_MASTER_INGESTION_FILENAME_PATH = "data/private/hr_data_all_staff.csv"
HR_MASTER_LOCATION_FILENAME_PATH = "data/private/city_people_locations.csv"

HR_MASTER_STAFFNUMBER = 'Persno'
HR_EXPECTED_STAFFNUMBER = 'StaffNumber'

ESSENTIAL_COL = "EssentialStaff"
ESSENTIAL_PERMIT_COL = "Essential Permit Issued"
ASSESSED_COL = "AssessedStaff"
HR_MASTER_FILENAME_PATH = "data/private/city_people"

DIRECTORATE_COL = 'Directorate'
DEPARTMENT_COL = 'Department'
NOT_ASSESSED_DIRECTORATES = {
    'ENERGY AND CLIMATE CHANGE',
    "SAFETY AND SECURITY",
    'COMMUNITY SERVICES and HEALTH',
}


def get_data_df(filename, minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_data_file:
        logging.debug("Pulling data from Minio bucket...")
        result = minio_utils.minio_to_file(
            temp_data_file.name,
            BUCKET,
            minio_access,
            minio_secret,
            CLASSIFICATION,
            minio_filename_override=filename
        )
        assert result

        logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
        data_df = pandas.read_csv(temp_data_file)

    return data_df


def merge_in_location_data(master_df, location_df):
    logging.debug(f"master_df.shape={master_df.shape}")
    employee_master_with_loc_df = master_df.merge(
        location_df,
        left_on=HR_MASTER_STAFFNUMBER,
        right_on=HR_EXPECTED_STAFFNUMBER,
        validate="one_to_one",
        how="left"
    )
    employee_master_with_loc_df[HR_EXPECTED_STAFFNUMBER] = employee_master_with_loc_df[HR_MASTER_STAFFNUMBER]
    logging.debug(f"employee_master_with_loc_df.shape={employee_master_with_loc_df.shape}")

    return employee_master_with_loc_df


def add_boolean_columns(master_df):
    master_df[ASSESSED_COL] = ~master_df[DIRECTORATE_COL].isin(NOT_ASSESSED_DIRECTORATES)
    logging.debug(
        f"master_df['{ASSESSED_COL}'].sum()/master_df.shape[0]={master_df[ASSESSED_COL].sum()}/{master_df.shape[0]}"
    )

    master_df[ESSENTIAL_COL] = master_df["Essential Permit Issued"] == 'YES'
    logging.debug(
        f"master_df['{ESSENTIAL_COL}'].sum()/master_df.shape[0]={master_df[ESSENTIAL_COL].sum()}/{master_df.shape[0]}"
    )

    return master_df


def validate_hr_data(master_df):
    assert master_df.shape[0] == master_df[HR_EXPECTED_STAFFNUMBER].nunique(), (
        f"master_df.shape[0]={master_df.shape[0]} vs "
        f"master_df[HR_MASTER_STAFFNUMBER].nunique()={master_df[HR_EXPECTED_STAFFNUMBER].nunique()}"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("Fetch[ing] HR master data")
    hr_master_ingestion_df = get_data_df(HR_MASTER_INGESTION_FILENAME_PATH,
                                         secrets["minio"]["edge"]["access"],
                                         secrets["minio"]["edge"]["secret"])
    hr_master_location_df = get_data_df(HR_MASTER_LOCATION_FILENAME_PATH,
                                        secrets["minio"]["edge"]["access"],
                                        secrets["minio"]["edge"]["secret"])
    logging.info("Fetch[ed] HR master data")

    logging.debug(
        f"hr_master_ingestion_df[DEPARTMENT_COL].value_counts()=\n"
        f"{hr_master_ingestion_df[DEPARTMENT_COL].value_counts()}"
    )

    logging.info("Merg[ing] in HR attribute data")
    hr_master_df = add_boolean_columns(hr_master_ingestion_df)
    logging.info("Merg[ed] in HR attribute data")

    logging.info("Merg[ing] in location data")
    hr_master_df = merge_in_location_data(hr_master_df, hr_master_location_df)
    logging.info("Merg[ed] in HR Master data")

    logging.info("Validat[ing] HR master data")
    validate_hr_data(hr_master_df)
    logging.info("Validat[ed] HR master data")

    # Writing result out
    logging.info("Writing cleaned HR Form DataFrame to Minio...")
    hr_master_df.drop([HR_MASTER_STAFFNUMBER], axis='columns')
    minio_utils.dataframe_to_minio(hr_master_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=HR_MASTER_FILENAME_PATH,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
