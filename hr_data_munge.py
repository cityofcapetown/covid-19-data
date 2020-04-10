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
HR_MASTER_FILENAME_PATH = "data/private/city_people.csv"

HR_TRANSACTIONAL_STAFFNUMBER = 'Employee No'
HR_TRANSACTIONAL_COLUMNS = [HR_TRANSACTIONAL_STAFFNUMBER, 'Categories', 'Date']
HR_MASTER_STAFFNUMBER = 'StaffNumber'

VALID_STATUSES = (
    r"Working remotely \(NO Covid-19 exposure\)",
    r"At work \(on site\)",
    "On leave",
    "On suspension",
    "Absent from work (unauthorised)",
    "Quarantine leave – working remotely",
    "Quarantine leave – unable to work remotely",
    "Quarantine leave – working remotely, Covid-19 exposure / isolation",
    r"Sick \(linked to Covid-19\)",
    r"Sick \(NOT linked to Covid-19\)",
    "On Lockdown leave – unable to work remotely",
    "On Lockdown leave – able to work remotely"
)
STATUSES_VALIDITY_PATTERN = "^(" + ")$|^(".join(VALID_STATUSES) + "$)"

HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS = {
    "Employee No": lambda col: (col.str.match(r"^\d{8}$") == True),
    "Categories": lambda col: (col.str.match(STATUSES_VALIDITY_PATTERN) == True),
    "Date": lambda col: pandas.to_datetime(col, format="%Y-%m-%d %H:%M:%S", errors='coerce').notna(),
}

CLEANED_HR_TRANSACTIONAL = "data/private/business_continuity_people_status"


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


def clean_hr_form(hr_df, master_df):
    logging.debug(f"hr_df.shape={hr_df.shape}")
    # Checking validity of HR DF, and *not* selecting invalid value
    hr_df["Valid"] = True
    for col, validity_func in HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS.items():
        col_validity = validity_func(hr_df[col])

        if col_validity.sum() > 0:
            logging.warning(f"Found {(~col_validity).sum()} invalid values in attribute '{col}'")
            logging.debug(f"hr_df[~col_validity].head(10)=\n{hr_df[~col_validity].head(10)}")

        hr_df.Valid &= col_validity

    cleaned_hr_df = hr_df.loc[
        hr_df.Valid,
        HR_TRANSACTIONAL_COLUMNS
    ].copy()
    logging.debug(f"cleaned_hr_df.shape={cleaned_hr_df.shape}")

    # Checking membership in master data file
    cleaned_hr_df.rename({HR_TRANSACTIONAL_STAFFNUMBER: HR_MASTER_STAFFNUMBER}, axis='columns', inplace=True)
    in_master = cleaned_hr_df[HR_MASTER_STAFFNUMBER].isin(master_df[HR_MASTER_STAFFNUMBER].astype('str'))
    if in_master.sum() < cleaned_hr_df.shape[0]:
        logging.warning(
            f"Found {cleaned_hr_df.shape[0] - in_master.sum()} observations in transactional data *not* in master data"
        )
        logging.debug(f"cleaned_hr_df[~in_master].head(10)=\n{cleaned_hr_df[~in_master].head(10)}")
        cleaned_hr_df = cleaned_hr_df[in_master]

    if cleaned_hr_df.shape[0] < hr_df.shape[0]:
        logging.warning(
            f"Dropped {hr_df.shape[0] - cleaned_hr_df.shape[0]} observations from {hr_df.shape[0]} observations of "
            f"transactional data")

    return cleaned_hr_df


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

    logging.info("Fetch[ing] HR data")
    hr_form_df = get_data_df(HR_FORM_FILENAME_PATH,
                             secrets["minio"]["edge"]["access"],
                             secrets["minio"]["edge"]["secret"])
    hr_master_df = get_data_df(HR_MASTER_FILENAME_PATH,
                               secrets["minio"]["edge"]["access"],
                               secrets["minio"]["edge"]["secret"])
    logging.info("Fetch[ed] HR data")

    logging.info("Clean[ing] HR form data")
    cleaned_hr_form_df = clean_hr_form(hr_form_df, hr_master_df)
    logging.info("Clean[ed] HR form data")

    # Writing result out
    logging.info("Writing cleaned HR Form DataFrame to Minio...")
    minio_utils.dataframe_to_minio(cleaned_hr_form_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=CLEANED_HR_TRANSACTIONAL,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
