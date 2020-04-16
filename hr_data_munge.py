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
HR_TRANSACTION_DATE = 'Date'
HR_TRANSACTIONAL_COLUMNS = [HR_TRANSACTIONAL_STAFFNUMBER, 'Categories', HR_TRANSACTION_DATE]
HR_MASTER_STAFFNUMBER = 'StaffNumber'

HR_COLUMNS_TO_FLATTEN = {HR_TRANSACTIONAL_STAFFNUMBER, 'Categories', 'Employee Name'}

VALID_STATUSES = (
    r"Working remotely \(NO Covid-19 exposure\)",
    r"At work \(on site\)",
    "On leave",
    "On suspension",
    r"Absent from work \(unauthorised\)",
    "Quarantine leave – working remotely",
    "Quarantine leave – unable to work remotely",
    "Quarantine leave – working remotely, Covid-19 exposure / isolation",
    r"Sick \(linked to Covid-19\)",
    r"Sick \(NOT linked to Covid-19\)",
    "On Lockdown leave – unable to work remotely",
    "On Lockdown leave – able to work remotely"
)
STATUSES_VALIDITY_PATTERN = "^(" + ")$|^(".join(VALID_STATUSES) + "$)"

ISO8601_FORMAT = "%Y-%m-%d %H:%M:%S"
HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS = {
    HR_TRANSACTIONAL_STAFFNUMBER: lambda col: (col.str.match(r"^\d{8}$") == True),
    "Categories": lambda col: (col.str.match(STATUSES_VALIDITY_PATTERN) == True),
    HR_TRANSACTION_DATE: lambda col: pandas.to_datetime(col, format=ISO8601_FORMAT, errors='coerce').notna(),
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


def flatten_hr_form(hr_df):
    null_entries = hr_df[HR_COLUMNS_TO_FLATTEN].isna().any(axis=1)
    logging.debug(f"Not flattening '{null_entries.sum()}'/'{hr_df.shape[0]}'")

    def flatten_row(row):
        # Flattening the columns that need it
        flattened_column_values = {
            col: [val for val in row[col].split(";") if val != ""]
            for col in HR_COLUMNS_TO_FLATTEN
        }
        try:
            row_df = pandas.DataFrame(flattened_column_values)

            # Broadcasting all of the other columns
            for col in hr_df.columns:
                if col not in HR_COLUMNS_TO_FLATTEN:
                    row_df[col] = row[col]

            return row_df
        except ValueError:
            length_strings = [col + ':' + str(len(values)) for col, values in flattened_column_values.items()]
            logging.error(
                f"Skipping {row['Manager Staff No']} on {row['Date']}, lengths are {','.join(length_strings)}")

    flat_df = pandas.concat(
        hr_df[~null_entries].apply(flatten_row, axis=1).values
    )

    # Appending the null entries
    # Probably no point, but that is not the job of this function!
    flat_df = flat_df.append(hr_df[null_entries]).reset_index(drop=True)

    return flat_df


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


def update_hr_form(cleaned_hr_df):
    current_state_df = get_data_df(CLEANED_HR_TRANSACTIONAL + ".csv",
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"])
    current_state_df[HR_MASTER_STAFFNUMBER] = current_state_df[HR_MASTER_STAFFNUMBER].astype('str')
    current_state_df = current_state_df[cleaned_hr_df.columns]

    logging.debug(f"current_state_df.shape=\n{current_state_df.shape}")
    logging.debug(f"cleaned_hr_df.shape=\n{cleaned_hr_df.shape}")

    # Combing, then dropping duplicates, based upon staff number and date
    combined_df = pandas.concat([current_state_df, cleaned_hr_df])
    deduped_df = combined_df.drop_duplicates(subset=[HR_MASTER_STAFFNUMBER, HR_TRANSACTION_DATE])

    logging.debug(f"deduped_df.shape={deduped_df.shape}")
    logging.debug(f"Got {deduped_df.shape[0] - current_state_df.shape[0]} new values")

    new_values_df = combined_df.drop_duplicates(subset=[HR_MASTER_STAFFNUMBER, HR_TRANSACTION_DATE], keep=False)
    logging.debug(
        f"new_values=\n{new_values_df}"
    )

    return deduped_df


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

    logging.info("Flatten[ing] HR form data")
    flat_hr_form_df = flatten_hr_form(hr_form_df)
    logging.info("Flatten[ed] HR form data")

    logging.info("Clean[ing] HR form data")
    cleaned_hr_form_df = clean_hr_form(flat_hr_form_df, hr_master_df)
    logging.info("Clean[ed] HR form data")

    logging.info("Dedup[ing] HR form data")
    deduped_hr_form_df = update_hr_form(cleaned_hr_form_df)
    logging.info("Dedup[ed] HR form data")

    # Writing result out
    logging.info("Writing cleaned HR Form DataFrame to Minio...")
    minio_utils.dataframe_to_minio(deduped_hr_form_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=CLEANED_HR_TRANSACTIONAL,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
