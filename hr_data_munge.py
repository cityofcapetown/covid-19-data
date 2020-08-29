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
HR_MASTER_STAFFNUMBER = 'StaffNumber'
HR_TRANSACTION_DATE = 'Date'
HR_STATUS = "Categories"
HR_UNIT_EVALUATION = "Evaluation"
HR_MANAGER = "Manager"
HR_MANAGER_STAFFNUMBER = 'Manager Staff No'
HR_TRANSACTIONAL_COLUMNS = [HR_TRANSACTIONAL_STAFFNUMBER, HR_STATUS, HR_TRANSACTION_DATE, HR_UNIT_EVALUATION,
                            HR_MANAGER, HR_MANAGER_STAFFNUMBER]

HR_COLUMNS_TO_FLATTEN = {HR_TRANSACTIONAL_STAFFNUMBER, HR_STATUS, 'Employee Name'}

VALID_STATUSES = (
    'At work',
    'Working from Home',
    'Off-Site',
    'COVID-19 Quarantine – Working',
    'COVID-19 Quarantine - Not Working',
    'COVID-19 Lockdown',
    'Sick Leave (linked to COVID-19)',
    'Sick Leave (NOT linked to COVID-19)',
    'Leave',
    'Suspended',
    'Absent'
)

STATUS_REMAP = {
    # Previous Statuses
    "At work (on site)": 'At work',
    "On leave": 'Leave',
    "On suspension": 'Suspended',
    'Absent from work (unauthorised)': 'Absent',
    "Quarantine leave – unable to work remotely": 'COVID-19 Lockdown',
    "Quarantine leave – working remotely, COVID 19 exposure / isolation": 'COVID-19 Quarantine – Working',
    r"Sick \(NOT linked to COVID 19\)": 'Sick Leave (NOT linked to COVID-19)',
    "On Lockdown leave – unable to work remotely": 'COVID-19 Lockdown',
    "On Lockdown leave – able to work remotely": 'Working from Home',
    "Quarantine leave – working remotely": 'Working from Home',

    # Other permutations seen
    r"Absent from work \(unauthorised\)": 'Absent',
    'At work \\\\(on site\\\\)': 'At work',
    r"At work \(on site\)": 'At work',
    r"Working remotely (COVID 19 exposure/isolation)": 'COVID-19 Quarantine – Working',
    r"Working remotely (NO COVID 19 exposure)": 'Working from Home',
    r"Working remotely (Covid-19 exposure/isolation)": 'COVID-19 Quarantine – Working',
    r"Working remotely (NO Covid-19 exposure)": 'Working from Home',
    'Sick (linked to COVID 19)': 'Sick Leave (linked to COVID-19)',
    r"Sick \(linked to Covid-19\)": 'Sick Leave (linked to COVID-19)',
    r"Sick \(NOT linked to Covid-19\)": 'Sick Leave (NOT linked to COVID-19)',
    'Sick Leave (NOT linked to COVID-19)': 'Sick Leave (NOT linked to COVID-19)',
    "Depot Close Due to Positive Case": 'COVID-19 Quarantine – Working',
    "Depot closed due Positive Case": 'COVID-19 Quarantine – Working',
    "On Rotation": 'At work',
    'Sick (NOT linked to COVID 19)': 'Sick Leave (NOT linked to COVID-19)',
    'Sick \\\\(NOT linked to COVID 19\\\\)': 'Sick Leave (linked to COVID-19)',
    r"Sick \(NOT linked to COVID-19\)": 'Sick Leave (NOT linked to COVID-19)',
    'Sick (NOT linked to COVID-19)': 'Sick Leave (NOT linked to COVID-19)',
    'Sick (NOT linked to Covid-19)': 'Sick Leave (NOT linked to COVID-19)',
    'Sick \\\\(linked to COVID 19\\\\)': 'Sick Leave (linked to COVID-19)',
    r"Sick \(linked to COVID 19\)": 'Sick Leave (linked to COVID-19)',
    "Sick \(linked to COVID-19\)": 'Sick Leave (linked to COVID-19)',
    "Sick (linked to COVID-19)": 'Sick Leave (linked to COVID-19)',
    "Sick (linked to Covid-19)": 'Sick Leave (linked to COVID-19)',
    'Sick (Linked to Covid-19)': 'Sick Leave (linked to COVID-19)',
    'Shift rest days': 'Leave',
    'quarantine leave - unable to work remotely': 'COVID-19 Lockdown',
    "Quarantine leave - unable to work remotely": 'COVID-19 Lockdown',
    "Quarantine leave - working remotely, COVID 19 exposure / isolation": 'COVID-19 Quarantine – Working',
    "Quarantine leave - working remotely, COVID-19 exposure / isolation": 'COVID-19 Quarantine – Working',
    "Quarantine leave - working remotely, COVID-19 exposure/isolation": 'COVID-19 Quarantine – Working',
    "Quarantine leave - working remotely, COVID-10 exposure/isolation": 'COVID-19 Quarantine – Working',
    'Quarantine leave - unable to work remotely, COVID-19 exposure/isolation': 'COVID-19 Quarantine - Not Working',
    "Quarantine leave - working remotely": 'Working from Home',

    # SAP Status Remaps
    # zemp_q0001_v7_bics_dash_auto
    'Sick Leave WD with c': 'Sick Leave (NOT linked to COVID-19)',
    'Lve WD All': "Leave",
    'Non compulsory leave': 'Leave',
    'Quarantine': 'COVID-19 Quarantine - Not Working',
    'Lve WD All : < day': "Leave",
    'Unpaid ILO Sick with': 'Sick Leave (NOT linked to COVID-19)',
    'Maternity - Paid': "Leave",
    'Suspended - Unpaid S': "Suspended",
    'Lve ILO Sick with ce': 'Sick Leave (NOT linked to COVID-19)',
    'Suspended - Unpaid': "Suspended",
    "Exam": "Leave",
    'Family Responsibilit': "Leave",
    'Maternity - Unpaid': "Leave",
    'Long Service Leave W': "Leave",
    'Unpaid - Authorized': "Absent",
    'Leave in Lieu of O/T': "Leave",
    'Study': "Leave",
    'Unpaid - Unauthorize': "Absent",
    'Lve ILO Sick w cert<': 'Sick Leave (NOT linked to COVID-19)',
    'Sick Lve with cert:': 'Sick Leave (NOT linked to COVID-19)',
    'Reward & Recognition': "Leave",
    'TOIL Leave - Days': "Leave",
    'Sick Lve w/o cert.:': 'Sick Leave (NOT linked to COVID-19)',
    'Court Attendance': "Leave",
    'Lve ILO Sick w/out c': 'Sick Leave (NOT linked to COVID-19)',
    'Sick Leave WD w/out': 'Sick Leave (NOT linked to COVID-19)',
    'Reward&Recognition <': "Leave",

    # zatt_2002_auto
    'Work from Home': 'Working from Home',

    # zemp_q0001_v7_bics_covid_auto
    'COVID-19 Quarantine': 'COVID-19 Quarantine - Not Working',
}
for val in STATUS_REMAP.values():
    assert val in VALID_STATUSES, f"{val} not in status list"

VALID_EVALUATION_STATUSES = (
    'We can deliver on daily tasks',
    'We can deliver 75% or less of daily tasks',
    'We cannot deliver on daily tasks',
    None
)

EVALUATION_STATUS_REMAP = {
    'We can deliver on 75% or less of daily tasks': 'We can deliver 75% or less of daily tasks',
    'We can do the bare minimum': 'We can deliver 75% or less of daily tasks',
    'We can continue as normal': 'We can deliver on daily tasks',
    None: None
}
for val in EVALUATION_STATUS_REMAP.values():
    assert val in VALID_EVALUATION_STATUSES, f"{val} not in status list"

ISO8601_FORMAT = "%Y-%m-%d %H:%M:%S"
HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS = {
    # col : (validation function, debugging function)
    HR_TRANSACTIONAL_STAFFNUMBER: (lambda col: (col.str.match(r"^\d{8}$") == True),
                                   lambda invalid_df: invalid_df[HR_TRANSACTIONAL_STAFFNUMBER].head(10)),
    HR_STATUS: (lambda col: (col.isin(VALID_STATUSES) == True),
                lambda
                    invalid_df: f"\n{invalid_df[HR_STATUS].value_counts()}, \n{invalid_df[HR_STATUS].value_counts().index}"),
    HR_TRANSACTION_DATE: (lambda col: pandas.to_datetime(col, format=ISO8601_FORMAT, errors='coerce').notna(),
                          lambda
                              invalid_df: f"\n{invalid_df[HR_TRANSACTION_DATE].value_counts()}, \n{invalid_df[HR_TRANSACTION_DATE].value_counts().index}"),
    HR_UNIT_EVALUATION: (lambda col: (col.isin(VALID_EVALUATION_STATUSES)),
                         lambda
                             invalid_df: f"\n{invalid_df[HR_UNIT_EVALUATION].value_counts()}, \n{invalid_df[HR_UNIT_EVALUATION].value_counts().index}")
}

HR_APPROVER = "Approver"
HR_APPROVER_STAFFNUMBER = "ApproverStaffNumber"
HR_TRANSACTIONAL_COLUMN_RENAME_DICT = {
    HR_TRANSACTIONAL_STAFFNUMBER: HR_MASTER_STAFFNUMBER,
    HR_MANAGER: HR_APPROVER,
    HR_MANAGER_STAFFNUMBER: HR_APPROVER_STAFFNUMBER
}

CLEANED_HR_TRANSACTIONAL_PREFIX = "data/private/business_continuity_people_status"
CLEANED_HR_TRANSACTIONAL_FILENAME = f"{CLEANED_HR_TRANSACTIONAL_PREFIX}.csv"


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
    logging.debug(f"Not flattening '{(null_entries).sum()}'/'{hr_df.shape[0]}' because they have null entries")

    already_flat = True
    for col in HR_COLUMNS_TO_FLATTEN:
        flat_mask = hr_df[col].str.contains(";") == True
        already_flat &= ~(flat_mask)

    logging.debug(f"Not flattening '{(already_flat).sum()}'/'{hr_df.shape[0]}' because they seem to already be flat")

    def flatten_row(row):
        # Flattening the columns that need it
        flattened_column_values = {
            col: [val for val in row[col].split(";")]
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
                f"Skipping {row['Manager Staff No']} on {row['Date']}, lengths are {','.join(length_strings)}"
            )

    flat_df = pandas.concat(
        hr_df[~(null_entries | already_flat)].apply(flatten_row, axis=1).values
    )

    # Appending entries without ";"
    flat_df = flat_df.append(hr_df[already_flat]).reset_index(drop=True)

    # Appending the null entries
    # Probably no point, but that is not the job of this function!
    flat_df = flat_df.append(hr_df[null_entries]).reset_index(drop=True)

    return flat_df


def clean_hr_form(hr_df, master_df):
    logging.debug(f"hr_df.shape={hr_df.shape}")

    # Extracting staff number
    hr_df[HR_TRANSACTIONAL_STAFFNUMBER] = hr_df[HR_TRANSACTIONAL_STAFFNUMBER].astype(str).str.extract(".*(\d{8}).*")

    # Remapping statuses
    hr_df[HR_STATUS] = hr_df[HR_STATUS].apply(
        lambda val: STATUS_REMAP.get(val, str(val).strip())
    )

    # Remapping evaluation statuses
    hr_df[HR_UNIT_EVALUATION] = hr_df[HR_UNIT_EVALUATION].apply(
        lambda val: EVALUATION_STATUS_REMAP.get(val, str(val).strip())
    )

    # Checking validity of HR DF, and *not* selecting invalid value
    hr_df["Valid"] = True
    for col, (validity_func, debug_func) in HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS.items():
        col_validity = validity_func(hr_df[col])

        if col_validity.sum() != col_validity.shape[0]:
            logging.warning(f"Found {(~col_validity).sum()} invalid values in attribute '{col}'")
            logging.debug(f"hr_df[~col_validity].head(10)=\n{hr_df[~col_validity].head(10)}")
            logging.debug(f"debug_func(hr_df[~col_validity])=\n{debug_func(hr_df[~col_validity])}")

        hr_df.Valid &= col_validity

    cleaned_hr_df = hr_df.loc[
        hr_df.Valid,
        HR_TRANSACTIONAL_COLUMNS
    ].copy()
    logging.debug(f"cleaned_hr_df.shape={cleaned_hr_df.shape}")

    # Checking membership in master data file
    cleaned_hr_df.rename(HR_TRANSACTIONAL_COLUMN_RENAME_DICT, axis='columns', inplace=True)
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
            f"transactional data"
        )

    return cleaned_hr_df


def update_hr_dataset(cleaned_hr_df, current_state_df):
    current_state_df[HR_MASTER_STAFFNUMBER] = current_state_df[HR_MASTER_STAFFNUMBER].astype('str')
    current_state_df = current_state_df[cleaned_hr_df.columns]

    logging.debug(f"current_state_df.shape={current_state_df.shape}")
    logging.debug(f"cleaned_hr_df.shape={cleaned_hr_df.shape}")

    # Combining, then dropping duplicates, based upon staff number and date
    combined_df = pandas.concat([current_state_df, cleaned_hr_df])
    dummy_date_col = 'temp_date'
    combined_df[dummy_date_col] = pandas.to_datetime(combined_df[HR_TRANSACTION_DATE]).dt.date

    # First, sorting in terms of the timestamp, then deduping on the *date*,
    # this should keep the most recent record for each day
    deduped_df = combined_df.sort_values(
        by=[HR_TRANSACTION_DATE], ascending=False
    ).drop_duplicates(subset=[HR_MASTER_STAFFNUMBER, dummy_date_col])

    logging.debug(
        f"deduped_df['{HR_TRANSACTION_DATE}'].value_counts()=\n{deduped_df[HR_TRANSACTION_DATE].value_counts()}"
    )

    logging.debug(f"deduped_df.shape={deduped_df.shape}")
    logging.debug(f"Got {deduped_df.shape[0] - current_state_df.shape[0]} new values")

    new_values_df = combined_df.sort_values(
        by=[HR_TRANSACTION_DATE], ascending=False
    ).drop_duplicates(subset=[HR_MASTER_STAFFNUMBER, dummy_date_col], keep=False)
    logging.debug(
        f"new_values=\n{new_values_df}"
    )

    # Getting rid of dummy date column before returning
    deduped_df.drop(dummy_date_col, axis='columns', inplace=True)

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
    hr_transactional_df = get_data_df(CLEANED_HR_TRANSACTIONAL_FILENAME,
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
    deduped_hr_form_df = update_hr_dataset(cleaned_hr_form_df)
    logging.info("Dedup[ed] HR form data")

    # Writing result out
    logging.info("Writing cleaned HR Form DataFrame to Minio...")
    minio_utils.dataframe_to_minio(deduped_hr_form_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=CLEANED_HR_TRANSACTIONAL_PREFIX,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
