import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import pandas

BUCKET = 'covid'
CLASSIFICATION = minio_utils.DataClassification.EDGE
HR_TRANSACTIONAL_FILENAME_PATH = "data/private/business_continuity_people_status.csv"
HR_MASTER_FILENAME_PATH = "data/private/city_people.csv"

HR_STAFFNUMBER = 'StaffNumber'
HR_TRANSACTION_DATE = 'Date'
HR_TRANSACTION_EVALUATION = 'Evaluation'
HR_LOCATION = "LocationWkt"
HR_CATEGORIES = 'Categories'
HR_TRANSACTIONAL_COLUMNS = [HR_STAFFNUMBER, HR_CATEGORIES, HR_TRANSACTION_DATE, HR_TRANSACTION_EVALUATION]

DATE_COL_FORMAT = "%Y-%m-%d"
HR_ORG_UNIT_COLUMNS = [
    'Org Unit Name', 'Directorate', 'Department', 'Branch', 'Section',
    'Division', 'Div Sub Area', 'Unit', 'Subunit'
]

HR_ORG_UNIT_STATUSES = "data/private/business_continuity_org_unit_statuses"


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


def merge_df(hr_df, hr_master_df):
    combined_df = hr_df.merge(
        hr_master_df,
        left_on=HR_STAFFNUMBER,
        right_on=HR_STAFFNUMBER,
        how='left',
        validate="many_to_one",
    )
    logging.debug(f"combined_df.head(5)=\n{combined_df.head(5)}")

    return combined_df


def get_org_unit_df(combined_df):
    # We only care about dates
    combined_df[HR_TRANSACTION_DATE] = pandas.to_datetime(
        combined_df[HR_TRANSACTION_DATE]
    ).dt.strftime(DATE_COL_FORMAT)

    # Apparently what is needed to fill NaN columns
    filled_df = combined_df.copy()
    filled_df.loc[:, HR_ORG_UNIT_COLUMNS] = filled_df.loc[:, HR_ORG_UNIT_COLUMNS].fillna("N/A")
    logging.debug(f"filled_df.head(5)=\n{filled_df.head(5)}")
    logging.debug(f"filled_df.shape=\n{filled_df.shape}")

    groupby_cols = [*HR_ORG_UNIT_COLUMNS, HR_TRANSACTION_DATE]
    flattened_org_unit_df = (
        # select the most common value in the evaluation col
        filled_df.groupby(groupby_cols, sort=False)
                .apply(
                    lambda df: pandas.DataFrame({
                                    HR_TRANSACTION_EVALUATION: df[HR_TRANSACTION_EVALUATION].mode(),
                                    HR_LOCATION: df[HR_LOCATION].mode(),
                                    **df[HR_CATEGORIES].value_counts().to_dict()
                    })
            )
            # getting back to a dataframe
            .reset_index()
            # cleaning up the new index that appears
            .drop(f"level_{len(groupby_cols)}", axis='columns')
    )
    logging.debug(f"flattened_org_unit_df.head(5)=\n{flattened_org_unit_df.head(5)}")
    logging.debug(f"flattened_org_unit_df.shape=\n{flattened_org_unit_df.shape}")

    melted_df = flattened_org_unit_df.melt(
        id_vars=[*groupby_cols, HR_TRANSACTION_EVALUATION, HR_LOCATION],
        var_name=HR_CATEGORIES,
        value_name="StatusCount"
    )
    melted_df.loc[:, "StatusCount"] = melted_df.loc[:, "StatusCount"].fillna(0)
    logging.debug(f"melted_df.head(5)=\n{melted_df.head(5)}")
    logging.debug(f"melted_df.columns=\n{melted_df.columns}")

    # Coverage
    logging.debug(f"melted_df.Directorate.value_counts()={melted_df.Directorate.value_counts()}")
    logging.debug(f"melted_df.Department.value_counts()={melted_df.Department.value_counts()}")
    logging.debug(f"melted_df.Branch.value_counts()={melted_df.Branch.value_counts()}")
    logging.debug(f"melted_df.Section.value_counts()={melted_df.Branch.value_counts()}")

    return melted_df


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
    hr_transactional_df = get_data_df(HR_TRANSACTIONAL_FILENAME_PATH,
                                      secrets["minio"]["edge"]["access"],
                                      secrets["minio"]["edge"]["secret"])
    hr_master_df = get_data_df(HR_MASTER_FILENAME_PATH,
                               secrets["minio"]["edge"]["access"],
                               secrets["minio"]["edge"]["secret"])
    logging.info("Fetch[ed] HR data")

    logging.info("Merg[ing] Transactional and Master data")
    merged_df = merge_df(hr_transactional_df, hr_master_df)
    logging.info("Merg[ed] Transactional and Master data")

    logging.info("Dedup[ing] HR form data")
    org_unit_df = get_org_unit_df(merged_df)
    logging.info("Dedup[ed] HR form data")

    # Writing result out
    logging.info("Writing cleaned HR Form DataFrame to Minio...")
    minio_utils.dataframe_to_minio(org_unit_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=HR_ORG_UNIT_STATUSES,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
