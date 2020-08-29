import json
import logging
import os
import sys

from db_utils import minio_utils
import pandas

import hr_data_munge
import hr_sap_data_to_minio

BUCKET = 'covid'
CLASSIFICATION = minio_utils.DataClassification.EDGE
CLEANED_HR_TRANSACTIONAL = "data/private/business_continuity_people_status"

HR_MASTER_APPROVER_COLUMN = "Approver Name"
HR_MASTER_APPROVER_STAFFNUMBER_COLUMN = "Approver Staff No"

SAP_FILES_COLUMN_MAP = {
    "zemp_q0001_v7_bics_covid_auto": {
        "Employee": hr_data_munge.HR_TRANSACTIONAL_STAFFNUMBER,
        'Unnamed: 5': hr_data_munge.HR_STATUS,
        'Calendar day': hr_data_munge.HR_TRANSACTION_DATE,
    },
    "zatt_2002_auto": {
        "Employee": hr_data_munge.HR_TRANSACTIONAL_STAFFNUMBER,
        'Unnamed: 5': hr_data_munge.HR_STATUS,
        'Date': hr_data_munge.HR_TRANSACTION_DATE,
    },
    "zemp_q0001_v7_bics_dash_auto": {
        "Employee": hr_data_munge.HR_TRANSACTIONAL_STAFFNUMBER,
        'Absence Type': hr_data_munge.HR_STATUS,
        'Calendar day': hr_data_munge.HR_TRANSACTION_DATE,
    }
}

HR_MASTER_TO_TRANSACTIONAL_REMAP = {
    HR_MASTER_APPROVER_COLUMN: hr_data_munge.HR_MANAGER,
    HR_MASTER_APPROVER_STAFFNUMBER_COLUMN: hr_data_munge.HR_MANAGER_STAFFNUMBER
}

SAP_DATE_FORMAT = "%d.%m.%Y"
HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS = {
    # col : (validation function, debugging function)
    hr_data_munge.HR_TRANSACTION_DATE: (
        lambda col: (
            pandas.to_datetime(col, format=SAP_DATE_FORMAT, errors='coerce').notna() &
            (pandas.to_datetime(col, format=SAP_DATE_FORMAT, errors='coerce').date() <= pandas.Timestamp.today().date())
        ),
        lambda invalid_df: (
            f"\n{invalid_df[hr_data_munge.HR_TRANSACTION_DATE].value_counts()}, "
            f"\n{invalid_df[hr_data_munge.HR_TRANSACTION_DATE].value_counts().index}"
        )),
}

HR_TRANSACTIONAL_COLUMN_RENAME_DICT = {
    HR_MASTER_APPROVER_COLUMN: hr_data_munge.HR_APPROVER,
    HR_MASTER_APPROVER_STAFFNUMBER_COLUMN: hr_data_munge.HR_APPROVER_STAFFNUMBER
}


def remap_columns(data_df, columns_remap):
    logging.debug(f"data_df.columns={data_df.columns}")
    data_df.rename(columns=columns_remap, inplace=True)
    logging.debug(f"data_df.columns={data_df.columns}")

    return data_df


def fill_columns(data_df, master_df):
    # Alas, we don't know anything about the unit's status
    data_df[hr_data_munge.HR_UNIT_EVALUATION] = None

    # We can looking up approver details in HR master data though
    lookup_df = master_df.copy()[[hr_data_munge.HR_MASTER_STAFFNUMBER,
                                  HR_MASTER_APPROVER_COLUMN,
                                  HR_MASTER_APPROVER_STAFFNUMBER_COLUMN]]

    logging.debug(f"(pre-approver details lookup) data_df.shape={data_df.shape}")
    data_df = data_df.merge(lookup_df,
                            left_on=hr_data_munge.HR_TRANSACTIONAL_STAFFNUMBER,
                            right_on=hr_data_munge.HR_MASTER_STAFFNUMBER,
                            how="left", validate="many_to_one")
    data_df.rename(
        columns=HR_MASTER_TO_TRANSACTIONAL_REMAP,
        inplace=True
    )
    logging.debug(f"(post-approver details lookup) data_df.shape={data_df.shape}")
    logging.debug(f"data_df.sample(1).iloc[0]=\n{data_df.sample(1).iloc[0]}")

    return data_df


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
    hr_master_df = hr_data_munge.get_data_df(hr_data_munge.HR_MASTER_FILENAME_PATH,
                                             secrets["minio"]["edge"]["access"],
                                             secrets["minio"]["edge"]["secret"])
    hr_transactional_df = hr_data_munge.get_data_df(hr_data_munge.CLEANED_HR_TRANSACTIONAL_FILENAME,
                                                    secrets["minio"]["edge"]["access"],
                                                    secrets["minio"]["edge"]["secret"])
    logging.info("Fetch[ed] HR data")

    for sap_file_suffix, sap_file_columns_remap in SAP_FILES_COLUMN_MAP.items():
        sap_filename = f"{hr_sap_data_to_minio.SAP_RAW_FILENAME_PREFIX}{sap_file_suffix}.csv"
        logging.info(f"Fetch[ing] SAP HR data '{sap_filename}'")
        hr_sap_df = hr_data_munge.get_data_df(sap_filename,
                                              secrets["minio"]["edge"]["access"],
                                              secrets["minio"]["edge"]["secret"])
        logging.info(f"Fetch[ed] SAP HR data '{sap_filename}'")

        logging.info(f"Remapp[ing] SAP HR data '{sap_filename}'")
        remapped_df = remap_columns(hr_sap_df, sap_file_columns_remap)
        logging.info(f"Remapp[ed] SAP HR data '{sap_filename}'")

        logging.info(f"Fill[ing] SAP HR data '{sap_filename}'")
        filled_df = fill_columns(remapped_df, hr_master_df)
        logging.info(f"Fill[ed] HR form data '{sap_filename}'")

        logging.info(f"Clean[ing] SAP HR data '{sap_filename}'")
        # Overridding transactional column checks
        hr_data_munge.HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS = {
            **hr_data_munge.HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS,
            **HR_TRANSACTIONAL_COLUMN_VERIFICATION_FUNCS
        }
        hr_data_munge.HR_TRANSACTIONAL_COLUMN_RENAME_DICT = {
            **hr_data_munge.HR_TRANSACTIONAL_COLUMN_RENAME_DICT,
            **HR_TRANSACTIONAL_COLUMN_RENAME_DICT
        }
        cleaned_df = hr_data_munge.clean_hr_form(filled_df, hr_master_df)
        logging.info(f"Clean[ed] SAP HR data '{sap_filename}'")

        logging.info(f"Dedup[ing] SAP HR data '{sap_filename}'")
        hr_transactional_df = hr_data_munge.update_hr_dataset(cleaned_df, hr_transactional_df)
        logging.info(f"Dedup[ed] SAP HR data '{sap_filename}'")

    # Writing result out
    logging.info("Writing cleaned SAP HR DataFrame back to Minio...")
    minio_utils.dataframe_to_minio(hr_transactional_df, BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   minio_utils.DataClassification.EDGE,
                                   filename_prefix_override=CLEANED_HR_TRANSACTIONAL,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
