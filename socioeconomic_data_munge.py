import contextlib
import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import pandas

import socioeconomic_sharepoint_to_minio

# Source Data Cols
YEAR_COL = "Year"

# Tidy Data Cols
DATE_COL = "date"
MEASURE_COL = "measure"
FEATURE_COL = "feature"
VALUE_COL = "value"

TIDY_MINIO_NAME = "socioeconomic_tidy_data"


@contextlib.contextmanager
def get_socioeconomic_data(minio_access, minio_secret):
    minio_path = f"{socioeconomic_sharepoint_to_minio.RESTRICTED_PREFIX}" \
                 f"{socioeconomic_sharepoint_to_minio.COVID_DATA_FILE}"

    with tempfile.NamedTemporaryFile("rb") as temp_data_file:
        result = minio_utils.minio_to_file(
            filename=temp_data_file.name,
            minio_filename_override=minio_path,
            minio_bucket=socioeconomic_sharepoint_to_minio.COVID_BUCKET,
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=minio_utils.DataClassification.EDGE,
        )
        assert result

        yield pandas.read_excel(temp_data_file.name, sheet_name=None, engine='openpyxl')


def flatten_socioeconomic_dict(socioeconomic_data_dict):
    # Treating each sheet as a measure
    tidy_socioeconomic_data_df = pandas.concat((
        measure_df.melt(
            id_vars=[YEAR_COL], var_name=FEATURE_COL,
        ).assign(
            **{MEASURE_COL: measure}
        ).dropna(subset=[VALUE_COL]).rename(columns={YEAR_COL: DATE_COL})
        for measure, measure_df in socioeconomic_data_dict.items()
    ))
    logging.debug(f"tidy_socioeconomic_data_df.shape={tidy_socioeconomic_data_df.shape}")
    logging.debug(
        f"tidy_socioeconomic_data_df['{MEASURE_COL}'].value_counts()=\n"
        f"{tidy_socioeconomic_data_df[MEASURE_COL].value_counts()}"
    )
    logging.debug(
        f"tidy_socioeconomic_data_df['{FEATURE_COL}'].value_counts()=\n"
        f"{tidy_socioeconomic_data_df[FEATURE_COL].value_counts()}"
    )
    logging.debug(f"tidy_socioeconomic_data_df.sample(10)=\n{tidy_socioeconomic_data_df.sample(10)}")

    return tidy_socioeconomic_data_df


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("G[etting] data from Minio")
    with get_socioeconomic_data(secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"]) as se_data_dict:
        logging.info("G[ot] data from Minio")

        logging.info("Tid[ying] data")
        tidy_se_data_df = flatten_socioeconomic_dict(se_data_dict)
        logging.info("Tid[ied] data")

        logging.info("Wr[iting] data to Minio...")
        result = minio_utils.dataframe_to_minio(
                tidy_se_data_df,
                filename_prefix_override=socioeconomic_sharepoint_to_minio.RESTRICTED_PREFIX + TIDY_MINIO_NAME,
                minio_bucket=socioeconomic_sharepoint_to_minio.COVID_BUCKET,
                minio_key=secrets["minio"]["edge"]["access"],
                minio_secret=secrets["minio"]["edge"]["secret"],
                data_classification=minio_utils.DataClassification.EDGE,
                data_versioning=False,
                file_format="csv"
            )
        assert result

        logging.info("...Wr[ote] data to Minio")
