# base imports
import json
import logging
import os
import sys
# external imports
from db_utils import minio_utils
import pandas as pd
# local imports
from vaccine_data_to_minio import COVID_BUCKET, EDGE_CLASSIFICATION, VAX_MERGE_STR
from vaccine_sequencing_munge import (OUTPUT_PREFIX_ANN, OUTPUT_PREFIX_AGG, VACCINE_SEQ_ANN, Q1, RISK_SCORE,
                                      minio_to_df, PARQUET_READER)
from vaccine_annotate_HR_munge import VAX_TYPE, VAX_DATE, DOSE_NO
from vaccine_register_munge import J_AND_J_VACC
from vaccine_register_willing_munge import VACCINE_REGISTER_AGG_WILLING
from vaccine_rollout_time_series import (TS_PREFIX, OUTFILE_PREFIX, AGG_TYPE, AGG_TYPE_NAMES, TOTAL_STAFF,
                                         VACCINATED, VACCINATED_CUMSUM, VACCINATED_REL, TOP_LEVEL,
                                         SECOND_DOSE, OUTOUT_COL_ORDER, AGG_LEVELS, get_agg_timeseries, get_agg_totals)

# input settings
CITY_VAX_REGISTER = f"{VACCINE_REGISTER_AGG_WILLING}.parquet"
SEQUENCING_ANNOTATED = f"{VACCINE_SEQ_ANN}.parquet"

# outfile
OUTFILE_PREFIX += "-willing"

AGG_LEVELS += [RISK_SCORE]
TOTAL_STAFF = f"{TOTAL_STAFF}{OUTFILE_PREFIX}"
VACCINATED = f"{VACCINATED}{OUTFILE_PREFIX}"
VACCINATED_CUMSUM = f"{VACCINATED_CUMSUM}{OUTFILE_PREFIX}"
VACCINATED_REL = f"{VACCINATED_REL}{OUTFILE_PREFIX}"


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
    vax_register_dfs = []
    for filename, prefix in [
        (SEQUENCING_ANNOTATED, OUTPUT_PREFIX_ANN),
        (CITY_VAX_REGISTER, OUTPUT_PREFIX_AGG),
    ]:
        df = minio_to_df(
            minio_filename_override=f"{prefix}{filename}",
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=PARQUET_READER
        )
        df[SUBDISTRICT] = df[SUBDISTRICT].str.replace("_", " ") 
        df[SUBDISTRICT] = df[SUBDISTRICT].str.replace(" support", "", case=False) 
        df[SUBDISTRICT] = df[SUBDISTRICT].str.replace("Mitchell's", "Mitchells", case=False)
        vax_register_dfs.append(df)

    # dataframes
    annotated_sequencing_df, vaccine_register_agg_df = vax_register_dfs
    annotated_sequencing_df_willing = annotated_sequencing_df.query(f"`{Q1}` == 1").copy()
    vaccine_register_agg_df_willing = vaccine_register_agg_df.query(f"`{Q1}` == '1.0'").copy()

    if annotated_sequencing_df.empty or annotated_sequencing_df.empty:
        logging.error(f"Empty df, no willing staff in dataframe")
        sys.exit(-1)

    # ----------------------------------
    # get J&J one dose vaccinations
    logging.info("Filter[ing] to J&J vaccinations")
    jj_1shot = vaccine_register_agg_df_willing.query(f"`{VAX_TYPE}` == @J_AND_J_VACC").copy()
    logging.info("Filter[ed] to J&J vaccinations")

    # if other exists, will need additional aggregation
    logging.info("Check[ing] for other vaccination types")
    # split out other vaccination types
    other_vaccines = vaccine_register_agg_df.query(f"`{VAX_TYPE}` != @J_AND_J_VACC").copy()
    vaccine_types = other_vaccines[VAX_TYPE].unique()
    other_vaccines_full = other_vaccines.query(f"`{DOSE_NO}` == @SECOND_DOSE").copy()
    logging.info("Check[ed] for other vaccination types")

    # make sure only 1st dose for J&J, aggregations assume single dose for fully vaccinated
    logging.info("Check[ing] that J&J has only dose 1 entries")
    jj_doses = jj_1shot[DOSE_NO].nunique()
    if jj_doses != 1:
        logging.error(f"{jj_doses} doses detected for J&J vaccine\nExpected only one dose")
        sys.exit(-1)
    logging.info("Check[ed] that J&J has only dose 1 entries")

    fully_vaccinated_df = pd.concat([jj_1shot, other_vaccines_full])

    # convert date format
    fully_vaccinated_df[VAX_DATE] = pd.to_datetime(fully_vaccinated_df[VAX_DATE]).dt.date

    # ----------------------------------
    # get total time series
    vaccination_totals_time_series = get_agg_totals(fully_vaccinated_df, annotated_sequencing_df_willing)
    vaccination_totals_time_series[AGG_TYPE] = TOP_LEVEL
    vaccination_totals_time_series[AGG_TYPE_NAMES] = TOP_LEVEL

    collected_agg_df = vaccination_totals_time_series.copy()
    for i, agg_level in enumerate(AGG_LEVELS):
        if agg_level == RISK_SCORE:
            fully_vaccinated_df[agg_level] = fully_vaccinated_df[agg_level].astype(float)
            fully_vaccinated_df[agg_level] = fully_vaccinated_df[agg_level].astype(int)
        logging.info("Aggregat[ing] vaccine total time series")
        staff_agg_level_ts = get_agg_timeseries(
            fully_vaccinated_df,
            agg_level,
            annotated_sequencing_df_willing,
            VAX_MERGE_STR,
        )
        collected_agg_df = pd.concat([collected_agg_df, staff_agg_level_ts])
        logging.info("Aggregat[ed] vaccine total time series")

    collected_agg_df = collected_agg_df[OUTOUT_COL_ORDER].copy()
    # convert column for parquet
    collected_agg_df[AGG_TYPE_NAMES] = collected_agg_df[AGG_TYPE_NAMES].astype(str)

    # ----------------------------------
    logging.info(f"Push[ing] data to mino for {OUTFILE_PREFIX}")
    result = minio_utils.dataframe_to_minio(
        collected_agg_df,
        COVID_BUCKET,
        secrets["minio"]["edge"]["access"],
        secrets["minio"]["edge"]["secret"],
        EDGE_CLASSIFICATION,
        filename_prefix_override=f"{TS_PREFIX}{OUTFILE_PREFIX}",
        file_format="csv",
        data_versioning=False
    )
    logging.info(f"Push[ed] data to mino for {OUTFILE_PREFIX}")

    logging.info("...Done!")
