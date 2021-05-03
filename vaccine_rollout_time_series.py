# base imports
import json
import logging
import os
import sys
# external imports
from db_utils import minio_utils
import pandas as pd
import numpy as np
# local imports
from vaccine_data_to_minio import COVID_BUCKET, EDGE_CLASSIFICATION
from vaccine_sequencing_munge import (OUTPUT_PREFIX_ANN, OUTPUT_PREFIX_AGG, UNIQUE_STAFF_LIST_ANN, DIRECTORATE_COL,
                                      DEPARTMENT_COL, SUBDISTRICT, BRANCH, FACILITY, POSITION, GENDER, HR_MASTER_DATA,
                                      STAFF_NO, minio_to_df, CSV_READER, PARQUET_READER, RISK_SCORE)
from vaccine_annotate_HR_munge import VAX_TYPE, VAX_DATE, DOSE_NO
from vaccine_register_munge import VACCINE_REGISTER_AGG, COUNT, J_AND_J_VACC
from vaccine_register_willing_munge import VACCINE_REGISTER_AGG_WILLING, STAFF_TYPE

# input settings
STAFF_DF = f"{UNIQUE_STAFF_LIST_ANN}.parquet"
STAFF_DF_OVERRIDE = f"{OUTPUT_PREFIX_ANN}{STAFF_DF}"
STAFF_READER = PARQUET_READER
CITY_VAX_REGISTER_OVERRIDE = f"{OUTPUT_PREFIX_AGG}{VACCINE_REGISTER_AGG_WILLING}.parquet"

# outfile
TS_PREFIX = "data/private/staff_vaccine/time_series/"
OUTFILE_PREFIX = f"staff-vaccination-time-series"

AGG_TYPE = "aggregation_level"
AGG_TYPE_NAMES = "aggregation_level_names"
TOP_LEVEL = "city_wide"
SECOND_DOSE = "2nd Dose"
TOTAL_STAFF = "total_staff"
VACCINATED = "vaccinated"
VACCINATED_CUMSUM = "vaccinated_cumsum"
VACCINATED_REL = "vaccinated_relative"

AGG_LEVELS = [DIRECTORATE_COL, DEPARTMENT_COL, BRANCH, FACILITY, POSITION, GENDER, SUBDISTRICT, STAFF_TYPE]
OUTOUT_COL_ORDER = [VAX_DATE, AGG_TYPE, AGG_TYPE_NAMES, TOTAL_STAFF, VACCINATED, VACCINATED_CUMSUM,
                    VACCINATED_REL] + AGG_LEVELS

AGG_GROUP_DICT = {
    DIRECTORATE_COL: [DIRECTORATE_COL],
    DEPARTMENT_COL: [DIRECTORATE_COL, DEPARTMENT_COL],
    BRANCH: [DIRECTORATE_COL, DEPARTMENT_COL, BRANCH],
    FACILITY: [FACILITY],
    POSITION: [POSITION],
    GENDER: [GENDER],
    SUBDISTRICT: [SUBDISTRICT],
    STAFF_TYPE: [STAFF_TYPE],
    RISK_SCORE: [RISK_SCORE],
}


def get_agg_timeseries(vacc_df, agg_col, staff_totals_df, staff_col):
    agg_cols = AGG_GROUP_DICT[agg_col]
    vaccination_time_series = vacc_df.groupby([VAX_DATE] + agg_cols).agg(
        **{VACCINATED: (COUNT, "sum")}
    ).reset_index()

    # add staff totals
    staff_totals_counts = staff_totals_df.groupby(agg_cols).agg(
        **{TOTAL_STAFF: (staff_col, "count")}
    ).reset_index()

    staff_agg_ts = pd.merge(
        vaccination_time_series,
        staff_totals_counts,
        on=agg_cols,
        how="left",
        validate="m:1"
    )

    staff_agg_ts.sort_values([VAX_DATE], ascending=True, inplace=True)
    staff_agg_ts[VACCINATED_CUMSUM] = staff_agg_ts.groupby(agg_cols, sort=False).agg(
        **{VACCINATED_CUMSUM: (VACCINATED, "cumsum")}
    )

    staff_agg_ts[VACCINATED_REL] = staff_agg_ts[VACCINATED_CUMSUM] / staff_agg_ts[TOTAL_STAFF]
    staff_agg_ts[AGG_TYPE_NAMES] = staff_agg_ts[agg_col]
    staff_agg_ts[AGG_TYPE] = agg_col

    return staff_agg_ts


def get_agg_totals(vacc_df, staff_totals_df):
    total_staff = staff_totals_df[STAFF_NO].nunique()

    vaccination_totals_time_series = vacc_df.groupby(VAX_DATE).agg(
        **{VACCINATED: (COUNT, "sum")}
    ).reset_index()

    # annotate with staff target totals
    vaccination_totals_time_series[VACCINATED_CUMSUM] = vaccination_totals_time_series[VACCINATED].cumsum()
    vaccination_totals_time_series[TOTAL_STAFF] = total_staff
    vaccination_totals_time_series[VACCINATED_REL] = (
            vaccination_totals_time_series[VACCINATED_CUMSUM] / vaccination_totals_time_series[TOTAL_STAFF]
    )

    return vaccination_totals_time_series


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
    for (filename_override, reader) in [(CITY_VAX_REGISTER_OVERRIDE, PARQUET_READER),
                                        (STAFF_DF_OVERRIDE, STAFF_READER)
                                        ]:
        df = minio_to_df(
            minio_filename_override=filename_override,
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=reader
        )
        df[SUBDISTRICT] = df[SUBDISTRICT].str.replace("_", " ") 
        df[SUBDISTRICT] = df[SUBDISTRICT].str.replace(" support", "", case=False) 
        df[SUBDISTRICT] = df[SUBDISTRICT].str.replace("Mitchell's", "Mitchells", case=False)
        df[SUBDISTRICT] = df[SUBDISTRICT].str.replace("Nothern", "Northern", case=False)
        vax_register_dfs.append(df)
    
    # dataframes
    vaccine_register_agg_df, staff_list_df = vax_register_dfs
    
    
    # ----------------------------------
    # get J&J one dose vaccinations
    logging.info("Filter[ing] to J&J vaccinations")
    jj_1shot = vaccine_register_agg_df.query(f"`{VAX_TYPE}` == @J_AND_J_VACC").copy()
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
    logging.info("Calculat[ing] top level aggregation")
    vaccination_totals_time_series = get_agg_totals(fully_vaccinated_df, staff_list_df)
    vaccination_totals_time_series[AGG_TYPE] = TOP_LEVEL
    vaccination_totals_time_series[AGG_TYPE_NAMES] = TOP_LEVEL
    logging.info("Calculat[ing] top level aggregation")
    
    collected_agg_df = vaccination_totals_time_series.copy()
    for i, agg_level in enumerate(AGG_LEVELS):
        logging.info("Aggregat[ing] vaccine total time series")
        staff_agg_level_ts = get_agg_timeseries(
            fully_vaccinated_df,
            agg_level,
            staff_list_df,
            STAFF_NO,
        )
        collected_agg_df = pd.concat([collected_agg_df, staff_agg_level_ts])
        logging.info("Aggregat[ed] vaccine total time series")

    collected_agg_df = collected_agg_df[OUTOUT_COL_ORDER].copy()

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
