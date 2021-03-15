"""
This script takes the spv linelist and outputs lag adjusted values for covid cases in tidy format for CT subdistricts
the metro and WC non-metro areas
"""

__author__ = "Colin Anthony"

# base imports
import json
import logging
import os
import pathlib
import sys
import tempfile
# external imports
from db_utils import minio_utils
import pandas as pd


# data settings
COVID_BUCKET = "covid"
RESTRICTED_PREFIX = "data/private/"
WC_CASES = "wc_all_cases.csv"
DISTRICT_LAG = "spv_lag_freq_table_wc_districts.csv"
SUBDISTRICT_LAG = "spv_lag_freq_table_wc_subdistricts.csv"
WC_LAG = "spv_lag_freq_table_wc.csv"

CASES_ADJUSTED_PREFIX = "spv_cases_lag_adjusted_metro"
HOSP_ADJUSTED_PREFIX = "spv_hosp_lag_adjusted_metro"
ICU_ADJUSTED_PREFIX = "spv_icu_lag_adjusted_metro"
DEATHS_ADJUSTED_PREFIX = "spv_deaths_lag_adjusted_metro"

EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE

SUBDISTRICTS = {'Northern', 'Southern', 'Eastern', 'Western',
                'Mitchells Plain', 'Tygerberg', 'Khayelitsha', 'Klipfontein'}
CT_CITY = 'City of Cape Town'
CT_PREFIX_STRIP = f"{CT_CITY} - "
DIAGNOSIS = "Date.of.Diagnosis"
HOSP = "Admission.Date"
ICU = "Date.of.ICU.Admission"
DEATH = "Date.of.Death"
EXPORT = "Export.Date"
SUBDISTRICT = "Subdistrict"
DISTRICT = "District"

DATE = "Date"
LAG_DAY = "lag_day"
LAG_TYPE = 'lag_type'
METRO_MEDIAN = "CT_Metro_median"
NON_METRO_MEDIAN = "Non_Metro_median"
MEDIAN = "median"
NON_METRO_LABEL = "Non_Metro_(WC)"
CT_METRO_LABEL = "CT_Metro"

METRO_LEVEL_COLS = [
    EXPORT, DATE, LAG_DAY,
    CT_METRO_LABEL, NON_METRO_LABEL,
    CT_METRO_LABEL + "_(lag_adjusted)", NON_METRO_LABEL + "_(lag_adjusted)"
]
SUBD_COLS = [f"{subd.replace(' ', '_')}" for subd in SUBDISTRICTS]
SUBD_LAG_COLS = [f"{subd.replace(' ', '_')}_(lag_adjusted)" for subd in SUBDISTRICTS]
SUBD_LEVEL_COLS = [EXPORT, DATE, LAG_DAY] + SUBD_COLS + SUBD_LAG_COLS


SECRETS_PATH_VAR = "SECRETS_PATH"


def minio_csv_to_df(minio_filename_override, minio_bucket, minio_key, minio_secret):
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        minio_result = minio_utils.minio_to_file(
            filename=temp_data_file.name,
            minio_filename_override=minio_filename_override,
            minio_bucket=minio_bucket,
            minio_key=minio_key,
            minio_secret=minio_secret,
            data_classification=EDGE_CLASSIFICATION,
        )
        if not minio_result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...")
            fetched_df = pd.read_csv(temp_data_file.name, engine='c', encoding='ISO-8859-1')
            return fetched_df


def write_to_minio(bucket, prefix, secret_key, secret_access, dataframe, out_file):
    # write to minio
    logging.info(f"Push[ing] {out_file} data to minio")
    minio_result = minio_utils.dataframe_to_minio(
        dataframe,
        filename_prefix_override=f"{prefix}{out_file}",
        minio_bucket=bucket,
        minio_key=secret_key,
        minio_secret=secret_access,
        data_classification=EDGE_CLASSIFICATION,
        data_versioning=False,
        file_format="csv")

    if not minio_result:
        logging.debug(f"Sending collected data to minio failed")
        sys.exit(-1)
    logging.info(f"Push[ed] {out_file} to minio")

    return minio_result


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    logging.info(f"Fetch[ing] secrets")
    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        logging.info("Trying to load from file")
        secrets_file = pathlib.Path("/home/jovyan/secrets/secrets.json").resolve()
        if not secrets_file.exists():
            print("could not find your secrets")
            sys.exit(-1)
        else:
            secrets = json.load(open(secrets_file))
            logging.info("Loaded secrets from file")
    else:
        logging.info("Setting secrets variables")
        secrets_file = os.environ[SECRETS_PATH_VAR]
        if not pathlib.Path(secrets_file).exists():
            logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
            sys.exit(-1)
        secrets = json.load(open(secrets_file))
    logging.info(f"Fetch[ed] secrets")

    logging.info(f"Fetch[ing] data from minio")
    spv_latest = minio_csv_to_df(
        minio_filename_override=f"{RESTRICTED_PREFIX}{WC_CASES}",
        minio_bucket=COVID_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )

    wc_lag_adjust = minio_csv_to_df(
        minio_filename_override=f"{RESTRICTED_PREFIX}{WC_LAG}",
        minio_bucket=COVID_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )

    wc_lag_adjust.rename(columns={"lag_days": LAG_DAY}, inplace=True)
    logging.info(f"Fetch[ed] data from minio")

    # add the lag day
    spv_latest_select = spv_latest.copy()
    spv_latest_select.loc[:, EXPORT] = pd.to_datetime(spv_latest_select[EXPORT], format='%b  %d %Y %I:%M%p').dt.date
    spv_latest_select.sort_values(DIAGNOSIS, ascending=True, inplace=True)
    for kind, outname in [
        (DIAGNOSIS, CASES_ADJUSTED_PREFIX),
        (HOSP, HOSP_ADJUSTED_PREFIX),
        (ICU, ICU_ADJUSTED_PREFIX),
        (DEATH, DEATHS_ADJUSTED_PREFIX),
    ]:
        logging.info(f"process[ing] {kind} data")
        logging.info(f"Filter[ing] to {kind}")
        df = spv_latest_select[[EXPORT, kind, SUBDISTRICT]].copy()
        if df.empty:
            logging.error(f"Empty dataframe for {kind}")
            sys.exit(-1)

        logging.info(f"Add[ing] lag day value")
        df.loc[:, LAG_DAY] = (
                    pd.to_datetime(df[EXPORT], format='%Y-%m-%d') - pd.to_datetime(df[kind], format='%Y-%m-%d')).dt.days

        # fix subdistrict names
        logging.info("Fix[ing] subdistrict names")
        df[SUBDISTRICT] = df[SUBDISTRICT].apply(
            lambda x: x.replace(CT_PREFIX_STRIP, ""))
        logging.info("Fix[ed] subdistrict names")

        # split out metro and non-metro
        logging.info("Filter[ing] to metro and non-metro df's")
        non_metro = df.query(
            f"{SUBDISTRICT} not in (@SUBDISTRICTS)").copy().dropna()
        metro = df.query(
            f"{SUBDISTRICT}.isin(@SUBDISTRICTS)").copy().dropna()
        logging.info("Filter[ed] to metro and non-metro df's")

        assert not non_metro.empty, "No values in cases datafile outside of Metro"
        assert not metro.empty, "No values in cases datafile inside Metro"

        # aggreagate by non-metro
        logging.info("Calculat[ing] non-metro aggregations")
        non_metro_agg = non_metro.groupby([EXPORT, kind, LAG_DAY]).agg(
            count=(SUBDISTRICT, "count")
        ).reset_index()
        non_metro_agg[SUBDISTRICT] = NON_METRO_LABEL
        logging.info("Calculat[ed] non-metro aggregations")

        # metro subdistrict
        logging.info("Calculat[ing] subdistrict aggregations")
        metro_agg = metro.groupby([EXPORT, kind, SUBDISTRICT, LAG_DAY]).agg(
            count=(EXPORT, "count")
        ).reset_index()
        logging.info("Calculat[ed] subdistrict aggregations")

        # metro whole
        logging.info("Calculat[ing] metro aggregations")
        metro_whole_agg = metro.groupby([EXPORT, kind, LAG_DAY]).agg(
            count=(EXPORT, "count")
        ).reset_index()
        metro_whole_agg[SUBDISTRICT] = CT_METRO_LABEL
        logging.info("Calculat[ed] metro aggregations")

        # join them all together
        logging.info("Concatenat[ing] dataframes")
        combined_df = pd.concat([metro_agg, non_metro_agg, metro_whole_agg])
        logging.info("Concatenat[ed] dataframes")

        # convert dates
        combined_df.loc[:, DATE] = pd.to_datetime(combined_df[kind], format='%Y-%m-%d')

        # pivot df
        logging.info("Pivot[ing] dataframe to tidy format")
        subdist_wide = combined_df.pivot(
            index=[EXPORT, DATE, LAG_DAY],
            columns=SUBDISTRICT,
            values="count"
        ).reset_index().fillna(0)
        logging.info("Pivot[ed] dataframe to tidy format")

        # get the lag adjust values
        logging.info("Lag Adjust[ing] metro and non-metro counts")

        # convert to int to allow merge on lag day key
        subdist_wide[LAG_DAY] = subdist_wide[LAG_DAY].astype(int)

        # merge the lag adjustment values
        subdist_wide_lag = pd.merge(
            subdist_wide,
            wc_lag_adjust.query(f"{LAG_TYPE} == @kind")[[LAG_DAY, MEDIAN]],
            on=LAG_DAY,
            how="left",
            validate="1:1"
        ).fillna(1)

        # do the adjustment
        subdist_wide_lag[f"{CT_METRO_LABEL}_(lag_adjusted)"] = (
                subdist_wide_lag[CT_METRO_LABEL] / subdist_wide_lag[MEDIAN])
        subdist_wide_lag[f"{NON_METRO_LABEL}_(lag_adjusted)"] = (
                subdist_wide_lag[NON_METRO_LABEL] / subdist_wide_lag[MEDIAN])

        subdist_wide_adjust = subdist_wide_lag.copy()
        logging.info("Lag Adjust[ed] metro and non-metro counts")

        logging.info("Lag Adjust[ing] subdistrict counts")
        for subd in SUBDISTRICTS:
            subdist_wide_adjust[f"{subd}_(lag_adjusted)"] = (
                    subdist_wide_adjust[subd] / subdist_wide_adjust[MEDIAN])

        # drop the median column
        subdist_wide_adjust.drop(columns=MEDIAN, inplace=True)

        logging.info("Lag Adjust[ed] subdistrict counts")
        subdist_wide_adjust.rename(
            columns={col: col.replace(" ", "_") for col in subdist_wide_adjust.columns},
            inplace=True
        )

        # separate metro from subdistrict level
        logging.info("Separat[ing] metro and subdistrict data")
        metro_level_df = subdist_wide_adjust[METRO_LEVEL_COLS].copy()
        metro_subd_level_df = subdist_wide_adjust[SUBD_LEVEL_COLS].copy()
        logging.info("Separat[ed] metro and subdistrict data")

        for df, outfile in [
            (metro_level_df, outname),
            (metro_subd_level_df, f"{outname}_subdistricts"),
        ]:

            # write to minio
            logging.info(f"Push[ing] {outfile} data to minio")
            result = write_to_minio(
                COVID_BUCKET, RESTRICTED_PREFIX,
                secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                df, outfile
            )
            logging.info(f"Push[ed] {outfile} to minio")

        logging.info("Done")
