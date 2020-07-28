# base imports
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
import json
import logging
import os
import pathlib
import sys
import tempfile
# external imports
from db_utils import minio_utils
import pandas as pd
from pandas.tseries.offsets import BDay


__author__ = "Colin Anthony"


# set the minio variables
MINIO_BUCKET = 'covid'
PREFIX_OVERRIDE = "data/private/"
LAG_ADJUSTED_OUTFILE = "ct-all-cases-lag-adjusted"
MINIO_CLASSIFICATION = minio_utils.DataClassification.EDGE
# set the dataset
LATEST_SPV_FILE = "ct_all_cases.csv"
# set the filter dates
START_DATE = "2020-04-28"


def minio_csv_to_df(minio_filename_override, minio_key, minio_secret):
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        result = minio_utils.minio_to_file(
            filename=temp_data_file.name,
            minio_filename_override=minio_filename_override,
            minio_bucket=MINIO_BUCKET,
            minio_key=minio_key,
            minio_secret=minio_secret,
            data_classification=MINIO_CLASSIFICATION,
        )
        if not result:
            logging.debug(f"Could not get data from minio bucket")
            sys.exit(-1)
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...") 
            df = pd.read_csv(temp_data_file, engine='c', encoding='ISO-8859-1')
            return df

        
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        sys.exit(-1)

    logging.info("Setting secrets variables")
    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))
    if not pathlib.Path(secrets_path).glob("*.json"):
        logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
        sys.exit(-1)
        
    # _________________________________________________________________
    # get spv latest
    logging.debug(f"Getting the latest spv data")
    spv_latest = minio_csv_to_df(
        minio_filename_override=f"{PREFIX_OVERRIDE}{LATEST_SPV_FILE}",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    
    # latest export date
    logging.debug(f"getting the latest export date")
    latest_export = pd.to_datetime(spv_latest["Export.Date"].max()).strftime("%Y-%m-%d")
    logging.debug(f"latest export date is {latest_export}")
    
    spv_latest_filt = spv_latest[["Export.Date", "Date.of.Diagnosis", "Admission.Date", "Date.of.ICU.Admission", "Date.of.Death"]].copy()
    spv_latest_filt.loc[:, "Export.Date"] = pd.to_datetime(spv_latest_filt["Export.Date"]).dt.date
    spv_latest_filt.sort_values("Date.of.Diagnosis", ascending=True, inplace=True)
    
    # get counts from spv
    logging.debug(f"getting the counts for each category in the spv data")
    spv_latest_filt['Diagnoses_count'] = (
        spv_latest_filt.groupby(["Date.of.Diagnosis"])['Date.of.Diagnosis'].transform('count'))
    spv_latest_filt['Admissions_count'] = (
        spv_latest_filt.groupby(["Admission.Date"])['Admission.Date'].transform('count'))
    spv_latest_filt['ICUAdmissions_count'] = (
        spv_latest_filt.groupby(["Date.of.ICU.Admission"])['Date.of.ICU.Admission'].transform('count'))
    spv_latest_filt['Deaths_count'] = (
        spv_latest_filt.groupby(["Date.of.Death"])['Date.of.Death'].transform('count'))
    
    # get lag day for each date
    logging.debug(f"Calculating the lag days")
    spv_latest_filt.loc[:, "diag_lag"] = (pd.to_datetime(spv_latest_filt["Export.Date"]) - pd.to_datetime(spv_latest_filt["Date.of.Diagnosis"])).dt.days
    spv_latest_filt.loc[:, "admit_lag"] = (pd.to_datetime(spv_latest_filt["Export.Date"]) - pd.to_datetime(spv_latest_filt["Admission.Date"])).dt.days
    spv_latest_filt.loc[:, "icu_lag"] = (pd.to_datetime(spv_latest_filt["Export.Date"]) - pd.to_datetime(spv_latest_filt["Date.of.ICU.Admission"])).dt.days
    spv_latest_filt.loc[:, "death_lag"] = (pd.to_datetime(spv_latest_filt["Export.Date"]) - pd.to_datetime(spv_latest_filt["Date.of.Death"])).dt.days
    
    # deduplicate diagnoses df and remove oldlder than May and weird future data errors
    diag_cnts_df = spv_latest_filt.loc[~spv_latest_filt.duplicated(["Date.of.Diagnosis"], keep="first"), 
                                       ["Export.Date", "Date.of.Diagnosis", "diag_lag", "Diagnoses_count"]]
    diag_cnts_df.sort_values("Date.of.Diagnosis", ascending=True, inplace=True)
    diag_cnts_df = diag_cnts_df.query("(`Date.of.Diagnosis` >= @START_DATE) & (`Date.of.Diagnosis` <= @latest_export)")
    diag_cnts_df.rename(columns={"diag_lag": "lag_days"}, inplace=True)
    
    # deduplicate admissions df and remove oldlder than May and weird future data errors
    admit_cnts_df = spv_latest_filt.loc[~spv_latest_filt.duplicated(["Admission.Date"], keep="first"), 
                                        ["Export.Date", "Admission.Date", "admit_lag", "Admissions_count"]]
    admit_cnts_df.sort_values("Admission.Date", ascending=True, inplace=True)
    admit_cnts_df = admit_cnts_df.query("(`Admission.Date` >= @START_DATE) & (`Admission.Date` <= @latest_export)")
    admit_cnts_df.rename(columns={"admit_lag": "lag_days"}, inplace=True)
    
    # deduplicate ICU admissions df and remove oldlder than May and weird future data errors
    icu_cnts_df = spv_latest_filt.loc[~spv_latest_filt.duplicated(["Date.of.ICU.Admission"], keep="first"), 
                                      ["Export.Date", "Date.of.ICU.Admission", "icu_lag", "ICUAdmissions_count"]]
    icu_cnts_df.sort_values("Date.of.ICU.Admission", ascending=True, inplace=True)
    icu_cnts_df = icu_cnts_df.query("(`Date.of.ICU.Admission` >= @START_DATE) & (`Date.of.ICU.Admission` <= @latest_export)")
    icu_cnts_df.rename(columns={"icu_lag": "lag_days"}, inplace=True)
    
    # deduplicate deaths df and remove oldlder than May and weird future data errors
    mort_cnts_df = spv_latest_filt.loc[~spv_latest_filt.duplicated(["Date.of.Death"], keep="first"), 
                                       ["Export.Date", "Date.of.Death", "death_lag", "Deaths_count"]]
    mort_cnts_df.sort_values("Date.of.Death", ascending=True, inplace=True)
    mort_cnts_df = mort_cnts_df.query("(`Date.of.Death` >= @START_DATE) & (`Date.of.Death` <= @latest_export)")
    mort_cnts_df.rename(columns={"death_lag": "lag_days"}, inplace=True)

    for (df, item_date, lag, cnt) in [(diag_cnts_df, "Date.of.Diagnosis", "lag_days", "Diagnoses_count"), 
               (admit_cnts_df, "Admission.Date", "lag_days", "Admissions_count"), 
               (icu_cnts_df, "Date.of.ICU.Admission", "lag_days", "ICUAdmissions_count"), 
               (mort_cnts_df, "Date.of.Death", "lag_days", "Deaths_count")]:
        df.loc[:, lag] = df[lag].astype(int)
        df.loc[:, cnt] = df[cnt].astype(int)
        
    # _________________________________________________________________
    # read in the lag distribution and make the adjustments 
    logging.debug(f"Getting the lag adjustemnt tables from  minio")
    diag_lag_df = minio_csv_to_df(
        minio_filename_override=f"{PREFIX_OVERRIDE}wc_spv_DiagnosisDate_freq_table.csv",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    admit_lag_df = minio_csv_to_df(
        minio_filename_override=f"{PREFIX_OVERRIDE}wc_spv_AdmissionDate_freq_table.csv",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    icu_lag_df = minio_csv_to_df(
        minio_filename_override=f"{PREFIX_OVERRIDE}wc_spv_ICUAdmissionDate_freq_table.csv",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )
    mort_lag_df = minio_csv_to_df(
        minio_filename_override=f"{PREFIX_OVERRIDE}wc_spv_DeathDate_freq_table.csv",
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
    )    
    
    # make the lag adjustments
    logging.debug(f"add the lag adjusted counts column to the dataframe")
    diag_cnts_df = diag_cnts_df.join(diag_lag_df.set_index("lag_days"), on="lag_days")
    diag_cnts_df.loc[:, "median"] = diag_cnts_df["median"].apply(lambda x: 1 if pd.isna(x) else x)
    diag_cnts_df.loc[:, "Diagnoses_adjcount"] = diag_cnts_df["Diagnoses_count"] / diag_cnts_df["median"]
    diag_cnts_df.rename(columns={"Date.of.Diagnosis": "Date"}, inplace=True)
    diag_cnts_df.loc[:, "Date"] = pd.to_datetime(diag_cnts_df["Date"])

    admit_cnts_df = admit_cnts_df.join(admit_lag_df.set_index("lag_days"), on="lag_days")
    admit_cnts_df.loc[:, "median"] = admit_cnts_df["median"].apply(lambda x: 1 if pd.isna(x) else x)
    admit_cnts_df.loc[:, "Admissions_adjcount"] = admit_cnts_df["Admissions_count"] / admit_cnts_df["median"]
    admit_cnts_df.rename(columns={"Admission.Date": "Date"}, inplace=True)
    admit_cnts_df.loc[:, "Date"] = pd.to_datetime(admit_cnts_df["Date"])

    icu_cnts_df = icu_cnts_df.join(icu_lag_df.set_index("lag_days"), on="lag_days")
    icu_cnts_df.loc[:, "median"] = icu_cnts_df["median"].apply(lambda x: 1 if pd.isna(x) else x)
    icu_cnts_df.loc[:, "ICUAdmissions_adjcount"] = icu_cnts_df["ICUAdmissions_count"] / icu_cnts_df["median"]
    icu_cnts_df.rename(columns={"Date.of.ICU.Admission": "Date"}, inplace=True)
    icu_cnts_df.loc[:, "Date"] = pd.to_datetime(icu_cnts_df["Date"])

    mort_cnts_df = mort_cnts_df.join(mort_lag_df.set_index("lag_days"), on="lag_days")
    mort_cnts_df.loc[:, "median"] = mort_cnts_df["median"].apply(lambda x: 1 if pd.isna(x) else x)
    mort_cnts_df.loc[:, "Deaths_adjcount"] = mort_cnts_df["Deaths_count"] / mort_cnts_df["median"]
    mort_cnts_df.rename(columns={"Date.of.Death": "Date"}, inplace=True)
    mort_cnts_df.loc[:, "Date"] = pd.to_datetime(mort_cnts_df["Date"])
    
    # _________________________________________________________________
    # create a master dataframe with the lag adjusted spv 
    logging.debug(f"generating master dataframe to plot from")
    plot_master_df = pd.DataFrame({"Date": pd.date_range(START_DATE, latest_export)})
    plot_master_df.loc[:, "Date"] = pd.to_datetime(plot_master_df["Date"])
    plot_master_df.set_index("Date", inplace=True)

    for col, values_df in [("Diagnoses", diag_cnts_df), ("Admissions", admit_cnts_df), ("ICUAdmissions", icu_cnts_df), ("Deaths", mort_cnts_df)]:
        plot_master_df[col + "_Count"] = values_df.set_index("Date")[col + "_count"]
        plot_master_df[col + "_AdjustedCount"] = values_df.set_index("Date")[col + "_adjcount"]
    plot_master_df.reset_index(inplace=True)

    result = minio_utils.dataframe_to_minio(
        plot_master_df,
        filename_prefix_override=f"{PREFIX_OVERRIDE}{LAG_ADJUSTED_OUTFILE}",
        minio_bucket=MINIO_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
        data_classification=MINIO_CLASSIFICATION,
        data_versioning=False,
        file_format="csv")
        
    if not result:
        logging.debug(f"Sending collected data to minio failed")
