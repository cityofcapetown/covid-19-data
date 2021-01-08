# base imports
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
import numpy as np
import pandas as pd


__author__ = "Colin Anthony"


# set the minio bucket
BUCKET = 'covid'
# set the number of data points required for calculating the median for each lag day
WINDOW = 21
# set the number of days from today to drop due to incomplete lag curves
DROP_LAST_DAYS = 10
# set the max number of lag days to calculate adjustments for
MAX_LAG_DAYS = 60
# set the filter date
TODAY = datetime.today()
START_DATE = datetime.strptime("2020-04-28", '%Y-%m-%d')
EXPORT_DATE_COL = "Export.Date"
# set the collected spv file
LATEST_COLLECTED_SPV = "spv_collected_latest.csv"
# set the override file names
OVERRIDE_WC = "data/private/spv_lag_freq_table_wc"
OVERRIDE_DIST = "data/private/spv_lag_freq_table_wc_districts"
OVERRIDE_SUBD = "data/private/spv_lag_freq_table_wc_subdistricts"
# set target columns for lag calculation
DIAGNOSIS_LAG = "Date.of.Diagnosis"
ADMISSION_LAG = "Admission.Date"
ICU_LAG = "Date.of.ICU.Admission"
DEATH_LAG = "Date.of.Death"


def minio_csv_to_df(minio_filename_override, minio_bucket, minio_key, minio_secret, data_classification):
    """Function to read in the collected covid daily linelist.csv file 

    Args:
        minio_filename_override (str): name of the minio override filepath 
        minio_bucket (str): minio bucket name
        minio_key (str): minio access key
        minio_secret (str): minio secret
        data_classification (str): minio class

    Returns:
        [Object]: Pandas DataFrame
    """
    logging.debug("Pulling data from Minio bucket...")
    with tempfile.NamedTemporaryFile() as temp_data_file:
        try:
            result = minio_utils.minio_to_file(filename=temp_data_file.name,
                                            minio_filename_override=minio_filename_override,
                                            minio_bucket=minio_bucket,
                                            minio_key=minio_key,
                                            minio_secret=minio_secret,
                                            data_classification=data_classification,
                                            )
        except Exception as e:
            logging.debug(f"Could not get data from minio bucket for {minio_filename_override}\nReturning empty dataframe")
            sys.exit(-1)
        
        else:
            logging.debug(f"Reading in raw data from '{temp_data_file.name}'...") 
            df = pd.read_csv(temp_data_file, low_memory=False)
            return df


def filter_df(linelists_df, DATE_COL_TO_USE, DROP_LAST_DAYS):
    """
    Function to filter the spv dataframe by target columnm as well as by date 
    and calculate the lag days between the export data and the target column data 
    and filter out entries with negative lag days (erroneous dates in the future)
    Args:
        linelists_df (obj): Pandas DataFrame of spv data
        DATE_COL_TO_USE (str): the column name in the df to calculate lag stats on
        DROP_LAST_DAYS (int): the number of days from today to drop due to incomplete lag curve
    Returns:
        [Object]: Pandas DataFrame
    """
    wc_all_linelists_filt = linelists_df[[EXPORT_DATE_COL, DATE_COL_TO_USE]].copy()
    wc_all_linelists_filt.loc[:, EXPORT_DATE_COL] = pd.to_datetime(wc_all_linelists_filt[EXPORT_DATE_COL], errors = 'coerce').dt.date
    wc_all_linelists_filt.loc[:, EXPORT_DATE_COL] = pd.to_datetime(wc_all_linelists_filt[EXPORT_DATE_COL], errors = 'coerce')
    wc_all_linelists_filt.loc[:, DATE_COL_TO_USE] = pd.to_datetime(wc_all_linelists_filt[DATE_COL_TO_USE], errors = 'coerce')
    
    # get the latest export date
    logging.debug(f"getting the latest export date")
    latest_export = wc_all_linelists_filt[EXPORT_DATE_COL].max()
    logging.debug(f"latest export date is {latest_export}")
    
    # calculate the lag days
    wc_all_linelists_filt.loc[:, "lag_days"] = (wc_all_linelists_filt[EXPORT_DATE_COL] - wc_all_linelists_filt[DATE_COL_TO_USE]).dt.days
    wc_all_linelists_filt.sort_values(by=[DATE_COL_TO_USE], ascending=True, inplace=True)
    
    # drop last {DROP_LAST_DAYS} days because of incomplete lag curves
    logging.debug(f"Dropping the last {DROP_LAST_DAYS} days because of incomplete lag curves")
    end_date = TODAY - timedelta(days=DROP_LAST_DAYS)
    dates = pd.date_range(end=end_date, start=START_DATE).date.tolist()
    dates = [x.strftime('%Y-%m-%d') for x in dates]
    # ensure weird future dates are filtered out
    logging.debug(f"Filtering out dates in the future from {TODAY} and before {START_DATE}")
    use_dates_df = wc_all_linelists_filt[(wc_all_linelists_filt[DATE_COL_TO_USE] <= TODAY) & 
                                         (wc_all_linelists_filt[DATE_COL_TO_USE] >= START_DATE) & 
                                         (wc_all_linelists_filt["lag_days"] > 0)].copy()
    use_dates_df.sort_values([DATE_COL_TO_USE], ascending=False, inplace=True)
    
    return use_dates_df


def get_lag_freqs(use_dates_df, DATE_COL_TO_USE, max_lag_days, min_lag_points):
    """
    Function to calculate the freq of total cases at each lag day
    Args:
        use_dates_df (obj): the filtered DataFrame of spv data with lag days from filter_df()
        DATE_COL_TO_USE (str): the column name in the df to calculate lag stats on
        max_lag_days (int): the maximum number of lag days to include for lag adjustments
        min_lag_points (int): the minimum number of lag day points to allow for inclusion 
    Returns:
        [Object]: Pandas DataFrame
    """
    cols = [DATE_COL_TO_USE, "lag_days", "count", "freq"]
    master_lag_df = pd.DataFrame(columns=cols)

    # group by target date category and calculate the freq of total cases at each lag day
    grp_diag = use_dates_df.groupby([DATE_COL_TO_USE], sort=False)
    for grp_name, grp_df in grp_diag:
        sorted_grp = grp_df.sort_values(["lag_days"]).copy()
        use_date = grp_name.date()

        # don't use data if it has an incomplete lag curve
        if grp_df["lag_days"].max() < DROP_LAST_DAYS:
            continue

        # drop lag days greater than the max specified
        use_lag_df = sorted_grp.query("lag_days <= @max_lag_days").copy()
        if use_lag_df.empty:
            logging.debug(f"no lag days less than {max_lag_days} for {use_date}")
            continue
        lag_cnts = use_lag_df.groupby(["lag_days"])[EXPORT_DATE_COL].count().reset_index()
        lag_cnts.rename(columns={EXPORT_DATE_COL: "count"},  inplace=True)

        # get the max value to represent the likely final true value
        try:
            last_val = lag_cnts["count"].iloc[-1]
        except IndexError as e:
            logging.error(f"{e}\n{use_lag_df}")
            sys.exit()

        # calcualte the frequency
        lag_cnts.loc[:, "freq"] = lag_cnts["count"] / last_val
        lag_cnts.loc[:, DATE_COL_TO_USE] = use_date

        # append the group entries into the master dataframe
        frames = [master_lag_df, lag_cnts]
        master_lag_df = pd.concat([frm[cols] for frm in frames])
    
    return master_lag_df


def get_lag_median_stdev(master_lag_df, DATE_COL_TO_USE, window):
    """
    Function to calculate the median and stdev for each lag day
    Args:
        master_lag_df (obj): the DataFrame of spv data with freq of total cases at each lag day from get_lag_freqs()
        DATE_COL_TO_USE (str): the column name in the df to calculate lag stats on
        window (int): the number of data points required for calculating the median for each lag day
    Returns:
        [Object]: Pandas DataFrame
    """
    cols = ["lag_days", "median", "stdev"]
    final_lag_df = pd.DataFrame(columns=cols)

    lag_days = []
    lag_medians = []
    stderrs = []

    # group by lag day and get the median and std using the most recent {window} of entries
    lag_avr_df = master_lag_df.groupby(["lag_days"], sort=False)
    for lag_day, grp_df in lag_avr_df:
        sorted_grp = grp_df.sort_values([DATE_COL_TO_USE], ascending=False).copy()
        # get the most recent {window} of values for the lag day
        use_lag_df = grp_df.iloc[0:window,:]
        lag_days.append(lag_day)
        lag_medians.append(use_lag_df["freq"].median())
        stderrs.append(use_lag_df["freq"].std())

    # collect lists into dataframe
    final_lag_df.loc[:, "lag_days"] = lag_days
    final_lag_df.loc[:, "median"] = lag_medians
    final_lag_df.loc[:, "stdev"] = stderrs
    final_lag_df.sort_values("lag_days", ascending=True, inplace=True)
    return final_lag_df


def district_label_fix(x, y):
    if x == "Unallocated" and not pd.isna(y):
        return y
    else:
        return x


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
    
    # import the collected spv data
    logging.debug(f"Get the linelist data from minio")
    target_file = f"data/private/{LATEST_COLLECTED_SPV}"
    spv_linelists_df = minio_csv_to_df(
            minio_filename_override=target_file,
            minio_bucket=BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=minio_utils.DataClassification.EDGE,
        )
    spv_linelists_df.fillna(0, inplace=True)
    if spv_linelists_df.empty:
        logging.debug(f"dataframe was empty for target file {target_file}\nexiting")
        sys.exit(-1)
    
    # check which naming system was used in the latest export
    latest_export = spv_linelists_df["Export.Date"].max()
    check = spv_linelists_df.query("`Export.Date` == @latest_export") 
    if any(check["Subdistrict"].str.contains(" - ")):
        new_name_system = True
    else:
        new_name_system = False
    # specify the target column for lag calculation
    logging.info("Setting the target column for lag calculations")
    all_lag_columns = [DIAGNOSIS_LAG, ADMISSION_LAG, ICU_LAG, DEATH_LAG]
    
    # initialize the output dataframe for subdistricts
    out_lags_df_subd = pd.DataFrame()
    
    # get the lag distributions for all the subdistricts if there is enough data
    logging.debug("Getting lag distribution for all subdistricts where possible")
    
    ###################################
    ###########
    # correct the District value where its unallocated and subdistric is not unallocated
    
    # get the district value from the subdistrict name if present else nan
    spv_linelists_df.loc[:, "new_d"] = spv_linelists_df["Subdistrict"].apply(lambda x: x.split(" - ")[0] if len(x.split(" - ")) > 1 else np.nan)
    
    # update the district name
    spv_linelists_df.loc[:, "District"] = spv_linelists_df.apply(lambda df: district_label_fix(df["District"], df["new_d"]), axis="columns")
    spv_linelists_df.drop(columns="new_d", inplace=True)

    # get the correct subdistrict name
    spv_linelists_df.loc[:, "Subdistrict"] = spv_linelists_df["Subdistrict"].apply(lambda x: x.split(" - ")[1] if len(x.split(" - ")) > 1 else x)
    
    ############
    ############
    if new_name_system:
        # add district name back in for the merge operations when adjusting plot values 
        spv_linelists_df.loc[:, "Subdistrict"] = spv_linelists_df['District'] + " - " + spv_linelists_df["Subdistrict"]
        spv_linelists_df.loc[:, "Subdistrict"] = spv_linelists_df["Subdistrict"].str.replace("Unallocated - Unallocated", "Unallocated")
   
    ############
    ###################################

    for (dist, subd), dist_subd_df in spv_linelists_df.groupby(["District", "Subdistrict"]):
        for date_col_to_use in all_lag_columns:
            logging.info(f"Setting the target column for lag calculations to {date_col_to_use}")

            # drop last {DROP_LAST_DAYS} days because of incomplete lag curves and drop the days before data collection was relevant
            logging.info("Filtering to specific columns and converting to datetime format and Calculating the lag days")
            logging.debug(f"working on {dist} : {subd}")
            use_dates_df = filter_df(dist_subd_df, date_col_to_use, DROP_LAST_DAYS)

            if use_dates_df.empty:
                logging.error(f"Filtering returned an empty datagrame for {dist}: {subd}: {date_col_to_use}\nSkipping")
                continue

            #  calculate the freq of total cases at each lag day for each target date
            logging.info(f"calculating the freq of total cases at each lag day for each target date")
            master_lag_df = get_lag_freqs(use_dates_df, date_col_to_use, MAX_LAG_DAYS, DROP_LAST_DAYS)

            if master_lag_df.empty:
                logging.error(f"Freq calculation returned an empty datagrame for {dist}: {subd}: {date_col_to_use}\nSkipping")
                continue
            # calculate the median and stdev for each lag day
            logging.info(f"calculating the median and stdev for each lag day")
            final_lag_df = get_lag_median_stdev(master_lag_df, date_col_to_use, WINDOW)
            if final_lag_df.empty:
                logging.error(f"Median calculation returned an empty datagrame for {dist}: {subd}: {date_col_to_use}\nSkipping")
                continue
            
            # add the district and subdistrict columns
            final_lag_df.loc[:, "District"] = dist
            final_lag_df.loc[:, "Subdistrict"] = subd
            final_lag_df.loc[:, "lag_type"] = date_col_to_use
            
            # concatentate to the main outfule
            logging.debug(f"Concatenating subdistrict lags to master dataframe")
            out_lags_df_subd = pd.concat([out_lags_df_subd, final_lag_df])

    # get the lags for whole districts
    # initialize the output dataframe for Districts
    out_lags_df_dist = pd.DataFrame()

    logging.debug("Getting lag distribution for all districts")
    for dist, dist_df in spv_linelists_df.groupby(["District"]):
        for date_col_to_use in all_lag_columns:
            logging.info(f"Setting the target column for lag calculations to {date_col_to_use}")

            # drop last {DROP_LAST_DAYS} days because of incomplete lag curves and drop the days before data collection was relevant
            logging.info("Filtering to specific columns and converting to datetime format and Calculating the lag days")
            use_dates_df = filter_df(dist_df, date_col_to_use, DROP_LAST_DAYS)

            if use_dates_df.empty:
                logging.error(f"Filtering returned an empty dataframe for {dist}: {subd}: {date_col_to_use}\nSkipping")
                continue

            #  calculate the freq of total cases at each lag day for each target date
            logging.info(f"calculating the freq of total cases at each lag day for each target date")
            master_lag_df = get_lag_freqs(use_dates_df, date_col_to_use, MAX_LAG_DAYS, DROP_LAST_DAYS)

            if master_lag_df.empty:
                logging.error(f"Freq calculation returned an empty dataframe for {dist}: {subd}: {date_col_to_use}\nSkipping")
                continue
            # calculate the median and sem for each lag day
            logging.info(f"calculating the median and sem for each lag day")
            final_lag_df = get_lag_median_stdev(master_lag_df, date_col_to_use, WINDOW)
            if final_lag_df.empty:
                logging.error(f"Median calculation returned an empty dataframe for {dist}: {subd}: {date_col_to_use}\nSkipping")
                continue
            
            # add the district and subdistrict columns
            final_lag_df.loc[:, "District"] = dist
            final_lag_df.loc[:, "Subdistrict"] = np.nan
            final_lag_df.loc[:, "lag_type"] = date_col_to_use
            
            # concatentate to the main outfule
            logging.debug(f"Concatenating district lags to master dataframe")
            out_lags_df_dist = pd.concat([out_lags_df_dist, final_lag_df])

    # get the lag for the whole of the WC
    # initialize the output dataframe for the WC
    out_lags_df_wc = pd.DataFrame()

    logging.info(f"Setting the target column for lag calculations to {date_col_to_use}")
    for date_col_to_use in all_lag_columns:
        # drop last {DROP_LAST_DAYS} days because of incomplete lag curves and drop the days before data collection was relevant
        logging.info("Filtering to specific columns and converting to datetime format and Calculating the lag days")
        use_dates_df = filter_df(spv_linelists_df, date_col_to_use, DROP_LAST_DAYS)

        if use_dates_df.empty:
            logging.error(f"Filtering returned an empty datagrame for WC linelist {date_col_to_use}\nSkipping")
            continue

        #  calculate the freq of total cases at each lag day for each target date
        logging.info(f"calculating the freq of total cases at each lag day for each target date")
        master_lag_df = get_lag_freqs(use_dates_df, date_col_to_use, MAX_LAG_DAYS, DROP_LAST_DAYS)

        if master_lag_df.empty:
            logging.error(f"Freq calculation returned an empty datagrame for for WC linelist: {date_col_to_use}\nSkipping")
            continue
        # calculate the median and sem for each lag day
        logging.info(f"calculating the median and sem for each lag day")
        final_lag_df = get_lag_median_stdev(master_lag_df, date_col_to_use, WINDOW)
        if final_lag_df.empty:
            logging.error(f"Median calculation returned an empty datagrame for for WC linelist: {date_col_to_use}\nSkipping")
            continue
        # add the district and subdistrict columns
        final_lag_df.loc[:, "District"] = np.nan
        final_lag_df.loc[:, "Subdistrict"] = np.nan
        final_lag_df.loc[:, "lag_type"] = date_col_to_use

        # concatentate to the main outfule
        logging.debug(f"Concatenating WC linelist lags to master dataframe")
        out_lags_df_wc = pd.concat([out_lags_df_wc, final_lag_df])

    # put the file in minio
    logging.info(f"Sending lag dataframes to minio")
    all_lag_tupples = ((OVERRIDE_SUBD, out_lags_df_subd), (OVERRIDE_DIST, out_lags_df_dist), (OVERRIDE_WC, out_lags_df_wc))
    for override_name, lag_df in all_lag_tupples:
        logging.debug(f"sending {override_name} lag to minio")
        result = minio_utils.dataframe_to_minio(
            lag_df,
            minio_bucket="covid",
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=minio_utils.DataClassification.EDGE,
            filename_prefix_override=override_name,
            data_versioning=False,
            file_format="csv")

        if not result:
            logging.debug(f"Sending collected data to minio failed")

    logging.info(f"Done")
