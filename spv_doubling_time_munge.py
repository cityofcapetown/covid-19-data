"""
This script takes the calculated rolling mean of cases and deaths for CT subdistricts, the metro and WC non-metro areas
and calculates the growth rate and doubling time
"""

__author__ = "Colin Anthony"

# base imports
from enum import Enum
import json
import logging
import os
import pathlib
import sys
import tempfile
# external imports
from db_utils import minio_utils
import pandas as pd
import numpy as np
# local imports
from spv_metro_subdistricts_munge import minio_csv_to_df, write_to_minio


# minio settings
COVID_BUCKET = "covid"
RESTRICTED_PREFIX = "data/private/"
CASES_ADJUSTED_METRO = "spv_cases_lag_adjusted_metro.csv"
DEATHS_ADJUSTED_METRO = "spv_deaths_lag_adjusted_metro.csv"
CASES_ADJUSTED_METRO_SUBD = "spv_cases_lag_adjusted_metro_subdistricts.csv"
DEATHS_ADJUSTED_METRO_SUBD = "spv_deaths_lag_adjusted_metro_subdistricts.csv"
CASES_DT_OUTFILE_PREFIX = "spv_cases_doubling_time_metro"
DEATHS_DT_OUTFILE_PREFIX = "spv_deaths_doubling_time_metro"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE

# column constants
DATE = "Date"
DAYS = "Days"
CASES = "Cases"
DEATHS = "Deaths"
CUM_VALS = "Cumulative_Values"
DAILY_VALS = "Daily_Values"
PREV_WEEK = 'Previous_Week'
WEEKLY_DIFF = "Weekly_Delta"
PERCENT_CHANGE = "Relative_Weekly_Delta"
ACTIVE_COUNTS = "Presumed_Active_Counts"
ACTIVE_CHANGE = "Presumed_Active_Counts_Delta"
ACTIVE_CHANGE_DELTA = "Relative_Presumed_Active_Counts_Delta"
GROWTH_RATE = "Growth_Decay_Rate"
DOUBLE_TIME = "Doubling_Time"
DISTRICT = "District"
SUBDISTRICT = "Subdistrict"
REGION = "Region"
METRO = "CT_Metro_(lag_adjusted)"
NON_METRO = "Non_Metro_(WC)_(lag_adjusted)"
METRO_REGIONS = [METRO, NON_METRO]
SUBD_REGIONS = [
    'Northern_(lag_adjusted)',
    'Southern_(lag_adjusted)',
    'Eastern_(lag_adjusted)',
    'Western_(lag_adjusted)',
    'Mitchells_Plain_(lag_adjusted)',
    'Tygerberg_(lag_adjusted)',
    'Khayelitsha_(lag_adjusted)',
    'Klipfontein_(lag_adjusted)'
]

# settings
START_DATE = "2020-03-27"

# set the window size (in days) for comparison of rates and changes
WINDOW = 7

# secrets var
SECRETS_PATH_VAR = "SECRETS_PATH"


class DoublingMethod:
    """
    Class to hold functions for the different doubling time algorithms
    """
    def __init__(self, growth_rate):
        self.growth_rate = growth_rate * 100

    def default(self):
        """
        Method for calculating doubling time from the growth rate, with higher precision than Rule of 69
        Returns:
            dbl_time (float): the doubling time
        """
        dbl_time = (np.log(2) * 100) / self.growth_rate
        return dbl_time

    def rule69(self):
        """
        Rule 69 method for calculating doubling time from the growth rate
        Returns:
            dbl_time (float): the doubling time
        """
        dbl_time = 69 / self.growth_rate
        return dbl_time

    def padeapproximant(self):
        """
        Pade Approximant method for calculating doubling time from the growth rate
        Returns:
            dbl_time (float): the doubling time
        """
        dbl_time = (69.3 / self.growth_rate) * ((600 + (4 * self.growth_rate)) / (600 + self.growth_rate))
        return dbl_time

    def emrule(self):
        """
        E-M Rule method for calculating doubling time from the growth rate
        Returns:
            dbl_time (float): the doubling time
        """
        dbl_time = 70 / (self.growth_rate * (198 / (200 - self.growth_rate)))
        return dbl_time


def percent_change_calc(src_df, region_col):
    """
    calculate the percent change week on week
    Args:
        src_df (dataframe): the pandas dataframe with time series data
        region_col (str): column name of the region to use

    Returns:
        pct_change_df (dataframe): pandas dataframe with date and percentage change columns
    """

    pct_change_df = src_df[[DATE, region_col]].set_index(DATE).copy()
    if pct_change_df.empty:
        logging.error(f"Empty Dataframe for {region_col}")
        sys.exit(-1)

    # get rolling mean
    pct_change_df[region_col] = pct_change_df[region_col].rolling(WINDOW).mean()
    # add the column with the previous weeks values
    pct_change_df[PREV_WEEK] = pct_change_df.shift(periods=WINDOW)
    pct_change_df.reset_index(inplace=True)

    # calculate the % change
    pct_change_df[WEEKLY_DIFF] = pct_change_df[region_col] - pct_change_df[PREV_WEEK]
    pct_change_df[PERCENT_CHANGE] = pct_change_df[WEEKLY_DIFF] / pct_change_df[PREV_WEEK]

    pct_change_df_filt = pct_change_df[[DATE, PERCENT_CHANGE]].copy()
    pct_change_df_filt[DATE] = pct_change_df_filt[DATE].astype(str)

    return pct_change_df_filt


def active_case_change(src_df, region_col):
    """
    calculate the percent change week on week
    Args:
        src_df (dataframe): the pandas dataframe with time series data
        region_col (str): column name of the region to use

    Returns:
        pct_change_df (dataframe): pandas dataframe with date and percentage change columns
    """

    active_change_df = src_df[[DATE, region_col]].set_index(DATE).copy()
    if active_change_df.empty:
        logging.error(f"Empty Dataframe for {region_col}")
        sys.exit(-1)

    # get rolling mean
    active_change_df.rename(columns={region_col: ACTIVE_COUNTS}, inplace=True)

    active_change_df[ACTIVE_COUNTS] = active_change_df[ACTIVE_COUNTS].rolling(14).sum()  # .resample('W-MON').median()
    # add the column with the previous weeks values
    active_change_df[PREV_WEEK] = active_change_df.shift(periods=WINDOW)
    active_change_df.reset_index(inplace=True)

    # calculate the % change
    active_change_df[ACTIVE_CHANGE] = active_change_df[ACTIVE_COUNTS] - active_change_df[PREV_WEEK]
    active_change_df[ACTIVE_CHANGE_DELTA] = active_change_df[ACTIVE_CHANGE] / active_change_df[PREV_WEEK]
    active_change_df_filt = active_change_df[[DATE, ACTIVE_COUNTS, ACTIVE_CHANGE, ACTIVE_CHANGE_DELTA]].copy()
    active_change_df_filt[DATE] = active_change_df_filt[DATE].astype(str)

    return active_change_df_filt


def get_values(index, x_pts, y_pts):
    """
    function to get the data points for further calculations
    Args:
        index (int): the list index position for time zero
        x_pts (list): the list of dates
        y_pts (list): the list of cumulative count values

    Returns:
        days (int): the number of days between time point 1 and time point 2
        y1 (int): the value at time point 1
        y2 (int): the value at time point 2
    """
    if (index + WINDOW) > len(x_pts):
        raise IndexError
    if len(x_pts) != len(y_pts):
        raise ValueError
    x1 = x_pts[index]
    x2 = x_pts[index + WINDOW]
    days = x2 - x1
    if days < 1:
        logging.error("Can't have less than 1 day between intervals for daily growth rate")
        sys.exit()

    y1 = y_pts[index]
    y2 = y_pts[index + WINDOW]

    return days, y1, y2


def growth_decay_rate_calculator(days, y1, y2):
    """
    Calculates the exponential growth/decay rate
    Args:
        days (int): number of days between two observations
        y1: the initial observation
        y2: the new observation

    Returns:
        growth_rate (float): the exponential growth/decay rate
    """
    if days < 1:
        logging.error("Can't have less than 1 day between intervals for daily growth rate")
        sys.exit()

    if y1 == 0:
        logging.warning("initial value is zero, can't calculate growth rate,\n"
                        "returning nan for growth rate and doubling time")
        return np.nan

    growth_rate = np.log(y2 / y1) / days

    return growth_rate


def doubling_time(days, y1, y2, growth_rate):
    """
    Function to calculate the doubling time in days
    Args:
        days (int): number of days between points y1 and y2
        y1 (int): the initial value
        y2 (int): the new value
        growth_rate (float): the calculated growth rate between y1 and y2
    Returns:
        growth rate (float): the percentage growth between t1 and t2
        doubling time (float): the time in days to double the population from t1
    """

    if days < 1:
        logging.error("Can't have less than 1 day between intervals for daily growth rate")
        sys.exit()

    weekly_difference = y2 - y1

    if y1 == 0:
        logging.warning("initial value is zero, can't calculate growth rate,\n"
                        "returning nan for growth rate and doubling time")
        return weekly_difference, np.nan

    if growth_rate == 0:
        logging.warning("Growth rate is zero, returning nan for doubling time")
        return weekly_difference, np.nan

    method = DoublingMethod(growth_rate)
    dbl_time = method.padeapproximant()

    return weekly_difference, dbl_time


def dt_gr_wrapper(case_dates, days_number, cum_values, daily_values, region):
    """
    function to call helper functions for calculating growth/decay rate, doubling time and weekly difference
    Args:
        case_dates (list): list of dates for the relevant observations
        days_number (list): list of int as number of days since start date
        cum_values (list): list of the cumsum values
        daily_values (list): list of the daily counts
        region (str): the geographical region
        kind (str): the data type (deaths counts or diagnosis counts)

    Returns:
        dt_df (obj): pandas dataframe containing date, cumsum, daily count, growth/decay rate,
        doubling time, and weekly difference
    """
    if region in SUBD_REGIONS:
        category = SUBDISTRICT
    else:
        category = DISTRICT

    doubling_times_vals = []
    growth_rate_vals = []
    weekly_diff_vals = []
    for i in range(0, len(days_number) - WINDOW):
        time_span, start_value, new_value = get_values(index=i, x_pts=days_number, y_pts=cum_values)
        growth_decay_rate = growth_decay_rate_calculator(days=time_span, y1=start_value, y2=new_value)
        weekly_diff, dbl_time = doubling_time(
            days=time_span, y1=start_value, y2=new_value, growth_rate=growth_decay_rate
        )
        doubling_times_vals.append(dbl_time)
        growth_rate_vals.append(growth_decay_rate)
        weekly_diff_vals.append(weekly_diff)

    dt_df = pd.DataFrame()
    dt_df[DATE] = case_dates
    dt_df[category] = region
    dt_df[CUM_VALS] = cum_values
    dt_df[DAILY_VALS] = daily_values

    # offset the values with nan's such that the value represents a retrospective calculation of the past {WINDOW} days
    dt_df[GROWTH_RATE] = [np.nan] * WINDOW + growth_rate_vals
    dt_df[DOUBLE_TIME] = [np.nan] * WINDOW + doubling_times_vals
    dt_df[WEEKLY_DIFF] = [np.nan] * WINDOW + weekly_diff_vals
    dt_df[DATE] = dt_df[DATE].astype(str)

    return dt_df


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

    for dict_collection in [
        {"file": CASES_ADJUSTED_METRO, "kind": CASES, "outfile": CASES_DT_OUTFILE_PREFIX,
         "regions": METRO_REGIONS},
        {"file": CASES_ADJUSTED_METRO_SUBD, "kind": CASES, "outfile": f"{CASES_DT_OUTFILE_PREFIX}_subdistricts",
         "regions": SUBD_REGIONS},
        {"file": DEATHS_ADJUSTED_METRO, "kind": DEATHS, "outfile": DEATHS_DT_OUTFILE_PREFIX,
         "regions": METRO_REGIONS},
        {"file": DEATHS_ADJUSTED_METRO_SUBD, "kind": DEATHS, "outfile": f"{DEATHS_DT_OUTFILE_PREFIX}_subdistricts",
         "regions": SUBD_REGIONS}
    ]:
        file = dict_collection["file"]
        kind = dict_collection["kind"]
        outfile = dict_collection["outfile"]
        regions = dict_collection["regions"]

        logging.info(f"Fetch[ing] aggregated case/deaths data")
        df = minio_csv_to_df(
            minio_filename_override=f"{RESTRICTED_PREFIX}{file}",
            minio_bucket=COVID_BUCKET,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
        )
        logging.info(f"Fetch[ed] aggregated case/deaths data")

        master_df = pd.DataFrame()
        # filter to a reasonable start date
        logging.info(f"filter[ing] to date range")
        df_filt = df.query(f"@START_DATE < {DATE}").copy()
        df_filt[DATE] = pd.to_datetime(df_filt[DATE], format='%Y-%m-%d')
        logging.info(f"filter[ed] to date range")

        # get cumulative counts
        logging.info(f"Calculat[ing] cumsums")
        df_filt_cumulative = df_filt.set_index(DATE).cumsum().reset_index()
        logging.info(f"Calculat[ed] cumsums")

        # get the day number since start period
        df_filt_cumulative[DATE] = pd.to_datetime(df_filt_cumulative[DATE], format='%Y-%m-%d')
        df_filt_cumulative[DAYS] = (df_filt_cumulative[DATE] - pd.to_datetime(START_DATE, format='%Y-%m-%d')).dt.days

        regions_in_df = df_filt.columns.to_list()

        for region in regions:
            if region not in regions_in_df:
                logging.error(f"Region: {region} is not in the dataframe")
                sys.exit(-1)
            # get the week on week Percent change
            logging.info(f"Calculat[ing] percent change {region}")
            perc_change_df = percent_change_calc(df_filt, region)
            logging.info(f"Calculat[ed] percent change {region}")

            if kind == CASES:
                # get the week on week active case change
                logging.info(f"Calculat[ing] active case change {region}")
                active_change_df = active_case_change(df_filt, region)
                logging.info(f"Calculat[ed] active case change {region}")

            # get doubling times
            logging.info(f"Calculat[ing] doubling time for {region}")
            days_number = df_filt_cumulative[DAYS].to_numpy()
            cum_values = df_filt_cumulative[region].to_numpy()
            case_dates = df_filt_cumulative[DATE].to_numpy()
            daily_values = df_filt[region].to_numpy()
            double_time_df = dt_gr_wrapper(case_dates, days_number, cum_values, daily_values, region)
            logging.info(f"Calculat[ed] doubling time for {region}")

            # merge in the percent change
            logging.info(f"Merg[ing] doubling time and percent change for {region}")
            double_time_df = pd.merge(double_time_df, perc_change_df, on=DATE, how="left", validate="1:1")
            logging.info(f"Merg[ed] doubling time and percent change for {region}")

            # merge in the active case change
            if kind == CASES:
                logging.info(f"Merg[ing] doubling time and active case change for {region}")
                double_time_df = pd.merge(double_time_df, active_change_df, on=DATE, how="left", validate="1:1")
                logging.info(f"Merg[ed] doubling time and active case change for {region}")

            # add to master df
            logging.info(f"Add[ing] doubling time and percent change for {region} to master df")
            master_df = pd.concat([master_df, double_time_df])
            logging.info(f"Add[ed] doubling time and percent change for {region} to master df")

        # write to minio
        logging.info(f"Push[ing] {kind} data to mino ")
        result = write_to_minio(
            COVID_BUCKET, RESTRICTED_PREFIX,
            secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
            master_df, outfile
        )
        logging.info(f"Push[ed] {kind} data to mino ")

    logging.info(f"Done")
