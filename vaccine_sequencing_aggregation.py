# base imports
import json
import logging
import os
import sys
# external imports
from db_utils import minio_utils
import pandas as pd
# local imports
from vaccine_data_to_minio import COVID_BUCKET, EDGE_CLASSIFICATION, STAFF_MERGE_STR, VAX_MERGE_STR
from vaccine_sequencing_munge import (OUTPUT_PREFIX_ANN, PRIVATE_PREFIX, minio_to_df, PARQUET_READER, VACCINE_SEQ_ANN,
                                      UNIQUE_STAFF_LIST_ANN, Q1, Q2, RISK_SCORE, STAFF_TYPE, BRANCH)

# input settings
OUTPUT_PREFIX_AGG = f"{PRIVATE_PREFIX}staff_vaccine/aggregations/"
UNIQUE_STAFF_LIST_ANN = f"{OUTPUT_PREFIX_ANN}{UNIQUE_STAFF_LIST_ANN}.parquet"
VACCINE_SEQ_ANN = f"{OUTPUT_PREFIX_ANN}{VACCINE_SEQ_ANN}.parquet"

# outfile
SEQ_WILLINGNESS_TOTAL = "staff-sequencing-willingness-totals"
SEQ_WILLINGNESS_BRANCH = "staff-sequencing-willingness-by-branch"
SEQ_WILLINGNESS_TYPE = "staff-sequencing-willingness-by-type"
SEQ_WILLINGNESS_RISK = "staff-sequencing-willingness-by-risk-score"

# vaccine sequencing list cols
WILLING_GEN = "willing_to_vaccinate"
WILLING_PILOT = "willing_for_sisonke"
PERC_TOTAL_STAFF = "percent_total_sequenced_staff"
PERC_STAFF_SEQ = "percent_staff_sequenced"
PERC_WILLING_GEN = "percent_willing_to_vaccinate"
PERC_WILLING_PILOT = "percent_willing_for_sisonke"
TOTAL_STAFF = "total_staff"
TOTAL = "total"
CITY = "CT Metro"
STAFF_SEQD = "staff_sequenced"


def base_aggregation(df, group_col):
    agg_df = df.groupby(group_col).agg(
        **{TOTAL_STAFF: (STAFF_MERGE_STR, "count")},
    ).reset_index()

    return agg_df


def willingness_aggregation(df, group_col):
    agg_df = df.groupby(group_col).agg(
        **{WILLING_GEN: (Q1, "sum")},
        **{WILLING_PILOT: (Q2, "sum")},
        **{STAFF_SEQD: (VAX_MERGE_STR, "count")}
    ).reset_index()

    return agg_df


def willingness_agg_wrapper(df_vacc, df_seq, group_col, willing_only=False):
    # ----------------------------------
    # calculate staff willingness by branch
    logging.info("Aggregat[ing] vaccine sentiment by Branch")
    if willing_only:
        totals_willing_df = willingness_aggregation(df_seq, group_col)
        total_sequenced = totals_willing_df[STAFF_SEQD].sum()
        totals_willing_df[PERC_TOTAL_STAFF] = totals_willing_df[STAFF_SEQD] / total_sequenced
        totals_willing_df[PERC_WILLING_GEN] = totals_willing_df[WILLING_GEN] / totals_willing_df[
            STAFF_SEQD]
        totals_willing_df[PERC_WILLING_PILOT] = totals_willing_df[WILLING_PILOT] / totals_willing_df[
            STAFF_SEQD]

        totals_willing_df = totals_willing_df[
            [group_col, STAFF_SEQD, PERC_TOTAL_STAFF, WILLING_GEN, PERC_WILLING_GEN, WILLING_PILOT, PERC_WILLING_PILOT]
        ].copy()

    else:
        df_totals = base_aggregation(df_vacc, group_col)
        df_willingness_totals = willingness_aggregation(df_seq, group_col)

        # merge staff totals with willingness totals
        totals_willing_df = pd.merge(df_totals, df_willingness_totals, on=group_col, how="left", validate="1:1")

        # clean up the columns
        totals_willing_df[PERC_STAFF_SEQ] = totals_willing_df[STAFF_SEQD] / totals_willing_df[TOTAL_STAFF]
        totals_willing_df[PERC_WILLING_GEN] = totals_willing_df[WILLING_GEN] / totals_willing_df[STAFF_SEQD]
        totals_willing_df[PERC_WILLING_PILOT] = totals_willing_df[WILLING_PILOT] / totals_willing_df[STAFF_SEQD]
        totals_willing_df = totals_willing_df[
            [group_col, TOTAL_STAFF, STAFF_SEQD, PERC_STAFF_SEQ, WILLING_GEN, PERC_WILLING_GEN, WILLING_PILOT,
             PERC_WILLING_PILOT]
        ].copy()

    return totals_willing_df


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
    logging.info(f"Fetch[ing] the data")
    staff_list_df_fixed_filt_unique, annotated_sequencing_df = [
        minio_to_df(
            minio_filename_override=filename,
            minio_bucket=COVID_BUCKET,
            data_classification=EDGE_CLASSIFICATION,
            reader=PARQUET_READER
        ) for filename in [UNIQUE_STAFF_LIST_ANN, VACCINE_SEQ_ANN]
    ]
    logging.info(f"Fetch[ed] the data")

    # ----------------------------------
    # calculate staff willingness by branch
    logging.info("Aggregat[ing] vaccine sentiment by Branch")
    staff_seq_by_branch = willingness_agg_wrapper(staff_list_df_fixed_filt_unique, annotated_sequencing_df, BRANCH)
    logging.info("Aggregat[ed] vaccine sentiment by Branch")

    # ----------------------------------
    # calculate staff willingness by staff type
    logging.info("Aggregat[ing] vaccine sentiment by Staff type")
    staff_seq_by_type = willingness_agg_wrapper(staff_list_df_fixed_filt_unique, annotated_sequencing_df, STAFF_TYPE)
    logging.info("Aggregat[ed] vaccine sentiment by Staff type")

    # ----------------------------------
    # calculate staff willingness by risk score
    logging.info("Aggregat[ing] vaccine sentiment by risk score")
    willingness_risk_score = willingness_agg_wrapper(None, annotated_sequencing_df, RISK_SCORE, willing_only=True)
    logging.info("Aggregat[ed] vaccine sentiment by risk score")

    # ----------------------------------
    # calculate staff willingness totals off the branch df
    logging.info("Calculat[ing] staff totals")
    total_staff = staff_seq_by_branch[TOTAL_STAFF].sum()
    staff_sequenced = staff_seq_by_branch[STAFF_SEQD].sum()
    willing_vaccine = staff_seq_by_branch[WILLING_GEN].sum()
    willing_sisonke = staff_seq_by_branch[WILLING_PILOT].sum()
    percent_staff_sequenced = staff_sequenced / total_staff
    percent_willing_vaccine = willing_vaccine / staff_sequenced
    percent_willing_sisonke = willing_sisonke / staff_sequenced

    percent_willing_totals_df = pd.DataFrame(data={
        CITY: [TOTAL],
        TOTAL_STAFF: total_staff,
        STAFF_SEQD: [staff_sequenced],
        PERC_STAFF_SEQ: [percent_staff_sequenced],
        WILLING_GEN: [willing_vaccine],
        PERC_WILLING_GEN: [percent_willing_vaccine],
        WILLING_PILOT: [willing_sisonke],
        PERC_WILLING_PILOT: [percent_willing_sisonke],
    })
    logging.info("Calculat[ed] staff totals")

    # ----------------------------------
    logging.info(f"Push[ing] data to mino")
    for outfile, out_df, prefix in [
        (SEQ_WILLINGNESS_TOTAL, percent_willing_totals_df, OUTPUT_PREFIX_AGG),
        (SEQ_WILLINGNESS_BRANCH, staff_seq_by_branch, OUTPUT_PREFIX_AGG),
        (SEQ_WILLINGNESS_TYPE, staff_seq_by_type, OUTPUT_PREFIX_AGG),
        (SEQ_WILLINGNESS_RISK, willingness_risk_score, OUTPUT_PREFIX_AGG),
    ]:
        minio_utils.dataframe_to_minio(
            out_df,
            COVID_BUCKET,
            secrets["minio"]["edge"]["access"],
            secrets["minio"]["edge"]["secret"],
            EDGE_CLASSIFICATION,
            filename_prefix_override=f"{prefix}{outfile}",
            file_format="parquet",
            data_versioning=False

        )
    logging.info(f"Push[ing] data to mino")

    logging.info("...Done!")
