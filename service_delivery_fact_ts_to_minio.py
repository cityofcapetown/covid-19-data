import json
import os
import logging
import sys

from db_utils import minio_utils
import numpy
import pandas

SERVICE_STANDARDS_TOOL_PIPELINE_PREFIX = "service-standards-tool"
SERVICE_DELIVERY_CITY_FACTS_MINIO_NAME = f'{SERVICE_STANDARDS_TOOL_PIPELINE_PREFIX}.sd-city-facts'
SERVICE_DELIVERY_DIRECTORATE_FACTS_MINIO_NAME = f'{SERVICE_STANDARDS_TOOL_PIPELINE_PREFIX}.sd-directorate-facts'
SERVICE_DELIVERY_DEPARTMENT_FACTS_MINIO_NAME = f'{SERVICE_STANDARDS_TOOL_PIPELINE_PREFIX}.sd-department-facts'
FACTS_DATASETS = (
    SERVICE_DELIVERY_CITY_FACTS_MINIO_NAME,
    SERVICE_DELIVERY_DIRECTORATE_FACTS_MINIO_NAME,
    SERVICE_DELIVERY_DEPARTMENT_FACTS_MINIO_NAME
)
FACT_CLASSIFICATION = minio_utils.DataClassification.LAKE

DATE_COL = "date"
MEASURE_COL = "measure"
FEATURE_COL = "feature"
HEX_RESOLUTION_COL = "resolution"
HEX_INDEX_COL = "index"
VALUE_COL = "value"
FEATURE_TYPE_COL = "feature_type"

SERVICE_STANDARD_WEIGHTING_MEASURE = "service_standard_weighting"
LONG_BACKLOG_WEIGHTING_MEASURE = "long_backlog_weighting"
LINEAR_MEASURES = [
    "opened_count", "closed_count", "opened_within_target_sum", "closed_within_target_sum", "opened_still_open_sum",
    "backlog", SERVICE_STANDARD_WEIGHTING_MEASURE, LONG_BACKLOG_WEIGHTING_MEASURE
]

SERVICE_STANDARD_MEASURE = "service_standard"
LONG_BACKLOG_MEASURE = "long_backlog"
NON_LINEAR_MEASURES = [
    SERVICE_STANDARD_MEASURE, SERVICE_STANDARD_WEIGHTING_MEASURE,
    LONG_BACKLOG_MEASURE, LONG_BACKLOG_WEIGHTING_MEASURE
]

HEX_RESOLUTION = 3

ISO8601_DATE_FORMAT = "%Y-%m-%d"

COVID_BUCKET = "covid"
BUCKET_CLASSIFICATION = minio_utils.DataClassification.EDGE
SERVICE_DELIVERY_PREFIX = "data/private/business_continuity_service_delivery"


def get_fact_dataset(fact_bucket, minio_access, minio_secret):
    df = minio_utils.minio_to_dataframe(
        minio_bucket=fact_bucket,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=FACT_CLASSIFICATION,
        use_cache=True
    )

    logging.debug(f"{fact_bucket}.shape={df.shape}")
    logging.debug(f"{fact_bucket}.columns={df.columns}")

    return df[[DATE_COL, HEX_INDEX_COL, HEX_RESOLUTION_COL, FEATURE_TYPE_COL, FEATURE_COL, MEASURE_COL, VALUE_COL]]


def filter_by_resolution(fact_df, resolution=HEX_RESOLUTION):
    return fact_df.query(f"{HEX_RESOLUTION_COL} == {resolution}")


def _compute_weighted_average(day_df, value_measure, weighting_measure):
    clean_df = day_df.dropna(how='any')
    weights = clean_df[weighting_measure].values

    if weights.sum() > 0:
        return pandas.Series(numpy.average(
            clean_df[value_measure].values,
            weights=weights
        ))
    else:
        return pandas.Series([None])


def _compute_non_linear_measure(group_df, value_measure, weighting_measure):
    group_df[DATE_COL] = pandas.to_datetime(group_df[DATE_COL], format=ISO8601_DATE_FORMAT)

    pivot_df = group_df.pivot(
        index=[DATE_COL, HEX_RESOLUTION_COL, HEX_INDEX_COL], columns=MEASURE_COL, values=VALUE_COL
    ).fillna(0)
    if value_measure not in pivot_df.columns or weighting_measure not in pivot_df.columns:
        logging.warning(f"Skipping computing value of '{value_measure}', using '{weighting_measure}' as weights")
        return pandas.DataFrame()

    weighted_vals_df = pivot_df.groupby(
        pandas.Grouper(level=DATE_COL, freq="1D")
    ).apply(_compute_weighted_average,
            value_measure=value_measure,
            weighting_measure=weighting_measure)
    weighted_vals_df = weighted_vals_df.dropna().resample("1D").ffill()
    weighted_vals_df.columns = [VALUE_COL]

    return weighted_vals_df


def despatialise(fact_df):
    # Linear features are easy - just have to sum them
    linear_filtered_df = fact_df.query(f"{MEASURE_COL}.isin(@LINEAR_MEASURES)")
    logging.debug(f"linear_filtered_df.shape={linear_filtered_df.shape}")
    logging.debug(f"linear_filtered_df.columns={linear_filtered_df.columns}")

    linear_groupby_df = linear_filtered_df.groupby(
        [col for col in linear_filtered_df.columns
         if col not in (HEX_RESOLUTION_COL, HEX_INDEX_COL, VALUE_COL)]
    )[VALUE_COL].sum().reset_index()
    logging.debug(f"linear_groupby_df.shape={linear_groupby_df.shape}")
    logging.debug(f"linear_groupby_df.columns={linear_groupby_df.columns}")
    logging.debug(f"linear_groupby_df.head(10)=\n{linear_groupby_df.head(10)}")

    # service standard and long backlogs are a little trickier - need to do a weighted mean
    non_linear_filtered_df = fact_df.query(f"{MEASURE_COL}.isin(@NON_LINEAR_MEASURES)")
    logging.debug(f"non_linear_filtered_df.shape={non_linear_filtered_df.shape}")
    logging.debug(f"non_linear_filtered_df.columns={non_linear_filtered_df.columns}")
    logging.debug(f"non_linear_filtered_df['{MEASURE_COL}'].value_counts()=\n"
                  f"{non_linear_filtered_df[MEASURE_COL].value_counts()}")

    non_linear_groupby = non_linear_filtered_df.groupby(
        [col for col in non_linear_filtered_df.columns
         if col not in (HEX_RESOLUTION_COL, DATE_COL, HEX_INDEX_COL, MEASURE_COL, VALUE_COL)]
    )

    non_linear_groupby_df = pandas.concat((
        non_linear_groupby.apply(_compute_non_linear_measure,
                                 value_measure=value_col,
                                 weighting_measure=weighting_col).reset_index().assign(
            **{
                DATE_COL: lambda df: df[DATE_COL].dt.strftime(ISO8601_DATE_FORMAT),
                MEASURE_COL: value_col
            })
        for value_col, weighting_col in ((SERVICE_STANDARD_MEASURE, SERVICE_STANDARD_WEIGHTING_MEASURE),
                                         (LONG_BACKLOG_MEASURE, LONG_BACKLOG_WEIGHTING_MEASURE))
    ))
    logging.debug(f"non_linear_groupby_df.shape={non_linear_groupby_df.shape}")
    logging.debug(f"non_linear_groupby_df.columns={non_linear_groupby_df.columns}")
    logging.debug(f"non_linear_groupby_df.head(10)=\n{non_linear_groupby_df.head(10)}")

    combined_df = pandas.concat([
        linear_groupby_df, non_linear_groupby_df
    ])
    logging.debug(f"combined_df.shape={combined_df.shape}")
    logging.debug(f"combined_df.columns={combined_df.columns}")

    return combined_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error(f"'{SECRETS_PATH_VAR}' env var missing!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("G[etting] Data")
    source_fact_df = pandas.concat((
        get_fact_dataset(fact_datatset, secrets["minio"]["lake"]["access"], secrets["minio"]["lake"]["secret"])
        for fact_datatset in FACTS_DATASETS
    ))
    logging.debug(f"source_fact_df.shape={source_fact_df.shape}")
    logging.debug(f"source_fact_df.columns={source_fact_df.columns}")
    logging.info("G[ot] Data")

    logging.info("Filter[ing] by date")
    filtered_fact_df = filter_by_resolution(source_fact_df)
    logging.debug(f"filtered_fact_df.shape={filtered_fact_df.shape}")
    logging.debug(f"filtered_fact_df.columns={filtered_fact_df.columns}")
    logging.info("Filter[ed] by date")

    logging.info("Comput[ing] pure time series")
    ts_df = despatialise(filtered_fact_df)
    logging.info("Comput[ed] pure time series")

    logging.info("Wr[iting] to Minio")
    minio_utils.dataframe_to_minio(ts_df, COVID_BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   BUCKET_CLASSIFICATION,
                                   filename_prefix_override=SERVICE_DELIVERY_PREFIX,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("Wr[ote] to Minio")
