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
LINEAR_MEASURES = [
    "opened_count", "closed_count", "opened_within_target_sum", "closed_within_target_sum",
    "backlog", SERVICE_STANDARD_WEIGHTING_MEASURE
]

SERVICE_STANDARD_MEASURE = "service_standard"
SERVICE_STANDARD_MEASURES = [
    SERVICE_STANDARD_MEASURE, SERVICE_STANDARD_WEIGHTING_MEASURE
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


def _compute_weighted_average(day_df):
    clean_df = day_df.dropna(how='any')
    weights = clean_df[SERVICE_STANDARD_WEIGHTING_MEASURE].values

    if weights.sum() > 0:
        return pandas.Series(numpy.average(
            clean_df[SERVICE_STANDARD_MEASURE].values,
            weights=weights
        ))
    else:
        return pandas.Series([0])


def _compute_service_standard(group_df):
    group_df[DATE_COL] = pandas.to_datetime(group_df[DATE_COL], format=ISO8601_DATE_FORMAT)

    pivot_df = group_df.pivot(
        index=[DATE_COL, HEX_RESOLUTION_COL, HEX_INDEX_COL], columns=MEASURE_COL, values=VALUE_COL
    ).fillna(0)

    weighted_vals = pivot_df.groupby(pandas.Grouper(level=DATE_COL, freq="1D")).apply(_compute_weighted_average)
    weighted_vals = weighted_vals.resample("1D").ffill()
    weighted_vals.columns = [VALUE_COL]

    return weighted_vals


def despatialise(fact_df):
    # Linear features are easy - just have to sum them
    linear_filtered_df = fact_df.query(f"{HEX_RESOLUTION_COL} == {HEX_RESOLUTION} and "
                                       f"{MEASURE_COL}.isin(@LINEAR_MEASURES)")
    logging.debug(f"linear_filtered_df.shape={linear_filtered_df.shape}")
    logging.debug(f"linear_filtered_df.columns={linear_filtered_df.columns}")

    linear_groupby_df = linear_filtered_df.groupby(
        [col for col in linear_filtered_df.columns
         if col not in (HEX_RESOLUTION_COL, HEX_INDEX_COL, VALUE_COL)]
    )[VALUE_COL].sum().reset_index()
    logging.debug(f"linear_groupby_df.shape={linear_groupby_df.shape}")
    logging.debug(f"linear_groupby_df.columns={linear_groupby_df.columns}")
    logging.debug(f"linear_groupby_df.head(10)=\n{linear_groupby_df.head(10)}")

    # service standard is a little trickier - need to do a weighted mean
    ss_filtered_df = fact_df.query(f"{HEX_RESOLUTION_COL} == {HEX_RESOLUTION} and "
                                   f"{MEASURE_COL}.isin(@SERVICE_STANDARD_MEASURES)")
    logging.debug(f"ss_filtered_df.shape={ss_filtered_df.shape}")
    logging.debug(f"ss_filtered_df.columns={ss_filtered_df.columns}")
    ss_groupby_df = ss_filtered_df.groupby(
        [col for col in ss_filtered_df.columns
         if col not in (HEX_RESOLUTION_COL,  DATE_COL, HEX_INDEX_COL, MEASURE_COL, VALUE_COL)]
    ).apply(_compute_service_standard).reset_index()

    ss_groupby_df[DATE_COL] = ss_groupby_df[DATE_COL].dt.strftime(ISO8601_DATE_FORMAT)
    ss_groupby_df[MEASURE_COL] = SERVICE_STANDARD_MEASURE

    logging.debug(f"ss_groupby_df.shape={ss_groupby_df.shape}")
    logging.debug(f"ss_groupby_df.columns={ss_groupby_df.columns}")
    logging.debug(f"ss_groupby_df.head(10)=\n{ss_groupby_df.head(10)}")

    combined_df = pandas.concat([
        linear_groupby_df, ss_groupby_df
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

    source_fact_df = pandas.concat((
        get_fact_dataset(fact_datatset, secrets["minio"]["lake"]["access"], secrets["minio"]["lake"]["secret"])
        for fact_datatset in FACTS_DATASETS
    ))
    logging.debug(f"source_fact_df.shape={source_fact_df.shape}")
    logging.debug(f"source_fact_df.columns={source_fact_df.columns}")

    ts_df = despatialise(source_fact_df)

    minio_utils.dataframe_to_minio(ts_df, COVID_BUCKET,
                                   secrets["minio"]["edge"]["access"],
                                   secrets["minio"]["edge"]["secret"],
                                   BUCKET_CLASSIFICATION,
                                   filename_prefix_override=SERVICE_DELIVERY_PREFIX,
                                   data_versioning=False,
                                   file_format="csv")
