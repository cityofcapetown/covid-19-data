import json
import logging
import os
import sys
import tempfile

import pandas

from db_utils import minio_utils

from sd_media_data_to_minio import BUCKET, MINIO_CLASSIFICATION, FULL_FILENAME_PATH

PUBLISHED_COL = "published"
ID_COL = "id"
SENTIMENT_COL = "sentiment"
ENGAGEMENT_COL = "engagement"

SD_CONTENT_TERMS = (
    # General names
    {
        "features": ["city"],
        "regexes": ["basic\W*service", "service\W*delivery"],
        "tags": ["80989", "80942", "80911", "80944", ]
    },
    # Water and Sanitation
    {
        "features": ["city", "city-water_and_waste", "city-water_and_waste-water_and_sanitation"],
        "regexes": ["water", "sew(e|a)r", "drain",
                    "water\W*(meter|metre)", "water\W*management\W*device"],
        "tags": ["80952", "80953", "80959", "80960", "80956", "80958"]
    },
    # Solid Waste Management
    {
        "features": ["city", "city-water_and_waste", "city-water_and_waste-solid_waste_management"],
        "regexes": ["solid\W*waste", "rubbish", "garbage", "dirt", "refuse", "bin"],
        "tags": ["80957", "80954"]
    },
    # Electricity
    {
        "features": ["city", "city-energy_and_climate_change", "city-energy_and_climate_change-electricity"],
        "regexes": ["electricity", "energy", "power",
                    "street\W*light",
                    "(energy|power|electricity)\W*(meter|metre)", ],
        "tags": ['80963', ]
    },
    # RIMM
    {
        "features": ["city", "city-transport", "city-transport-roads_infrastructure_and_management"],
        "regexes": ["roads", "pot(\W|)hole"],
        "tags": ["80967", "80983", "80969"]
    },
    # Public Housing
    {
        "features": ["city", "city-human_settlements", "city-human_settlements-public_housing"],
        "regexes": ["(council|public)\W*(hous|flat)", ],
        "tags": ["80962", ]
    },
    # Recreation and Parks
    {
        "features": ["city", "city-community_services_and_health",
                     "city-community_services_and_health-recreation_and_parks"],
        "regexes": ["(council|public)\W*(park|ground|facility|beach)", ],
        "tags": ["80966", ]
    },
)

# Not currently used
# Additional, visibility field should be NA
# SD_MOST_ENGAGE_POST_EXCLUDE_AUTHORS = {
#     '6011868110',  # DA (Facebook)
#     '47797003787',  # Netball South Africa
#     '1528952840718244',  # President of Nigeria
#     '122121361542',  # SABC News
#     '113743842023920',  # Daily Voice
#     '10227041841',  # News24
#     '136956534616',  # TimesLive
#     '168892509821961',  # EWN
#     '160836574053016',  # ENCA
#     '21993963624',  # IOL
#     '123971017684407',  # HeraldLive
#     '160223335806',  # SPCA
#     '133414100095220',  # MotswedingFM
#     '190104684357654',  # ANC (Facebook)
#     '311936835992045',  # BizNews
# }

MEAN_SENTIMENT_COL = "mean_sentiment"
ENGAGEMENT_SUM_COL = "engagement_sum"
MENTIONS_COL = "mentions_count"

ISO8601_DATE_FORMAT = "%Y-%m-%d"

DATE_COL = "date"
FEATURE_COL = "feature"
VALUE_COL = "value"
MEASURE_COL = "measure"

SD_TIDY_DATE_FILE = "data/private/sd_tidy_media_data"


def get_data(minio_key, minio_access, minio_secret):
    with tempfile.NamedTemporaryFile() as temp_datafile:
        minio_utils.minio_to_file(
            filename=temp_datafile.name,
            minio_filename_override=minio_key + ".csv",
            minio_bucket="covid",
            minio_key=minio_access,
            minio_secret=minio_secret,
            data_classification=minio_utils.DataClassification.EDGE
        )
        data_df = pandas.read_csv(temp_datafile.name)

    data_df[PUBLISHED_COL] = pandas.to_datetime(data_df[PUBLISHED_COL], errors="coerce")
    logging.debug(f"data_df.shape={data_df.shape}")

    return data_df


def filter_mentions(data_df, regexes, tag_ids):
    regex_pattern = f"({')|('.join(regexes)})".lower()
    logging.debug(f"regex_pattern={regex_pattern}")
    tag_ids_pattern = f"({')|('.join(tag_ids)})".lower()
    logging.debug(f"regex_pattern={tag_ids_pattern}")

    logging.debug(f"data_df.shape={data_df.shape}")
    df = data_df.loc[
        (data_df["extract"].str.lower().str.contains(regex_pattern).fillna(False)
         | (data_df["tagIds"].str.contains(tag_ids_pattern).fillna(False)))
    ]
    logging.debug(f"df.shape={df.shape}")

    return df


def resample_feature_mentions_df(feature_value, data_df):
    resampler = data_df.resample(on=PUBLISHED_COL, rule="1D")

    resampled_df = resampler[SENTIMENT_COL].mean().rename(MEAN_SENTIMENT_COL).to_frame()
    resampled_df[ENGAGEMENT_SUM_COL] = resampler[ENGAGEMENT_COL].sum()
    resampled_df[MENTIONS_COL] = resampler[SENTIMENT_COL].count()
    resampled_df[DATE_COL] = resampled_df.index.to_series().dt.strftime(ISO8601_DATE_FORMAT)
    resampled_df[FEATURE_COL] = feature_value

    logging.debug(f"( pre-0 drop) resampled_df.shape={resampled_df.shape}")
    resampled_df = resampled_df.query(f"{MENTIONS_COL} > 0")
    logging.debug(f"(post-0 drop) resampled_df.shape={resampled_df.shape}")

    melt_df = resampled_df.melt(
        id_vars=[DATE_COL, FEATURE_COL],
        value_vars=[ENGAGEMENT_SUM_COL, MENTIONS_COL, MEAN_SENTIMENT_COL],
        var_name=MEASURE_COL, value_name=VALUE_COL
    ).reset_index()

    logging.debug(f"resampled_df.shape={resampled_df.shape}")

    return melt_df


SD_MEDIA_TIDY_PATH = "data/private/sd_media_tidy"


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_PATH"

    if SECRETS_PATH_VAR not in os.environ:
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_PATH"]
    secrets = json.load(open(secrets_path))

    logging.info("G[etting] data...")
    mentions_df = get_data(FULL_FILENAME_PATH,
                           secrets["minio"]["edge"]["access"],
                           secrets["minio"]["edge"]["secret"])
    logging.info("G[ot] data...")

    logging.info("Select[ing] relevant mentions per feature")
    feature_df_dicts = {}
    for content_dict in SD_CONTENT_TERMS:
        features = content_dict["features"]
        filtered_df = filter_mentions(mentions_df, content_dict["regexes"], content_dict["tags"])

        for feature in features:
            logging.debug(f"Appending to '{feature}' df")
            if feature in feature_df_dicts:
                logging.debug(f"( pre-add) feature_df_dicts['{feature}'].shape={feature_df_dicts[feature].shape}")
            else:
                logging.debug(f"( pre-add) feature_df_dicts['{feature}'].shape=(0,0)")
            feature_df_dicts[feature] = pandas.concat([
                feature_df_dicts.get(feature, pandas.DataFrame()), filtered_df
            ]).drop_duplicates(subset=ID_COL)
            logging.debug(f"(post-add) feature_df_dicts['{feature}'].shape={feature_df_dicts[feature].shape}")

    logging.info("Select[ed] relevant mentions per feature")

    logging.info("Resampl[ing] per feature")
    tidy_df = pandas.concat([
        resample_feature_mentions_df(feature, feature_df)
        for feature, feature_df in feature_df_dicts.items()
    ])
    logging.debug(f"tidy_df.shape={tidy_df.shape}")
    logging.info("Resampl[ed] per feature")

    logging.info("Writing DataFrame to Minio...")
    minio_utils.dataframe_to_minio(tidy_df, BUCKET,
                                   secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"],
                                   MINIO_CLASSIFICATION,
                                   filename_prefix_override=SD_TIDY_DATE_FILE,
                                   data_versioning=False,
                                   file_format="csv")
    logging.info("...Done!")
