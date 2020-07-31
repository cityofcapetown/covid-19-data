import json
import logging
import os
import sys
import tempfile

from db_utils import minio_utils
import geopandas
import pandas
import shapely.wkt

import cod_to_minio

GEOCODING_LOOKUP = "community-organisation-database.geocode-lookup"

cod_flattened_columns = ["ORG_NAME", 'CATEGORY_DESCRIPTION', 'SECTOR_DESCRIPTION1', 'ORGWARDS', 'SUBCOUNCIL_DESCRIPTION',
                         'IG_WARD', 'ORG_STREETNO', 'ORG_STREETNAME', 'ORG_SUBURBNAME', 'ORG_POSTAL_POST_CODE']

REPORT_EMAIL_ADDRESS = 'ORG_EMAIL_ADDRESS'
ORG_EMAIL_ADDRESS = 'ORG_EMAIL_ADDRESS'
REPORT_PHONE_COLUMN = "TEL_NUMBER"
ORG_PHONE_COLUMN = "ORG_PHONE_NUMBER"
SECTOR_COLUMN = "SECTOR_DESCRIPTION1"

COD_ADDRESS_COLUMN = 'ORG_ADDRESS'
LOCATION_COLUMN = "LOCATION"

COMMUNITY_ORGANISATION_BUCKET_NAME = "community-organisation-database.community-organisations"
COMMUNITY_ORGANISATION_DATASET_PREFIX = "community-organisations"


def get_report_export(minio_access, minio_secret):
    data_df = minio_utils.minio_to_dataframe(
        cod_to_minio.COMMUNITY_ORGANISATION_DATABASE_REPORT_EXPORT_BUCKET,
        minio_key=minio_access, minio_secret=minio_secret,
        data_classification=minio_utils.DataClassification.EDGE
    )

    return data_df


def get_geocode_lookup(minio_access, minio_secret):
    lookup_df = minio_utils.minio_to_dataframe(
        GEOCODING_LOOKUP,
        minio_key=minio_access, minio_secret=minio_secret,
        data_classification=minio_utils.DataClassification.EDGE
    )

    return lookup_df


def flatten_export_report(report_df):
    logging.debug(f"report_df.shape={report_df.shape}")
    org_df = report_df.groupby(cod_flattened_columns).apply(
        lambda groupby_df: pandas.Series({
            ORG_PHONE_COLUMN: (
                groupby_df[REPORT_PHONE_COLUMN].mode()[0] if groupby_df[REPORT_PHONE_COLUMN].count() else None
            ),
            ORG_EMAIL_ADDRESS: (
                groupby_df[REPORT_EMAIL_ADDRESS].mode()[0] if groupby_df[REPORT_EMAIL_ADDRESS].count() else None
            )
        })
    ).reset_index()
    logging.debug(f"org_df.shape={org_df.shape}")

    return org_df


def location_lookup(org_df, geocode_lookup_df):
    org_df[COD_ADDRESS_COLUMN] = (
            org_df['ORG_STREETNO'] + " " + org_df['ORG_STREETNAME'] +
            ", " + org_df['ORG_SUBURBNAME'] +
            ", " + org_df['ORG_POSTAL_POST_CODE']
    )
    org_df.drop(columns=['ORG_STREETNO', 'ORG_STREETNAME', 'ORG_SUBURBNAME', 'ORG_POSTAL_POST_CODE'], inplace=True)

    merged_df = org_df.merge(
        geocode_lookup_df,
        left_on=COD_ADDRESS_COLUMN, right_on=COD_ADDRESS_COLUMN,
        how="left", validate="many_to_one"
    )
    logging.debug(f"Geocoded: {merged_df[LOCATION_COLUMN].count()}/{merged_df[LOCATION_COLUMN].shape[0]}")

    return merged_df


def generate_community_organisation_datasets(org_df):
    org_df[LOCATION_COLUMN] = org_df[LOCATION_COLUMN].apply(
        lambda loc: shapely.wkt.loads(loc) if pandas.notna(loc) else None
    )
    org_gdf = geopandas.GeoDataFrame(org_df, geometry=LOCATION_COLUMN)

    # First, produce a dataset with everything in it
    yield "all", org_df, org_gdf

    # Then, sector by sector
    sectors = org_df[SECTOR_COLUMN].unique()
    logging.debug(f"Found the following sectors: {', '.join(sectors)}")
    for sector in sectors:
        yield sector, org_df.query(f"{SECTOR_COLUMN} == @sector"), org_gdf.query(f"{SECTOR_COLUMN} == @sector")


def write_org_data_to_minio(org_sector_name, org_df, org_gdf, minio_access, minio_secrety):
    sector_name_slug = org_sector_name.lower().strip().replace(" ", "-")
    dataset_name = "-".join([COMMUNITY_ORGANISATION_DATASET_PREFIX, sector_name_slug])
    logging.debug(f"Writing to '{org_df.shape[0]}' values to {dataset_name}")

    # First, the CSV
    result = minio_utils.dataframe_to_minio(
        org_df,
        minio_bucket=COMMUNITY_ORGANISATION_BUCKET_NAME,
        filename_prefix_override=dataset_name,
        minio_key=minio_access,
        minio_secret=minio_secrety,
        data_classification=minio_utils.DataClassification.EDGE,
        file_format="csv",
        data_versioning=False
    )
    assert result, "Writing CSV to minio failed"

    # Then, the GeoJSON
    with tempfile.TemporaryDirectory() as temp_dir:
        geojson_filename = dataset_name + ".geojson"
        local_path = os.path.join(temp_dir, geojson_filename)
        org_gdf.to_file(local_path, driver="GeoJSON")

        result = minio_utils.file_to_minio(
            local_path,
            minio_bucket=COMMUNITY_ORGANISATION_BUCKET_NAME,
            minio_key=minio_access,
            minio_secret=minio_secrety,
            data_classification=minio_utils.DataClassification.EDGE,
        )
        assert result, "Writing GeoJSON to minio failed"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_FILE"

    if SECRETS_PATH_VAR not in os.environ:
        print("Secrets path not found!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_FILE"]
    secrets = json.load(open(secrets_path))

    logging.info("Get[ing] data")
    report_data_df = get_report_export(secrets["minio"]["edge"]["access"],
                                       secrets["minio"]["edge"]["secret"])
    location_lookup_df = get_geocode_lookup(secrets["minio"]["edge"]["access"],
                                            secrets["minio"]["edge"]["secret"])
    logging.info("G[ot] data")

    logging.info("Flatten[ing] report data")
    flattened_report_df = flatten_export_report(report_data_df)
    logging.info("Flatten[ed] report data")

    logging.info("Merg[ing] in location data")
    community_org_df = location_lookup(flattened_report_df, location_lookup_df)
    logging.info("Merg[ed] in location data")

    logging.info("Generat[ing] Community Organisation Datasets")
    for sector_name, sector_org_df, sector_org_gdf in generate_community_organisation_datasets(community_org_df):
        logging.info(f"Generat[ed] '{sector_name.strip()}' Community Organisation Datasets")
        write_org_data_to_minio(sector_name, sector_org_df, sector_org_gdf,
                                secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])

    logging.info("...Done!")
