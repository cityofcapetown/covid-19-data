"""
script to calculate backlog % close and total requests opened metrics for city and department over a given period
and output a single geojson of the top n requests per hex, with associated backlog, service standard and total
requests opened metrics
"""

__author__ = "Colin Anthony"

# base imports
from datetime import timedelta
import json
import logging
import os
import pathlib
import sys
import tempfile
# external imports
from db_utils import minio_utils
import geopandas as gpd
import geojson
from h3 import h3
import pandas as pd
from shapely.geometry import Polygon
# local imports
import service_delivery_metrics_munge

# set bucket constants
SERVICE_FACTS_BUCKET = "service-standards-tool.sd-request-facts"
SERVICE_ATTRIBUTES = "service-standards-tool.sd-request-fact-attributes"
COVID_BUCKET = "covid"
PRIVATE_PREFIX = "data/private/"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
LAKE_CLASSIFICATION = minio_utils.DataClassification.LAKE

# outfiles
DEPT_SERVICE_METRICS_HEX_7 = "business_continuity_service_delivery_city_hex_7_metrics"
CITY_SERVICE_METRICS_JSON = "business_continuity_service_delivery_city_hex_top_n.geojson"

# the number of request codes per hex to capture
RESOLUTION = 7
SELECT_TOP_N = 5
OPEN_COUNT = "opened_count"
HEX_INDEX_COL = "index"
DATE_COL = "date"
RES_COL = "resolution"
GEO_COL = "geometry"
CODE = "Code"
FEATURE = "feature"

INDEX_COLS = [HEX_INDEX_COL, "directorate", "department", CODE]
METRIC_COLS = ["service_standard", "backlog", "total_opened"]

SECRETS_PATH_VAR = "SECRETS_PATH"


def get_hex_boundary(hex_index):
    """
    function to convert Uber hex string to polygon
    Args:
        hex_index: (str) Uber hex string

    Returns:
        (tuple) polygon coordinates (tuple of tuples)
    """
    boundary = h3.h3_to_geo_boundary(hex_index, geo_json=True)
    return boundary


def make_shapely_poly(x):
    """
    function to convery polygon list to Shapely Polygon object
    Args:
        x: (tuple) polygon coordinates (tuple of tuples)

    Returns:
        (object) shapely Polygon
    """
    return Polygon(x)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')
    # Loading secrets
    if SECRETS_PATH_VAR not in os.environ:
        logging.error("%s env var missing!", SECRETS_PATH_VAR)
        logging.info("Trying to load from file")
        secrets_file = pathlib.Path("/home/jovyan/secrets/service-turnaround-secrets.json").resolve()
        if not secrets_file.exists():
            print("could not find your secrets")
            sys.exit(-1)
        else:
            secrets = json.load(open(secrets_file))
            logging.info("Loaded secrets from file")
    else:
        logging.info("Setting secrets variables")
        secrets_path = os.environ[SECRETS_PATH_VAR]
        if not secrets_path:
            logging.error(f"Secrets file not found in ENV: {SECRETS_PATH_VAR}")
            sys.exit(-1)
        secrets = json.load(open(secrets_path))

    # get the data
    logging.info(f"Fetch[ing] {SERVICE_FACTS_BUCKET}")
    service_dfs = []
    for df_bucket in [SERVICE_FACTS_BUCKET, SERVICE_ATTRIBUTES]:
        fetched_df = minio_utils.minio_to_dataframe(
            minio_bucket=df_bucket,
            minio_key=secrets["minio"]["lake"]["access"],
            minio_secret=secrets["minio"]["lake"]["secret"],
            data_classification=LAKE_CLASSIFICATION,
        )
        service_dfs.append(fetched_df)
    logging.info(f"Fetch[ed] {SERVICE_FACTS_BUCKET}")

    service_facts = service_dfs[0]
    service_attribs = service_dfs[1]

    logging.info("Filter[ing] to hex 7 resolution")
    service_facts_hex_7 = service_facts.query(f"{RES_COL} == @RESOLUTION").copy().assign(
        **{DATE_COL: lambda df: pd.to_datetime(df[DATE_COL], format="%Y-%m-%d")}
    )
    logging.info("Filter[ed] to hex 7 resolution")

    # filter out those with no location data
    logging.info(f"Filter[ing] out hex index == 0")
    service_facts_hex_7_locs = service_facts_hex_7.query(f"{HEX_INDEX_COL} != '0'").copy()
    logging.info(f"Filter[ed] out hex index == 0")

    logging.info("Merg[ing] to annotations")
    res7_facts_annotated = pd.merge(service_facts_hex_7_locs, service_attribs, how="left", on=FEATURE, validate="m:1")
    logging.info("Merg[ed] to annotations")

    logging.info("Generat[ing] request code annotation name")
    res7_facts_annotated.loc[:, CODE] = service_delivery_metrics_munge.generate_request_code_name(res7_facts_annotated)
    logging.info("Generat[ed] request code annotation name")

    logging.info("Pivot[ing] dataframe")
    res7_pivot_df = service_delivery_metrics_munge.pivot_dataframe(res7_facts_annotated, INDEX_COLS)
    logging.info("Pivot[ed] dataframe")
    
    logging.info("Calculat[ing] metric values")
    res7_calc_df = service_delivery_metrics_munge.calculate_metrics_dataframe(res7_pivot_df, INDEX_COLS)
    logging.info("Calculat[ed] metric values")

    logging.info("Filter[ing] metrics to latest data date")
    res7_filt_df = service_delivery_metrics_munge.select_latest_value(res7_calc_df, INDEX_COLS)
    logging.info("Filter[ed] metrics to latest data date")

    logging.info("Calculat[ing] total requests attribute")
    res7_combined = service_delivery_metrics_munge.calc_total_values(res7_filt_df, res7_pivot_df, INDEX_COLS)
    res7_combined.drop(columns=[DATE_COL], inplace=True)
    logging.info("Calculat[ed] total requests attribute")

    logging.info("Dropp[ing] any entries where all metrics are NaNs")
    res7_combined = service_delivery_metrics_munge.drop_nas(res7_combined, INDEX_COLS)
    logging.info("Dropp[ed] any entries where all metrics are NaNs")
    
    # get top n request per hex
    logging.info(f"Filter[ing] to top {SELECT_TOP_N} codes per hex")
    top_n_codes_by_hex = res7_combined.sort_values([OPEN_COUNT], ascending=False).groupby([HEX_INDEX_COL]).head(
        SELECT_TOP_N)
    logging.info(f"Filter[ed] to top {SELECT_TOP_N} codes per hex")

    # add geometry and convert to geodf
    logging.info(f"Add[ing] hex geometry polygon")
    top_n_codes_by_hex[GEO_COL] = top_n_codes_by_hex[HEX_INDEX_COL].apply(
        lambda idx: make_shapely_poly(get_hex_boundary(idx)))
    logging.info(f"Add[ed] hex geometry polygon")

    # filter to only the target columns
    logging.info(f"Filter[ing] to target columns")
    top_n_codes_by_hex_filter = top_n_codes_by_hex[
        INDEX_COLS + [*METRIC_COLS, GEO_COL]].copy()
    logging.info(f"Filter[ing] to target columns")

    logging.info(f"Convert[ing] to geodataframe")
    top_n_codes_by_hex_geodf = gpd.GeoDataFrame(top_n_codes_by_hex_filter, geometry=GEO_COL, crs=f'epsg:4326')
    logging.info(f"Convert[ed] to geodataframe")

    # make geojson from scratch
    logging.info(f"Creat[ing] geojson from geodataframe")
    hex_groups = top_n_codes_by_hex_geodf.groupby(HEX_INDEX_COL)

    features = []
    for index, group_df in hex_groups:
        geometry = group_df["geometry"].to_list()[0]
        index_val = {HEX_INDEX_COL: index}
        master_reqs = group_df.fillna(0).to_dict(orient='records')
        for record in master_reqs:
            del record[HEX_INDEX_COL]
            del record["geometry"]

        request_types = {"request_types": master_reqs}
        properties = {**index_val, **request_types}
        features.append(geojson.Feature(properties=properties, geometry=geometry))
    feature_collection = geojson.FeatureCollection(features)
    logging.info(f"Creat[ed] geojson from geodataframe")

    with tempfile.TemporaryDirectory() as tdir, pathlib.Path(tdir) as tempdir:
        out_hex_geojson = pathlib.Path(tempdir, f"{CITY_SERVICE_METRICS_JSON}")
        geojson.dump(feature_collection, out_hex_geojson.open("w"))

        # put the file in minio
        logging.info(f"Push[ing] collected hex level 7 metrics geojson to minio")
        result = minio_utils.file_to_minio(
            filename=out_hex_geojson,
            minio_bucket=COVID_BUCKET,
            filename_prefix_override=PRIVATE_PREFIX,
            minio_key=secrets["minio"]["edge"]["access"],
            minio_secret=secrets["minio"]["edge"]["secret"],
            data_classification=EDGE_CLASSIFICATION,
        )

        if not result:
            logging.debug(f"Push[ing] data to minio failed")
            sys.exit(-1)
        logging.info(f"Push[ed] collected hex level 7 metrics geojson to minio")

    logging.info(f"Done")
