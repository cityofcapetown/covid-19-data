# base imports
from dataclasses import dataclass
import json
import logging
import os
import sys
# external imports
from db_utils import minio_utils
import geopandas as gpd
from geospatial_utils import mportal_utils
from h3 import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon
# local imports
from service_delivery_metrics_munge import select_latest_value


@dataclass
class SDTLayer:
    layer: str
    resolution: str
    id_col: str
    id_rename: str


COVID_BUCKET = "covid"
SERVICE_FACTS_BUCKET = "service-standards-tool.sd-request-facts"
PUBLIC_PREFIX = "data/public/"
PRIVATE_PREFIX = "data/private/"
SUBCOUNCILS = "subcouncils.geojson"
EDGE_CLASSIFICATION = minio_utils.DataClassification.EDGE
LAKE_CLASSIFICATION = minio_utils.DataClassification.LAKE

# area layers
ABSD_AREA_LAYER = "Area_Based_Service_Delivery_Areas"
SUBCOUNCIL_LAYER = "Sub_Council"
WARD_LAYER = "Wards"
SUBURB_LAYER = "Official_Suburbs"

# area col names
ABSD_NAME = "ABSD_NAME"
ABSD_NAME_NEW = "ABSD_name"
SC_NAME = "SUB_CNCL_NAME"
SC_NAME_NEW = "SubCouncil_name"
WARD_NAME = "WARD_NAME"
WARD_NAME_NEW = "Ward_name"
SUBURB_NAME = "OFC_SBRB_NAME"
SUBURB_NAME_NEW = "Official_Suburb"

OUTPUT_RESOLUTION = "area_type"
OUTPUT_RESOLUTION_VAL = "area_name"
ABSD_AREA = "ABSD_area"
SUBCOUNCIL = "SubCouncil"
WARD = "Ward"
SUBURB = SUBURB_NAME_NEW

# outfile
OUTFILE_PREFIX = "business_continuity_service_delivery"
AREA_METRICS = f"{OUTFILE_PREFIX}_area_based_metrics"

ABSD_CLASS = SDTLayer(ABSD_AREA_LAYER, ABSD_AREA, ABSD_NAME, ABSD_NAME_NEW)
SC_CLASS = SDTLayer(SUBCOUNCIL_LAYER, SUBCOUNCIL, SC_NAME, SC_NAME_NEW)
WARD_CLASS = SDTLayer(WARD_LAYER, WARD, WARD_NAME, WARD_NAME_NEW)
SUBURB_CLASS = SDTLayer(SUBURB_LAYER, SUBURB, SUBURB_NAME, SUBURB_NAME_NEW)

AGGREGATION_CLASS_LIST = [ABSD_CLASS, SC_CLASS, WARD_CLASS]  # , SUBURB_CLASS]

# Notificaiton facts constants
TARGET_RES = 6
if TARGET_RES not in [6, 7, 10]:
    logging.error(f"target resolution {TARGET_RES} is not valid. must be one of [6, 7, 10]")
    sys.exit(-1)

RES = "resolution"
DATE = "date"
VALUE = "value"
FEATURE = "feature"
MEASURE = "measure"

OPENED_COUNT = "opened_count"
SERVICE_STD = "service_standard"
CLOSE_COUNT = "closed_count"
CLS_IN_TARGET = "closed_within_target_sum"
STILL_OPEN_SUM = "opened_still_open_sum"
BACKLOG = "backlog"
LONG_BACKLOG_WEIGHTING = "long_backlog_weighting"
LONG_BACKLOG = "long_backlog"

OVERLAP_TAG = "is_overlap"
SCALE_AREA = "scale_area"
HEX_AREA = "hex_area"
PERC_AREA = "percent_area"

TARGET_DATE = "2020-10-12"
DATE_FORMAT = "%Y-%m-%d"
STALENESS_THRESHOLD = pd.Timedelta('28')
STD_EPSG = "epsg:4326"
METERS_EPSG = "EPSG:3857"

AREA_INDEX = "area_index"
GEOMETRY = "geometry"
AREAS = "areas"
HEX_INDEX = "index"

LAT = "lat"
LONG = "long"
BACKLOG_OCT = "backlog_oct"
BACKLOG_DELTA = "backlog_delta"
BACKLOG_DELTA_RELATIVE = "backlog_delta_relative"
SS_OCT = "service_standard_oct"
SS_DELTA = "service_std_delta"
LONG_BACKLOG_OCT = "long_backlog_oct"
LONG_BACKLOG_DELTA = "long_backlog_delta"


def get_hex_boundary(hex_index):
    try:
        boundary = h3.h3_to_geo_boundary(hex_index, geo_json=True)
    except (ValueError, TypeError) as e:
        logging.error(f"Incorrect value passes as a hex string {hex_index}")
        sys.exit(-1)
    return boundary


def make_shapely_poly(x):
    return Polygon(x)


def get_hex_overlaps(request_geodf, area_geodf, groupby_col):
    """
    Get the Uber hex id's that fall within a given spatial area
    """

    area_list = []
    for area_name, grp_area_df in area_geodf.groupby([groupby_col]):
        new_area_frame = gpd.overlay(grp_area_df, request_geodf, how='intersection')
        indices = new_area_frame[HEX_INDEX].to_list()

        area_list += [{
            groupby_col: area_name,
            AREA_INDEX: indices,
        }]

    req_index_ref = pd.DataFrame(area_list)

    return req_index_ref


def conver_hex_to_area(request_geodf, area_geodf, groupby_col):
    """
    Convert Unber hex geodf to alternate spatial mapping
    """

    req_sc_index_df = get_hex_overlaps(request_geodf, area_geodf, groupby_col)

    area_collected_list = []
    for area_name, group_area_df in area_geodf.groupby([groupby_col]):
        overlap_hex = req_sc_index_df.query(f"{groupby_col} == @area_name").copy()[AREA_INDEX].to_list()[0]
        if not overlap_hex:
            logging.warning(f"No hex overlap for {area_name}")
            backlog = np.nan
            service_std = np.nan
            long_backlog = np.nan
        else:
            new_area_frame = gpd.overlay(group_area_df, request_geodf, how='intersection')
            new_area_frame[SCALE_AREA] = new_area_frame.geometry.area
            new_area_frame[HEX_AREA] = hex_area
            new_area_frame[PERC_AREA] = new_area_frame[SCALE_AREA] / new_area_frame[HEX_AREA]
            new_area_frame[OVERLAP_TAG] = new_area_frame[HEX_INDEX].apply(lambda x: True if x in overlap_hex else False)
            new_area_frame.loc[(new_area_frame[OVERLAP_TAG] == False), PERC_AREA] = 1
            new_area_frame[BACKLOG] = new_area_frame[BACKLOG] * new_area_frame[PERC_AREA]
            new_area_frame[CLS_IN_TARGET] = new_area_frame[CLS_IN_TARGET] * new_area_frame[PERC_AREA]
            new_area_frame[CLOSE_COUNT] = new_area_frame[CLOSE_COUNT] * new_area_frame[PERC_AREA]
            new_area_frame[STILL_OPEN_SUM] = new_area_frame[STILL_OPEN_SUM] * new_area_frame[PERC_AREA]

            # long backlog
            new_area_frame[LONG_BACKLOG_WEIGHTING] = new_area_frame[OPENED_COUNT] * new_area_frame[PERC_AREA]

            still_open_df = new_area_frame.reset_index().sort_values([DATE]).set_index([DATE]).groupby(
                [HEX_INDEX], sort=False).apply(
                lambda df: df[[STILL_OPEN_SUM, OPENED_COUNT]].resample("1D").sum().cumsum()).reset_index()

            max_date = still_open_df.reset_index()[DATE].max()

            still_open_df[LONG_BACKLOG_WEIGHTING] = still_open_df[OPENED_COUNT]
            still_open_df[LONG_BACKLOG] = still_open_df.set_index([DATE]).shift(180, freq="D").reset_index(drop=True)[
                STILL_OPEN_SUM]

            # remove dates past max date due to shift
            still_open_df = still_open_df.query(f"{DATE} <= @max_date").copy()

            # calc final long backlog ratio
            still_open_df[LONG_BACKLOG] /= still_open_df[LONG_BACKLOG_WEIGHTING]

            long_backlog = still_open_df[LONG_BACKLOG].to_list()[0]
            backlog = new_area_frame[BACKLOG].sum()
            try:
                service_std = new_area_frame[CLS_IN_TARGET].sum() / new_area_frame[CLOSE_COUNT].sum()
            except ZeroDivisionError as e:
                logging.error(f"zero value for closed count {new_area_frame}\n{e}")
                sys.exit(-1)

        area_collected_list += [{
            groupby_col: area_name,
            BACKLOG: backlog,
            SERVICE_STD: service_std,
            LONG_BACKLOG: long_backlog,
        }]

    collected_area_df = pd.DataFrame(area_collected_list)

    return collected_area_df


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

    # ---------------------------
    logging.info(f"Fetch[ing] {SERVICE_FACTS_BUCKET}")
    service_facts = minio_utils.minio_to_dataframe(
        minio_bucket=SERVICE_FACTS_BUCKET,
        minio_key=secrets["minio"]["lake"]["access"],
        minio_secret=secrets["minio"]["lake"]["secret"],
        data_classification=LAKE_CLASSIFICATION,
    )
    logging.info(f"Fetch[ed] {SERVICE_FACTS_BUCKET}")

    # ---------------------------
    logging.info(f"Filter[ing] to hex resolution {TARGET_RES}")
    service_facts_hex_level = service_facts.query(f"{RES} == @TARGET_RES").copy().assign(
        date=lambda df: pd.to_datetime(df.date, format=DATE_FORMAT))
    logging.info(f"Filter[ed] to hex resolution {TARGET_RES}")

    logging.info(f"Pivot[ing] df")
    city_pivot_df = service_facts_hex_level.groupby(
        [DATE, HEX_INDEX, MEASURE], as_index=False).sum().pivot_table(
        columns=MEASURE,
        values=VALUE,
        index=[DATE, HEX_INDEX]
    ).fillna(0)
    logging.info(f"Pivot[ed] df")

    # ---------------------------
    # backlog calculation
    logging.info(f"Calculat[ing] backlog")
    city_pivot_df[BACKLOG] = city_pivot_df[OPENED_COUNT] - city_pivot_df[CLOSE_COUNT]
    logging.info(f"Calculat[ed] backlog")

    # rolling 6 month sums
    logging.info(f"Calculat[ing] rolling sums")
    city_backlog_df = city_pivot_df.groupby([HEX_INDEX]).apply(
        lambda df: df.rolling("180D", on=df.index.get_level_values(DATE)).sum()
    ).reset_index()
    logging.info(f"Calculat[ed] rolling sums")

    # ---------------------------
    logging.info("Check[ing] backlog loss due to no locs")
    total_backlog = city_backlog_df.drop_duplicates(subset=[HEX_INDEX], keep="last")[BACKLOG].sum()
    backlog_with_locs = city_backlog_df.query(f"{HEX_INDEX} != '0'").drop_duplicates(
        subset=[HEX_INDEX],
        keep="last"
    )[BACKLOG].sum()
    logging.info(f"Total backlog = {total_backlog}")
    logging.info(f"backlog with locs = {backlog_with_locs}")
    logging.info(f"backlog % with no locs = {(total_backlog - backlog_with_locs) / total_backlog * 100}")
    logging.info("Check[ed] backlog loss due to no locs")

    # ---------------------------
    # filter to requests with locations
    logging.info("Filter[ing] out entries with no locations")
    city_backlog_df_filt = city_backlog_df.query(f"{HEX_INDEX} != '0'").copy()
    logging.info("Filter[ed] out entries with no locations")

    # get reference values
    logging.info(f"Filter[ing] to {TARGET_DATE} data")
    city_backlog_df_oct = select_latest_value(city_backlog_df_filt, [HEX_INDEX], cut_off_date=TARGET_DATE)
    logging.info(f"Filter[ed] to {TARGET_DATE} data")

    # get latets values
    logging.info("Filter[ing] to latest data")
    city_backlog_df_latest = select_latest_value(city_backlog_df_filt, [HEX_INDEX])
    logging.info("Filter[ing] to latest data")

    # ---------------------------
    # add geometry
    logging.info("Add[ing] geometry")
    city_backlog_df_oct[GEOMETRY] = city_backlog_df_oct[HEX_INDEX].apply(
        lambda x: make_shapely_poly(get_hex_boundary(x)))
    city_backlog_df_latest[GEOMETRY] = city_backlog_df_latest[HEX_INDEX].apply(
        lambda x: make_shapely_poly(get_hex_boundary(x)))

    city_backlog_df_oct_gdf = gpd.GeoDataFrame(city_backlog_df_oct, geometry=GEOMETRY, crs=STD_EPSG)
    city_backlog_df_latest_geo = gpd.GeoDataFrame(city_backlog_df_latest, geometry=GEOMETRY, crs=STD_EPSG)

    # remove bad hex indices
    city_backlog_df_latest_geo[LONG] = city_backlog_df_latest_geo[GEOMETRY].apply(lambda x: x.exterior.coords[0][0])
    city_backlog_df_latest_geo[LAT] = city_backlog_df_latest_geo[GEOMETRY].apply(lambda x: x.exterior.coords[0][1])

    city_backlog_df_oct_gdf[LONG] = city_backlog_df_oct_gdf[GEOMETRY].apply(lambda x: x.exterior.coords[0][0])
    city_backlog_df_oct_gdf[LAT] = city_backlog_df_oct_gdf[GEOMETRY].apply(lambda x: x.exterior.coords[0][1])

    exclude = list(
        city_backlog_df_latest_geo.query(
            f"{LAT} > -32 or {LAT} < -34.6 or {LONG} < 18 or {LONG} > 19")["index"].unique())
    exclude += list(city_backlog_df_oct_gdf.query(
        f"{LAT} > -32 or {LAT} < -34.6 or {LONG} < 18 or {LONG} > 19")["index"].unique())

    city_backlog_df_latest_geo = city_backlog_df_latest_geo.query("index not in @exclude").copy()
    city_backlog_df_oct_gdf = city_backlog_df_oct_gdf.query("index not in @exclude").copy()

    if city_backlog_df_latest_geo.empty or city_backlog_df_oct_gdf.empty:
        logging.error(f"Empty dataframe after filtering bad hex indexes")
        sys.exit(-1)

    logging.info("Add[ed] geometry")

    # ---------------------------
    # convert to meters for area calc
    logging.info("Convert[ing] geometry to meters EPSG for Area calculation")

    city_backlog_df_latest_geo = city_backlog_df_latest_geo.to_crs(METERS_EPSG)
    city_backlog_df_oct_gdf = city_backlog_df_oct_gdf.to_crs(METERS_EPSG)
    logging.info("Convert[ed] geometry to meters EPSG for Area calculation")

    # get size of given hex area
    logging.info(f"Calculat[ing] res {TARGET_RES} area")
    hex_area = city_backlog_df_latest_geo[GEOMETRY].apply(lambda x: x.area).to_list()[0]
    logging.info(f"Calculat[ed] res {TARGET_RES} area")

    # ---------------------------
    all_areas_collected_df = pd.DataFrame()
    for layer_data_class in AGGREGATION_CLASS_LIST:
        area_layer = layer_data_class.layer
        resolution_name = layer_data_class.resolution
        targt_col_name = layer_data_class.id_col
        target_col_rename = layer_data_class.id_rename

        logging.info(f"Fetch[ing] {area_layer}")
        area_df = mportal_utils.load_mportal_layer(
            area_layer,
            minio_key=secrets["minio"]["lake"]["access"],
            minio_secret=secrets["minio"]["lake"]["secret"],
            return_gdf=True,
        )
        logging.info(f"Fetch[ed] {area_layer}")

        logging.debug(f"Renam[ing] {targt_col_name} to {target_col_rename}")
        area_df.rename(columns={targt_col_name: target_col_rename}, inplace=True)
        logging.debug(f"Renam[ed] {targt_col_name} to {target_col_rename}")

        logging.info("Convert[ing] geometry to meters EPSG for Area calculation reference df")
        area_geo_df = area_df.to_crs(METERS_EPSG).copy()
        logging.info("Convert[ed] geometry to meters EPSG for Area calculation reference df")

        # ---------------------------
        # get latest subcouncil mapping
        logging.info(f"Mapp[ing] hex to area for latest data")
        area_remapped_df_latest = conver_hex_to_area(
            city_backlog_df_latest_geo, area_geo_df, groupby_col=target_col_rename
        )
        logging.info(f"Mapp[ed] hex to area for latest data")

        # ---------------------------
        # get 2020-10-12 subcouncil mapping
        logging.info(f"Mapp[ing] hex to area for target date data")
        area_remapped_df_latest_oct = conver_hex_to_area(
            city_backlog_df_oct_gdf, area_geo_df, groupby_col=target_col_rename
        )
        area_remapped_df_latest_oct.rename(
            columns={
                BACKLOG: BACKLOG_OCT,
                SERVICE_STD: SS_OCT,
                LONG_BACKLOG: LONG_BACKLOG_OCT
            }, inplace=True)
        logging.info(f"Mapp[ed] hex to area for target date data")

        # ---------------------------
        # merge data
        logging.info(f"Merg[ing] latest and target date dfs")
        combined_remapped_df = pd.merge(
            area_remapped_df_latest,
            area_remapped_df_latest_oct,
            how="left",
            on=[target_col_rename],
            validate="1:1"
        )
        logging.info(f"Merg[ed] latest and target date dfs")

        # ---------------------------
        # calculate differences
        logging.info(f"Calculat[ing] metric differences between target and latest")
        combined_remapped_df[BACKLOG_DELTA] = combined_remapped_df[BACKLOG] - combined_remapped_df[BACKLOG_OCT]
        combined_remapped_df[BACKLOG_DELTA_RELATIVE] = (combined_remapped_df[BACKLOG_DELTA] /
                                                        combined_remapped_df[BACKLOG_OCT])
        combined_remapped_df[SS_DELTA] = combined_remapped_df[SERVICE_STD] - combined_remapped_df[SS_OCT]
        combined_remapped_df[LONG_BACKLOG_DELTA] = (combined_remapped_df[LONG_BACKLOG] -
                                                    combined_remapped_df[LONG_BACKLOG_OCT])
        logging.info(f"Calculat[ed] metric differences between target and latest")

        # reorder columns
        logging.info(f"Reorder[ing] columns")
        combined_remapped_df = combined_remapped_df[
            [target_col_rename,
             BACKLOG, BACKLOG_DELTA, BACKLOG_DELTA_RELATIVE,
             LONG_BACKLOG, LONG_BACKLOG_DELTA,
             SERVICE_STD, SS_DELTA]
        ].sort_values(target_col_rename)

        logging.info(f"Reorder[ed] columns")

        combined_remapped_df[OUTPUT_RESOLUTION] = resolution_name
        combined_remapped_df.rename(columns={target_col_rename: OUTPUT_RESOLUTION_VAL}, inplace=True)
        melted_df = pd.melt(
            combined_remapped_df,
            id_vars=[OUTPUT_RESOLUTION, OUTPUT_RESOLUTION_VAL],
            value_vars=[
                BACKLOG, BACKLOG_DELTA, BACKLOG_DELTA_RELATIVE,
                LONG_BACKLOG, LONG_BACKLOG_DELTA,
                SERVICE_STD, SS_DELTA
            ]
        )

        all_areas_collected_df = pd.concat([all_areas_collected_df, combined_remapped_df])

    # re-order columns for happy ocd
    all_areas_collected_df = all_areas_collected_df[[OUTPUT_RESOLUTION, OUTPUT_RESOLUTION_VAL, BACKLOG, BACKLOG_DELTA,
                                                     BACKLOG_DELTA_RELATIVE, LONG_BACKLOG, LONG_BACKLOG_DELTA,
                                                     SERVICE_STD, SS_DELTA]].copy()

    # put the file in minio
    logging.info(f"Push[ing] collected metrics data to minio")
    result = minio_utils.dataframe_to_minio(
        all_areas_collected_df,
        filename_prefix_override=f"{PRIVATE_PREFIX}{AREA_METRICS}",
        minio_bucket=COVID_BUCKET,
        minio_key=secrets["minio"]["edge"]["access"],
        minio_secret=secrets["minio"]["edge"]["secret"],
        data_classification=EDGE_CLASSIFICATION,
        data_versioning=False,
        file_format="csv"
    )

    if not result:
        logging.debug(f"Send[ing] data to minio failed")
    logging.info(f"Push[ed] collected  metrics data to minio")

    logging.info(f"Done")
