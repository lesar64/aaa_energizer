from energizer import *

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import timedelta, datetime
from IPython.display import set_matplotlib_formats
from pathlib import Path
import energizer

import h3
import shapely
import geopandas as gpd
import contextily as ctx
from shapely.geometry import shapely
import json
from h3 import h3
from shapely.geometry import shape
import folium
from tqdm import tqdm

pd.set_option('display.max_columns', None)
set_matplotlib_formats('retina')


# Generates hexagon adresses per trip with for given resolution
def generate_hexagons(df, h3_resolution):
    lst1 = []
    lst2 = []
    for i, rows in tqdm(df.iterrows()):
        hex1, hex2 = 0, 0
        hex1 = h3.geo_to_h3(df['pickup_centroid_latitude'][i], df['pickup_centroid_longitude'][i], h3_resolution)
        hex2 = h3.geo_to_h3(df['dropoff_centroid_latitude'][i], df['dropoff_centroid_longitude'][i], h3_resolution)
        lst1.append(hex1)
        lst2.append(hex2)
    df['hex_ID_pickup'] = lst1
    df['hex_ID_dropoff'] = lst2
    return df


# Saves trip_data with hexagon ids
def saving_hexagons(trip_data, resolution):
    print('Saving/Overwriting in progress ...')
    trip_data1 = trip_data[:int(len(trip_data) * 0.25)]
    print('First split')
    trip_data2 = trip_data[int(len(trip_data) * 0.25):int(len(trip_data) * 0.5)]
    print('Second split')
    trip_data3 = trip_data[int(len(trip_data) * 0.5):int(len(trip_data) * 0.75)]
    print('Third split')
    trip_data4 = trip_data[int(len(trip_data) * 0.75):]
    print('Forth split')

    trip_data1.to_parquet(Path("../../data/input/hexagons/" + resolution + "/trip_data1.parquet"))
    print('Saved No. 1')

    trip_data2.to_parquet(Path("../../data/input/hexagons/" + resolution + "/trip_data2.parquet"))
    print('Saved No. 2')

    trip_data3.to_parquet(Path("../../data/input/hexagons/" + resolution + "/trip_data3.parquet"))
    print('Saved No. 3')

    trip_data4.to_parquet(Path("../../data/input/hexagons/" + resolution + "/trip_data4.parquet"))
    print('Saved No. 4')
    print('Saving job finished')


# A function which transforms a data frame with Uber h3 hexagon indices into a geo data frame
def transform_df_to_gdf(df, crs='EPSG:4326'):
    return gpd.GeoDataFrame(data=df,
                            geometry=list(map(lambda h3_index: shapely.geometry.Polygon(
                                h3.h3_to_geo_boundary(h=h3_index, geo_json=True)), df.index)),
                            crs=crs)


# A function plotting a choropleth map for a geo data frame. It zooms in to Berlin and adds points of interest.
def plot_static_choropleth_map(gdf, ax, title, column='num_trips', label='Number of trips by hexagon', cmap='YlOrRd',
                               crs='EPSG:4326', vmax=None):
    # Construct choropleth map
    #     if vmax is None:
    #         gdf[column].max()

    gdf.cx[-87.85:-87.50, 41.65:42.05].plot(column=column, ax=ax, cmap=cmap, alpha=0.7, legend=True, vmax=vmax,
                                            legend_kwds={'label': label, 'orientation': 'horizontal'})
    ctx.add_basemap(ax=ax, crs=crs)
    ax.set_title(title)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')

    # Airport
    ax.scatter(x=-87.95, y=41.975084, label='Airport', color='red')


def trip_start_vis(df):
    start_count_df = df.groupby('hex_ID_pickup').agg({'trip_id': 'count'}, axis=1).rename({'trip_id': 'num_trips'},
                                                                                           axis=1)
    start_count_gdf = transform_df_to_gdf(start_count_df)

    end_count_df = df.groupby('hex_ID_dropoff').agg({'trip_id': 'count'}, axis=1).rename({'trip_id': 'num_trips'},
                                                                                          axis=1)
    end_count_gdf = transform_df_to_gdf(end_count_df)

    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(15, 15))
    plot_static_choropleth_map(start_count_gdf, ax1, 'Overall number of start trips per hexagon')
    plot_static_choropleth_map(end_count_gdf, ax2, 'Overall number of end trips per hexagon')