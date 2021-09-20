from energizer.io.utils import *
import pandas as pd
import numpy as np
import os
import pickle
from pathlib import Path
import geopandas as gpd
import datetime as dt


def read_all_files(raw=False, sample=False, filetype='parquet', hexagons=False, resolution='hex_res_7'):
    if filetype=='parquet':
        if hexagons:
                data_dir = Path("../../data/input/hexagons/"+resolution+"/")
                trip_data_df = pd.concat(
                    pd.read_parquet(parquet_file)
                    for parquet_file in data_dir.glob('*.parquet'))
                trip_data_df.reset_index(inplace=True, drop=True)
                return trip_data_df
        elif sample:
            data_dir = Path("../../data/input/trip_data_sample.parquet")
            trip_data = pd.read_parquet(data_dir)
            return trip_data
        else:
            if raw:
                data_dir = Path("../../data/input/raw/")
                trip_data_df = pd.concat(
                    pd.read_parquet(parquet_file)
                    for parquet_file in data_dir.glob('*.parquet'))
                trip_data_df.reset_index(inplace=True, drop=True)
                return trip_data_df
            if not raw:
                data_dir = Path("../../data/input/clean/")
                trip_data_df = pd.concat(
                    pd.read_parquet(parquet_file)
                    for parquet_file in data_dir.glob('*.parquet'))
                trip_data_df.reset_index(inplace=True, drop=True)
                return trip_data_df
    elif filetype=='geojson':
        data_dir = Path("../../data/input/Boundaries_Census_Tracts.geojson")
        geojson = gpd.read_file(data_dir)
        return geojson


def read_model(name="model.pkl"):
    model = pickle.load(open(os.path.join(get_data_path(), "output", "models", name + "_model.pkl"), 'rb'))
    return model


def cleaning_loop(trip_data, save=True, sample_set=False, frac=0.3):
    print('Let the cleaning begin ...')
    trip_data.drop_duplicates("trip_id", inplace=True)
    print('Dropped duplicates')
    trip_data.dropna(inplace=True)
    print('Dropped nan values for better data quality')

    trip_data.trip_id = trip_data.index
    print('Replaced trip_id')

    taxi_id = trip_data.taxi_id.unique()
    trip_data.taxi_id = trip_data.taxi_id.apply(lambda x: np.where(taxi_id == x)[0][0])
    print('Replaced taxi_id')

    numeric_cols = ["trip_id", "taxi_id", "trip_seconds", "trip_miles", "fare", "tips",
                    "tolls", "extras", "trip_total", "pickup_census_tract",
                    "pickup_community_area", "pickup_centroid_latitude",
                    "pickup_centroid_longitude", "dropoff_community_area",
                    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
                    "dropoff_census_tract"]
    string_cols = ["payment_type", "company"]
    datetime_cols = ["trip_start_timestamp", "trip_end_timestamp"]
    # geo_cols = ["pickup_centroid_location", "dropoff_centroid_location"]

    trip_data[numeric_cols] = trip_data[numeric_cols].apply(pd.to_numeric)
    print('Changed numeric types')
    trip_data[datetime_cols] = trip_data[datetime_cols].apply(pd.to_datetime)
    print('Changed datetime types')
    trip_data[string_cols] = trip_data[string_cols].astype(str)
    print('Changed string types')

    # DATA CLEANING SHOULD BE MADE HERE
    trip_data = trip_data[trip_data['trip_start_timestamp'] > '2013-01-01 00:00:00']
    trip_data = trip_data[trip_data['trip_start_timestamp'] < '2013-12-31 23:59:59']
    trip_data = trip_data[trip_data['trip_end_timestamp'] > '2013-01-01 00:00:00']
    trip_data = trip_data[trip_data['trip_start_timestamp'] < '2014-01-01 23:59:59']
    print('Cutted years')

    def hr_func(ts):
        return ts.hour

    trip_data['start_hour'] = trip_data['trip_start_timestamp'].apply(hr_func)
    trip_data

    trip_data = trip_data[trip_data['fare'].between(1, 7000)]
    trip_data = trip_data[trip_data['trip_miles'].between(0.01, 7000)]
    trip_data = trip_data[trip_data['trip_seconds'].between(20, 7000)]
    trip_data = trip_data[trip_data['trip_total'].between(1, 7000)]
    print('Filtered extreme values')

    if save:
        if sample_set:
            trip_data_sample = trip_data.sample(frac=frac)
            trip_data_sample.to_parquet(Path("../../data/input/trip_data_sample.parquet"))
            print('Sample set created')
            return trip_data_sample
        else:
            print('Saving/Overwriting in progress ...')
            trip_data1 = trip_data[:int(len(trip_data) * 0.25)]
            print('First split')
            trip_data2 = trip_data[int(len(trip_data) * 0.25):int(len(trip_data) * 0.5)]
            print('Second split')
            trip_data3 = trip_data[int(len(trip_data) * 0.5):int(len(trip_data) * 0.75)]
            print('Third split')
            trip_data4 = trip_data[int(len(trip_data) * 0.75):]
            print('Forth split')

            trip_data1.to_parquet(Path("../../data/input/clean/trip_data1.parquet"))
            print('Saved No. 1')

            trip_data2.to_parquet(Path("../../data/input/clean/trip_data2.parquet"))
            print('Saved No. 2')

            trip_data3.to_parquet(Path("../../data/input/clean/trip_data3.parquet"))
            print('Saved No. 3')

            trip_data4.to_parquet(Path("../../data/input/clean/trip_data4.parquet"))
            print('Saved No. 4')

    return trip_data