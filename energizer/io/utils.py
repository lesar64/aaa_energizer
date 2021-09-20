import os


def get_data_path():
    if os.path.isdir(os.path.join(os.getcwd(), 'data')):
        return os.path.join(os.getcwd(), 'data')
    elif os.path.isdir(os.path.join(os.getcwd(), "..", "data")):
        return os.path.join(os.getcwd(), "..", "data")
    elif os.path.isdir(os.path.join(os.getcwd(), "..", "..", "data")):
        return os.path.join(os.getcwd(), "..", "..", "data")
    else:
        raise FileNotFoundError


def merge_weather(df,df_weather):
    df_weather["datetime"]=pd.to_datetime(df_weather['datetime'])
    df_weather.rename(columns={"datetime": "round_datetime" }, inplace=True)
    # eine Spalte mit Datum
    df["round_datetime"] = df['trip_start_timestamp'].dt.round('H')
    df = pd.merge(df, df_weather, on="round_datetime", how='left') #merge city data with weather data
    df=df.drop("round_datetime", 1)
    return df