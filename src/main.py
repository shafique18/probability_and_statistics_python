
import sys
sys.path.insert(
    0, "C:\\Users\\EHNIY\\OneDrive - Bayer (1)\\Learning\\probability_and_statistics_python")

from src.code.data_visualization.bar_chart_pie_chart import BarandPieCharts
from src.code.data_visualization.one_way_table import OneWayTable
from pyspark.sql import SparkSession
from fastapi import FastAPI
from io import BytesIO
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
import pandas as pd
import json


spark = SparkSession.builder.appName("example").getOrCreate()

app = FastAPI()


def parse_csv(df):
    res = df.to_json()
    parsed = json.loads(res)
    return parsed


@app.get("/data_visualization/one_way_table/{library_name}/{colname_val}")
async def oneWayTable(library_name, colname_val):
    if library_name == 'pandas':
        raw_data = pd.read_csv(
            "C:\\Users\\EHNIY\\OneDrive - Bayer (1)\\Learning\\probability_and_statistics_python\\data\house-prices-advanced-regression-techniques\\train.csv")
        object_val = OneWayTable(colname=colname_val, sortIndex=False)
        return JSONResponse(content=jsonable_encoder(object_val(raw_data).to_json()))
    elif library_name == 'pyspark':
        raw_data = spark.read.csv(
            "C:\\Users\\EHNIY\\OneDrive - Bayer (1)\\Learning\\probability_and_statistics_python\\data\house-prices-advanced-regression-techniques\\train.csv", header=True)
        object_val = OneWayTable(colname=colname_val, sortIndex=False)
        return JSONResponse(content=jsonable_encoder(object_val(raw_data).toJSON().collect()))
    else:
        return {"Error": "Given Librarys is not supported in the current version"}


@app.get("/data_visualization/bar_pie_chart/{library_name}/{chart_type}")
async def barAndPieCharts(library_name, chart_type):
    if library_name == 'pandas':
        raw_data = pd.read_csv(
            "C:\\Users\\EHNIY\\OneDrive - Bayer (1)\\Learning\\probability_and_statistics_python\\data\house-prices-advanced-regression-techniques\\train.csv")
        bins = [1870, 1880, 1890, 1900, 1910, 1920, 1930,
                1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010]
        raw_data['year_bin'] = pd.cut(
            raw_data['YearBuilt'], bins, ordered=True)
        
        plot_val = raw_data['year_bin'].value_counts(sort=False)
        object_val = BarandPieCharts(plot_val.index, plot_val.values, 'Built Year', 'matplotlib', 'bar')

        fig = object_val(raw_data)

        buf = BytesIO()
        fig.savefig(buf, format="png")
        
        buf.seek(0)
            
        return StreamingResponse(buf, media_type="image/png")

    # elif library_name == 'pyspark':
    #     raw_data = spark.read.csv(
    #         "C:\\Users\\EHNIY\\OneDrive - Bayer (1)\\Learning\\probability_and_statistics_python\\data\house-prices-advanced-regression-techniques\\train.csv", header=True)
    # else:
    #     return {"Error": f"Given Library { library_name } is not supported in the current version"}
