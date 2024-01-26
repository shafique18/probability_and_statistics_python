
import sys
sys.path.insert(
    0, "C:\\Users\\EHNIY\\OneDrive - Bayer (1)\\Learning\\probability_and_statistics_python")
import json
import pandas as pd
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pyspark.sql import SparkSession
from src.code.data_visualization.one_way_table import OneWayTable

spark = SparkSession.builder.appName("example").getOrCreate()

app = FastAPI()


def parse_csv(df):
    res = df.to_json()
    parsed = json.loads(res)
    return parsed


@app.get("/data_visualization/{library_name}/{colname_val}")
async def root(library_name, colname_val):
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
