import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import SparkDataFrame
from typing import Union

from data_visualization import DataVisualizationAbstract


class OneWayTable(DataVisualizationAbstract):
    """


    Args:
        DataVisualizationAbstract (_type_): _description_
    """

    def transform(self, df: Union[pd.DataFrame, SparkDataFrame], colname: str, sortIndex: bool = True):
        """


        Args:
            df (Union[pd.DataFrame, SparkDataFrame]): _description_
            lib (str, optional): _description_. Defaults to 'pandas'.
            sort (bool, optional): _description_. Defaults to True.
        """

        if isinstance(df, pd.DataFrame):
            if sortIndex:
                return pd.crosstab(index=df[colname], columns='Count')
            else:
                return df[colname].value_counts(sort=False)

        if isinstance(df, SparkDataFrame):
            if sortIndex:
                saprk_df = df.withColumn(colname, df[colname].cast('int'))
                return saprk_df.groupBy(colname).count().orderBy(colname)
            else:
                return saprk_df.groupBy(colname).count()
