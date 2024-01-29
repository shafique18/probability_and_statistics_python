import sys
sys.path.insert(0, "C:\\Users\\EHNIY\\OneDrive - Bayer (1)\\Learning\\probability_and_statistics_python")

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from typing import Union

from src.code.data_visualization import DataVisualizationAbstract


class OneWayTable(DataVisualizationAbstract):
    """


    Args:
        DataVisualizationAbstract (_type_): _description_
    """

    def __init__(self, colname: str, sortIndex: bool = True) -> None:
        self.col_val = colname
        self.sortval = sortIndex

    def _transform(self, df: Union[pd.DataFrame, SparkDataFrame]):
        """


        Args:
            df (Union[pd.DataFrame, SparkDataFrame]): _description_
            lib (str, optional): _description_. Defaults to 'pandas'.
            sort (bool, optional): _description_. Defaults to True.
        """

        if isinstance(df, pd.DataFrame):
            if self.sortval:
                return pd.crosstab(index=df[self.col_val], columns='Count')
            else:
                return df[self.col_val].value_counts(sort=False)

        if isinstance(df, SparkDataFrame):
            if self.sortval:
                df = df.withColumn(self.col_val, df[self.col_val].cast('int'))
                return df.groupBy(self.col_val).count().orderBy(self.col_val)
            else:
                return df.groupBy(self.col_val).count()
