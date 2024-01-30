from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from src.code.data_visualization import DataVisualizationAbstract


class LineChartAndOgive(DataVisualizationAbstract):
    """


    Args:
        DataVisualizationAbstract (_type_): _description_
    """

    def __init__(self, x_column: str, y_column: str, library_name: str = 'matplotlib', title: str = 'Line Chart') -> None:
        """


        Args:
            x_column (str): _description_
            y_column (str): _description_
        """
        self.x_column = x_column
        self.y_column = y_column
        self.library_name = library_name
        self.title = title

    def _transform(self, df):
        if isinstance(df, SparkDataFrame):
            df = df.toPandas()

        if self.library_name == 'matplotlib':
            chart = plt.plot(df[self.x_column],
                             df[self.y_column],
                             linewidth=0.5,
                             linestyle='--',
                             color='b',
                             marker='o',
                             markersize=10,
                             markerfacecolor='red')
            chart.xticks(rotation='vertical')
            return chart
        elif self.library_name == 'plotly':
            fig = px.line(df, x=self.x_column,
                          y=self.y_column, title=self.title)
            return fig
