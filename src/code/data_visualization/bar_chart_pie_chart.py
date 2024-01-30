import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame as SparkDataFrame
import seaborn as sns
import plotly.express as px

from src.code.data_visualization import DataVisualizationAbstract


class BarandPieCharts(DataVisualizationAbstract):
    """
    This class is intended to provide a single interface for Bar graph and Pie charts

    """

    def __init__(self,
                 x_column: str,
                 y_column: str,
                 chart_title: str,
                 library_name: str = 'plotly',
                 chart_type: str = 'bar') -> None:
        """
        Constructor
        """
        self.x_column = x_column
        self.y_column = y_column
        self.chart_title = chart_title
        self.library_name = library_name
        self.chart_type = chart_type

    def _transform(self, df):
        """

        """
        try:
            if isinstance(df, SparkDataFrame):
                df = df.toPandas()
            if self.chart_type == 'bar':
                if self.library_name == 'plotly':
                    fig = px.bar(df, x=self.x_column, y=self.y_column,
                                 title=self.chart_title)
                if self.library_name == 'matplotlib':
                    fig = plt.bar(self.x_column, self.y_column)
            elif self.chart_type == 'pie':
                if self.library_name == 'plotly':
                    fig = px.pie(
                        df, values=df[self.x_column], names=df[self.y_column],
                        title=self.chart_title)
                if self.library_name == 'matplotlib':
                    fig = plt.pie(df.groupby(self.x_column).size(
                    ), labels=df[self.x_column].unique(), autopct='%1.00f%%')

            return fig
        finally:
            plt.close()

# Matplotlib
# Seaborn
# Plotnine(ggplot)
# Bokeh
# pygal
# Plotly
# geoplotlib
# Gleam
# missingno
# Leather
# Altair
# Folium
# PyGWalker
# pyecharts
# plotnine
# Holoviews
# vispy
