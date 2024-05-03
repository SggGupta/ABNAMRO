import pytest
import unittest

from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from src.main import top_3_most_sold_products_per_department

spark = SparkSession.builder.appName("chispa_test").getOrCreate()

# Define unit test base class
class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Sample PySpark ETL").getOrCreate()


# Define unit test
class TestTranformation(PySparkTestCase):
    def test_single_space(self):

        df1 = spark.read.csv('/Users/sapnagupta/Downloads/latest/test/resources/dataset_one.csv',header='true', inferSchema='true')
        df2 = spark.read.csv('/Users/sapnagupta/Downloads/latest/test/resources/dataset_two.csv',header='true', inferSchema='true')
        df3 = spark.read.csv('/Users/sapnagupta/Downloads/latest/test/resources/dataset_three.csv',header='true', inferSchema='true')

        df1 = df1.alias('df1')
        df2 = df2.alias('df2')
        df3 = df3.alias('df3')

        path = 'file:///Users/sapnagupta/Downloads/latest/test/top_3_most_sold_products_per_department'

        # Apply the transformation function from before
        top_3_most_sold_products_per_department(df1, df3, path)
        transformed_df = spark.read.csv('/Users/sapnagupta/Downloads/latest/test/top_3_most_sold_products_per_department/*.csv', inferSchema='true')

        expected_df = spark.read.csv('/Users/sapnagupta/Downloads/latest/test/expected_top_3_most_sold_products_per_department/*.csv', inferSchema='true')

        expected_df = expected_df.alias('expected_df')

        assert_df_equality(transformed_df, expected_df)