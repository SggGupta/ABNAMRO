import pytest
import unittest

from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from src.main import top_3_best_salesperson_department_country

spark = SparkSession.builder.appName("chispa_test").getOrCreate()

# Define unit test base class
class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Sample PySpark ETL").getOrCreate()


# Define unit test
class TestTranformation(PySparkTestCase):
    def test_single_space(self):

        df1 = spark.read.csv('./test/resources/dataset_one.csv',header='true', inferSchema='true')
        df2 = spark.read.csv('./test/resources/dataset_two.csv',header='true', inferSchema='true')
        df3 = spark.read.csv('./test/resources/dataset_three.csv',header='true', inferSchema='true')

        df1 = df1.alias('df1')
        df2 = df2.alias('df2')
        df3 = df3.alias('df3')



        path = './test/top_3_best_salesperson_department_country'

        # Apply the transformation function from before
        top_3_best_salesperson_department_country(df1,df2,df3,path)
        transformed_df = spark.read.csv('./test/top_3_best_salesperson_department_country/*.csv', inferSchema='true')
        expected_df = spark.read.csv('./test/expected_top_3_best_salesperson_department_country/*.csv', inferSchema='true')

        expected_df = expected_df.alias('expected_df')

        assert_df_equality(transformed_df, expected_df)