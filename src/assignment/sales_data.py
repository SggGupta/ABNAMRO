import argparse
import pyspark
from pyspark.sql import SparkSession
import logging
from logging import Logger
from typing import Dict, Any, Optional
from main import it_data
from main import marketing_address_info
from main import department_breakdown
from main import top_3_best_performers_per_department
from main import top_3_most_sold_products_per_department
from main import top_3_best_salesperson_department
from main import top_3_best_salesperson_department_country


class LoggerProvider:
    def get_logger(self, spark: SparkSession, custom_prefix: Optional[str] = ""):
        log4j_logger = spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(custom_prefix + self.__full_name__())

    def __full_name__(self):
        klass = self.__class__
        module = klass.__module__
        if module == "__builtin__":
            return klass.__name__  # avoid outputs like '__builtin__.str'
        return module + "." + klass.__name__


def run (spark, path):  
    df1 = spark.read.csv(path+'/dataset_one.csv',header='true', inferSchema='true')
    df2 = spark.read.csv(path+'/dataset_two.csv',header='true', inferSchema='true')
    df3 = spark.read.csv(path+'/dataset_three.csv',header='true', inferSchema='true')

    df1 = df1.alias('df1')
    df2 = df2.alias('df2')
    df3 = df3.alias('df3')

    path1 = './output/it_data'
    it_data(df1, df2, path1)
    path2 = './output/marketing_address_info'
    marketing_address_info(df1, df2,path2)   
    path3 = './output/department_breakdown'
    output3 = department_breakdown(df1, df2,path3)
    path4 =  './output/top_3_best_performers_per_department'  
    top_3_best_performers_per_department(df1,df2,path4)
    path5 = './output/top_3_most_sold_per_department_netherlands'
    top_3_most_sold_products_per_department(df1,df3,path5)
    path6 = './output/best_salesperson'
    top_3_best_salesperson_department(df2,df3,path6)
    path7 = './output/best_salesperson_department'
    top_3_best_salesperson_department_country(df1,df2,df3,path7)
    logging.debug("Debug message: END of the application")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='ABN AMRO assignment')
    parser.add_argument('--path', type=str, required=True, dest='path',
                        help='Path to the resources')

    args = parser.parse_args()
    print(args.path)
    print('User path is -> {}'.format(args.path))
    logging.info("ABN AMRO assignment")
    logging.debug("Debug message: START of the application")
    spark = SparkSession \
        .builder \
        .appName(args.path) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")
    run(spark,args.path)