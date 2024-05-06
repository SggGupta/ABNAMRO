import argparse
import os
import pyspark
import logging
import logging.config

from pyspark.sql.functions import split, explode, col, filter , sum , rank
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as w  

### Output #1 - **IT Data**
def it_data(df1, df2,path):
    output1 = df1.join(df2, df1.id == df2.id).select('df2.*').filter(df1.area == 'IT').sort('sales_amount', ascending=False)
    output1.limit(100).write.mode('overwrite').csv(path)


### Output #2 - **Marketing Address Information**
def marketing_address_info(df1, df2,path):
    output2 = df1.join(df2, df1.id == df2.id).select('df2.name','df2.address',explode(split(df2['address'],',')).alias("ZipCode")).filter(df1.area == 'Marketing')
    split_col = pyspark.sql.functions.split(output2['address'], ',')
    output2 = output2.withColumn('ZipCode', split_col.getItem(0))
    output2 = output2.withColumn('Address1', split_col.getItem(1))
    output2 = output2.withColumn('Address2', split_col.getItem(2))
    output2.select('name','ZipCode','address1','address2').write.mode('overwrite').csv(path)


### Output #3 - **Department Breakdown**
def department_breakdown(df1, df2,path):
    output3 = df1.join(df2, df1.id == df2.id).select('df1.area','df1.calls_made','df1.calls_successful','df2.sales_amount', 'df2.name' ).sort('sales_amount', ascending=False)
    output3 = output3.withColumn('percent', (col("df1.calls_successful")/col("df1.calls_made")* 100.0))
    output3 = output3.groupBy("area").agg(sum('df2.sales_amount').alias('sales_amount_sum'), sum('percent').alias('percent_sum'))
    output3 = output3.select('area','sales_amount_sum','percent_sum')
    output3.select('area','sales_amount_sum','percent_sum').write.mode('overwrite').csv(path)


### Output #4 - **Top 3 best performers per department**
def top_3_best_performers_per_department(df1, df2, path):
    output4 = df1.join(df2, df1.id == df2.id).select('df1.area','df1.calls_made','df1.calls_successful','df2.sales_amount', 'df2.name' ).sort('sales_amount', ascending=False)
    output4 = output4.withColumn('percent', (col("df1.calls_successful")/col("df1.calls_made")* 100.0))
    window = w.partitionBy(output4['area']).orderBy(output4['percent'].desc())
    output4 = output4.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3).filter(output4.percent>75)
    output4.select('area','name','percent').write.mode('overwrite').csv(path)


### Output #5 - **Top 3 most sold products per department in the Netherlands**
def top_3_most_sold_products_per_department(df1,df3,path):
    output5 = df1.join(df3, df1.id == df3.id).select('df3.*','df1.area').filter(df3.country == 'Netherlands')
    output5.select('df1.area','df3.product_sold','df3.quantity').groupBy('df1.area','df3.product_sold').agg(sum('df3.quantity'))
    window = w.partitionBy(output5['area']).orderBy(output5['quantity'].desc())
    result5 = output5.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3)
    result5.select('area','product_sold','quantity').write.mode('overwrite').csv(path)


### Output #6 - **Who is the best overall salesperson per country**
def top_3_best_salesperson_department(df2,df3,path):
    df3.show()
    output6 = df2.join(df3, df2.id == df3.caller_id).select('df3.country','df3.quantity','df2.name','df2.id','df2.sales_amount')
    window = w.partitionBy(output6['country']).orderBy(output6['sales_amount'].desc())
    result6 = output6.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 1)
    result6.select('name','country','quantity','sales_amount')
    result6.select('name','country','quantity','sales_amount').write.mode('overwrite').csv(path)


### Output #6 - **Who is the best overall salesperson per department per country**
def top_3_best_salesperson_department_country(df1,df2,df3,path):
    df3.show()
    output6 = df2.join(df3, df2.id == df3.caller_id).select('df3.country','df3.quantity','df2.name','df2.id','df2.sales_amount','df3.caller_id' )
    output6 = output6.join(df1, df2.id == df3.caller_id).select('df3.country','df3.quantity','df2.name','df2.id','df2.sales_amount','df1.area')
    window = w.partitionBy(output6['area'],output6['country']).orderBy(output6['sales_amount'].desc())
    result6 = output6.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 1)
    result6.select('name','area','country','quantity','sales_amount')
    result6.select('name','area','country','quantity','sales_amount').write.mode('overwrite').csv(path)



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

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='ABN AMRO assignment')
    parser.add_argument('--path', type=str, required=True, dest='path',
                        help='Path to the resources')

    args = parser.parse_args()
    print(args.path)
    print('User path is -> {}'.format(args.path))
    spark = SparkSession \
        .builder \
        .appName(args.path) \
        .getOrCreate()
    
    run(spark,args.path)

