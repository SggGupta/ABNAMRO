import argparse
import pyspark
from pyspark.sql.functions import split, explode, col, filter , sum , rank
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window as w  




def it_data (df1: DataFrame, df2: DataFrame, path: str)  -> DataFrame:
    '''Returns a dataframe with info of people that are working in selling IT Department''' 
    output1 = df1.join(df2, df1.id == df2.id).select('df2.*').filter(df1.area == 'IT').sort('sales_amount', ascending=False)
    output1.limit(100).write.mode('overwrite').csv(path)

def marketing_address_info(df1: DataFrame, df2: DataFrame, path: str)  -> DataFrame:
    '''Returns a dataframe with info of Marketing Address Information '''
    output2 = df1.join(df2, df1.id == df2.id).select('df2.name','df2.address',explode(split(df2['address'],',')).alias("ZipCode")).filter(df1.area == 'Marketing')
    split_col = pyspark.sql.functions.split(output2['address'], ',')
    output2 = output2.withColumn('ZipCode', split_col.getItem(0))
    output2 = output2.withColumn('Address1', split_col.getItem(1))
    output2 = output2.withColumn('Address2', split_col.getItem(2))
    output2.select('name','ZipCode','address1','address2').write.mode('overwrite').csv(path)

def department_breakdown(df1: DataFrame, df2: DataFrame, path: str)  -> DataFrame:
    '''Returns a dataframe with Department Breakdown'''
    output3 = df1.join(df2, df1.id == df2.id).select('df1.area','df1.calls_made','df1.calls_successful','df2.sales_amount', 'df2.name' ).sort('sales_amount', ascending=False)
    output3 = output3.withColumn('percent', (col("df1.calls_successful")/col("df1.calls_made")* 100.0))
    output3 = output3.groupBy("area").agg(sum('df2.sales_amount').alias('sales_amount_sum'), sum('percent').alias('percent_sum'))
    output3 = output3.select('area','sales_amount_sum','percent_sum')
    output3.select('area','sales_amount_sum','percent_sum').write.mode('overwrite').csv(path)

def top_3_best_performers_per_department(df1: DataFrame, df2: DataFrame, path: str)  -> DataFrame:
    '''Returns a dataframe with Top 3 best performers per department'''
    output4 = df1.join(df2, df1.id == df2.id).select('df1.area','df1.calls_made','df1.calls_successful','df2.sales_amount', 'df2.name' ).sort('sales_amount', ascending=False)
    output4 = output4.withColumn('percent', (col("df1.calls_successful")/col("df1.calls_made")* 100.0))
    window = w.partitionBy(output4['area']).orderBy(output4['percent'].desc())
    output4 = output4.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3).filter(output4.percent>75)
    output4.select('area','name','percent').write.mode('overwrite').csv(path)

def top_3_most_sold_products_per_department(df1: DataFrame,df3: DataFrame,path: str)  -> DataFrame:
    '''Returns a dataframe with Top 3 most sold products per department in the Netherlands'''
    output5 = df1.join(df3, df1.id == df3.id).select('df3.*','df1.area').filter(df3.country == 'Netherlands')
    output5.select('df1.area','df3.product_sold','df3.quantity').groupBy('df1.area','df3.product_sold').agg(sum('df3.quantity'))
    window = w.partitionBy(output5['area']).orderBy(output5['quantity'].desc())
    result5 = output5.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3)
    result5.select('area','product_sold','quantity').write.mode('overwrite').csv(path)

def top_3_best_salesperson_department(df2: DataFrame,df3: DataFrame,path: str)  -> DataFrame:
    '''Returns a dataframe with the best overall salesperson per country'''
    output6 = df2.join(df3, df2.id == df3.caller_id).select('df3.country','df3.quantity','df2.name','df2.id','df2.sales_amount')
    window = w.partitionBy(output6['country']).orderBy(output6['sales_amount'].desc())
    result6 = output6.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 1)
    result6.select('name','country','quantity','sales_amount')
    result6.select('name','country','quantity','sales_amount').write.mode('overwrite').csv(path)

def top_3_best_salesperson_department_country(df1: DataFrame,df2: DataFrame,df3: DataFrame,path: str)  -> DataFrame:
    '''Returns a dataframe with the best overall salesperson per department per country'''
    output6 = df2.join(df3, df2.id == df3.caller_id).select('df3.country','df3.quantity','df2.name','df2.id','df2.sales_amount','df3.caller_id' )
    output6 = output6.join(df1, df2.id == df3.caller_id).select('df3.country','df3.quantity','df2.name','df2.id','df2.sales_amount','df1.area')
    window = w.partitionBy(output6['area'],output6['country']).orderBy(output6['sales_amount'].desc())
    result6 = output6.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 1)
    result6.select('name','area','country','quantity','sales_amount')
    result6.select('name','area','country','quantity','sales_amount').write.mode('overwrite').csv(path)



