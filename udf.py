from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import shutil
from pyspark.sql import Row
from pyspark.sql.types import *
spark = SparkSession.builder.appName("flatmap testing").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
data1 = spark.read.csv("C:/input/transaction.csv",sep=",",inferSchema=True,header=True)
def validator(value):
    print(value)
    if value == "20-04-2018":
       return "2018"
custom_udf = F.udf(validator,StringType())
df1 = data1.withColumn("year",custom_udf("transaction_date"))
df1.show()