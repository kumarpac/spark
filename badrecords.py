
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import shutil
from pyspark.sql import Row
from pyspark.sql.types import *
spark = SparkSession.builder.appName("flatmap testing").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
data1 = spark.read.csv("C:/input/transaction.csv",sep=",",inferSchema=True,header=True)
df1 = data1.withColumn("year",F.substring("transaction_date",7,9))
df1.createOrReplaceTempView("addCol")
datalookup = spark.read.csv("C:/input/valid_year.csv",sep=",",inferSchema=True,header=True)
datalookup.createOrReplaceTempView("datalookup")
rejectedfilepath = "C:/input/invalidrecord.csv"
validrecord = "C:/input/validrecord.csv"
if os.path.exists(validrecord):
    try:
      shutil.rmtree(validrecord)
    except Exception as e:
        print("e")
if os.path.exists(rejectedfilepath):
    try:
      shutil.rmtree(rejectedfilepath)
    except Exception as e:
        print("e")
spark.sql("select transaction_id,username,usertype,transaction_date,year from" +"(select * from addCol a left join datalookup b on a.year = b.valid_year)" +
          "where valid_year is null").write.save("C:/input/invalidrecord.csv",format="csv")

spark.sql("select transaction_id,username,usertype,transaction_date,year from" +"(select * from addCol a left join datalookup b on a.year = b.valid_year)" +
          "where valid_year is not null").write.save("C:/input/validrecord.csv",format="csv")
# data1.show()
# # df1.show()
# # datalookup.show()
