from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import col , to_date , date_format
from operator import add
import os
import subprocess

data = os.listdir("/home/bigdata/spark_project/files/")
for file in data : 
     spark = SparkSession.builder.master("local").appName("Project").enableHiveSupport().getOrCreate()
     df1 = spark.read.csv("file:///home/bigdata/spark_project/files/"+file , header = True)
     df1.views = df1.select(split(col("views"),",")[0])
     df1 = df1.withColumn("title" , regexp_replace("title" , ",",""))
     df1 = df1.withColumn("publish_time" , date_format("publish_time" , "yyyy-MM-dd")).withColumn("publish_time" ,to_date("publish_time" , "yyyy-MM-dd")).withColumnRenamed("publish_time" , "publish_date")
     df1 = df1.withColumn("trending_date" , to_date("trending_date" , 'yy.dd.MM'))
     df1=df1.withColumn("published_year" , year(df1.publish_date))
     df1.show(10)
     df1.write.mode("append").insertInto("project.videos")
    # output = subprocess.Popen(["hadoop", "fs", "-put",file, "/data/"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
