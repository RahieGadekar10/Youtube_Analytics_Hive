from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions
import sqlalchemy
import pandas as pd
class Consumer:
    def __init__(self):
        self.spark =SparkSession.builder.master("local").appName("Hive_Project").config("spark.jars.package","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1").config("spark.ui.port","4041").getOrCreate()
        self.cat = self.spark.read.csv("US_category_id.csv" , header = True , inferSchema=True)
    def process_each_record(self, df, epoch_id):

        df = df.select("channel_title","title","trending_date","publish_date","days_trending","category_name","views","likes","dislikes","comment_count", "published_year","timestamp")
        df = df.toPandas()
        self.insert_to_sql(df=df)

    def insert_to_sql(self, df):
        df.to_csv("new1.csv")
        engine = sqlalchemy.create_engine("mysql+pymysql://rahie:rahie@localhost:3306/bigdata")
        df.to_sql(con=engine, name='hive_project', if_exists='append', index=False)

    def consume(self):
        schema= "video_id string, trending_date string ,title string ,channel_title string, category_id int,publish_time string,views string,likes string,dislikes string,comment_count string,comments_disabled string,ratings_disabled string,video_error_or_removed string"
        dataframe = self.spark.readStream.format("kafka").option("kafka.bootstrap.servers",'localhost:9092').option("subscribe","hive_topic").option("startingOffsets","latest").load()
        dataframe1 = dataframe.selectExpr("CAST(value as STRING)","timestamp")
        dataframe2 = dataframe1.select(from_csv(functions.col("value"), schema).alias("records"),"timestamp")
        dataframe3 = dataframe2.select("records.*","timestamp")
        dataframe3.views = dataframe3.select(split(col("views"),",")[0])
        df1 = dataframe3.withColumn("title" , regexp_replace("title" , ",",""))
        df1 = df1.withColumn("publish_time" , date_format("publish_time" , "yyyy-MM-dd")).withColumn("publish_time" ,to_date("publish_time" , "yyyy-MM-dd")).withColumnRenamed("publish_time" , "publish_date")
        df1 = df1.withColumn("trending_date" , to_date("trending_date" , 'yy.dd.MM'))
        df1=df1.withColumn("published_year" , year(df1.publish_date))
        df1 = df1.withColumn("days_trending" , datediff("trending_date","publish_date"))
        df1 = df1.join(self.cat,['category_id'],how='left')
        df1=df1.select("*")
        transformed_df = df1
        query = transformed_df.writeStream.format("console").trigger(processingTime="5 seconds").foreachBatch(self.process_each_record).start()
        #query = transformed_df.writeStream.format("console").trigger(processingTime="5 seconds").start()
        query.awaitTermination()

def main() :
    kafkaconsumer = Consumer()
    kafkaconsumer.consume()

if __name__ == "__main__" :
    main()


