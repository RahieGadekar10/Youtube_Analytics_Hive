from kafka import KafkaProducer
import time
from pyspark.sql import SparkSession

class Kafka_Producer :
    def __init__(self):
            pass

    def Produce(self):
        kafka_producer = KafkaProducer(bootstrap_servers = 'localhost:9092',value_serializer = lambda x : x.encode("utf-8"))
        nrow = 0
        spark = SparkSession.builder.master("local").appName("Hive_Project").config("spark.jars.package","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"). config("spark.ui.port", "4041").getOrCreate()
        df = spark.read.csv('USvideos.csv' , header=True , inferSchema= True)
        for row in df.rdd.toLocalIterator() :
            row_send = ','.join(map(str , list(row)))
            print(row_send)
            kafka_producer.send("hive_topic",row_send)
            nrow+=1
            time.sleep(1)
        return nrow

def main() :
    producer = Kafka_Producer()
    producer.Produce()

if __name__ == "__main__" :
    main()


