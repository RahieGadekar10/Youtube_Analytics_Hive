from pyspark.sql import SparkSession 

spark = SparkSession.builder.master("local").appName("demo").enableHiveSupport().getOrCreate()
spark.sql("use project")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

df = spark.sql("select sum(title) as Number_of_Videos, published_year from(select count(title) as title,published_year from analytics group by published_year) a group by published_year order by published_year")
df.write.mode("overwrite").insertInto("videos_yearly")

df2 = spark.sql("select * from(select channel_title ,views , ROW_NUMBER() OVER(PARTITION BY published_year ORDER BY views desc) as ranking ,published_year from analytics) a where a.ranking<=3 ")
df2.write.mode("overwrite").insertInto("viewers")

df3 = spark.sql("select * from(select title ,INT(views), ROW_NUMBER() over(partition by published_year order by views desc) as ranking, published_year from analytics) a where a.ranking<=3")
df3.write.mode("overwrite").insertInto("most_viewed")

df4 = spark.sql("select Year_Count ,Previous_Year_Count ,round(((Year_Count - Previous_Year_Count)/(cast(Previous_Year_Count AS numeric)))*100) as year_growth,published_year from (select Year_Count ,LAG(Year_Count , 1) OVER (ORDER BY published_year) as Previous_Year_Count, published_year from (select sum(Number_of_Channels) as Year_Count, published_year from (select count(channel_title) as Number_of_Channels, published_year from analytics group by published_year)a group by published_year order by published_year)b)c")
df4.write.mode("overwrite").insertInto("yearly_growth")

