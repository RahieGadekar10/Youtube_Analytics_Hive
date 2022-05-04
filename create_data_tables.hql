use project;
set hive.exec.dynamic.partition.mode=nonstrict;

create external table if not exists data_table(
title string , 
trending_date string)
row format delimited
fields terminated by',' 
stored as orc;

create table if not exists viewers
(channel_title string, 
views string , 
ranking string)
partitioned by (published_year string)
row format delimited 
fields terminated by','
stored as textfile
LOCATION 'hdfs://localhost:9000/user/hive/viewers/';

create table if not exists videos_yearly(
Number_of_Videos string)
partitioned by (published_year string)
row format delimited 
fields terminated by','
stored as textfile
LOCATION 'hdfs://localhost:9000/user/hive/yearly_videos/';

create table if not exists most_viewed(
title string , 
views string , 
ranking string )
partitioned by (published_year string)
row format delimited
fields terminated by','
stored as textfile
LOCATION 'hdfs://localhost:9000/user/hive/most_viewed/';

create table if not exists yearly_growth(
year_count string, 
previous_year_count string,
growth string)
partitioned by(published_year string)
row format delimited 
fields terminated by','
stored as textfile
LOCATION 'hdfs://localhost:9000/user/hive/year_growth/';
