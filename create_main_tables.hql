use project ; 
set hive.exec.dynamic.partition.mode=nonstrict;

create table if not exists category_table(
category_id string , 
category_name string)
row format delimited
fields terminated by',';

create table if not exists videos(
video_id string, 
trending_date string, 
title string, 
channel_title string,
category_id string,
publish_date string,
views string,
likes string,
dislikes string,
comment_count string,
comments_disabled string,
ratings_disabled string,
video_error_or_removed string,
published_year string)
row format delimited fields terminated by',' 
stored as orc;

create external table if not exists bigdata(video_id string, 
trending_date string, 
title string, 
channel_title string,
category_id string,
publish_date string,
views string,
likes string,
dislikes string,
comment_count string,
comments_disabled string,
ratings_disabled string,
video_error_or_removed string)
partitioned by (published_year string)
row format delimited fields terminated by',' 
stored as orc;

create external table if not exists analytics(
channel_title string,
title string,
publish_date string,
trending_date string,
days_trending string,
views string,
likes string,
dislikes string,
comment_count string,
category_name string)
partitioned by (published_year string)
row format delimited 
fields terminated by','
stored as parquet;
