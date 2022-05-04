use project ; 
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table analytics partition(published_year)
select 
channel_title,
title , 
publish_date,
d.trending_date , 
DATEDIFF(d.trending_date,publish_date) as days_trending,
IF(views is NULL, 0 , views) as views , 
IF(likes is NULL, 0 , likes) as likes, 
IF(dislikes is NULL, 0 , dislikes) as dislikes, 
IF(comment_count is NULL, 0 , comment_count) as comment_count,
c.category_name,
published_year
from bigdata as d 
left join category_table c on c.category_id=d.category_id 
where exists(select title, trending_date from data_table as v where d.trending_date = v.trending_date and d.title=v.title) 
order by publish_date
