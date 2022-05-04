use project;
set hive.exec.dynamic.partition.mode=nonstrict;

insert into bigdata partition(published_year) select v.video_id,
v.trending_date,
v.title,
v.channel_title,
v.category_id,
v.publish_date,
v.views,
v.likes,
v.dislikes,
v.comment_count,
v.comments_disabled,
v.ratings_disabled,
v.video_error_or_removed,
v.published_year from videos as v where not exists(
select b.trending_date,b.title 
from bigdata as b 
where v.trending_date=b.trending_date and v.title=b.title);

