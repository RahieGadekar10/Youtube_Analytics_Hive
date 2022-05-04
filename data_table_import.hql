use project; 
set hive.exec.dynamic.partition.mode = nonstrict;

insert overwrite table data_table(
select title , 
max(trending_date) as trending_date from bigdata
group by title);
