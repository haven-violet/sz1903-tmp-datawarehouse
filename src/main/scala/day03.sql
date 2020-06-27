set hive.exec.mode.local.auto=true;

show databases;
/**
  TODO DM集市层库表
 */
create database dm_release1903;
use dm_release1903;
select current_database();

/*
 sss 目标客户集市库表
 */
//sss 1.渠道用户统计
create external table if not exists dm_release1903.dm_customer_sources
(
    sources     string comment '渠道',
    channels    string comment '通道',
    device_type string comment '1 android| 2 ios | 9 其他',
    user_count  bigint comment '目标客户数量',
    total_count bigint comment '目标客户投放总量'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/release1903/dm/release_customer/dm_customer_sources/';

insert overwrite table dm_release1903.dm_customer_sources partition (bdp_day = "20200622")
select sources,
       channels,
       device_type,
       count(distinct device_num) as user_count,
       count(release_session) as total_count
from dw_release1903.dw_release_customer
group by sources,channels,device_type;

select * from dm_release1903.dm_customer_sources limit 3;


//sss 2.目标客户多维统计
create external table if not exists dm_release1903.dm_customer_cube
(
    sources     string comment '渠道',
    channels    string comment '通道',
    device_type string comment '1 android| 2 ios | 9 其他',
    age_range   string comment '1:18岁以下|2:18-25岁|3 26-35岁|4 36-45岁|5 45岁以上',
    gender      string comment '性别',
    area_code   string comment '地区',
    user_count  bigint comment '目标客户数量',
    total_count bigint comment '目标客户投放总量'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/release1903/dm/release_customer/dm_customer_cube/';
insert into table dm_release1903.dm_customer_cube partition (bdp_day = "20200622")
select
    sources,
    channels,
    device_type,
    gender,
    area_code,
    (
        case
            when age < 18 then '1'
            when age between 18 and 25 then '2'
            when age between 26 and 35 then '3'
            when age between 36 and 45 then '4'
            else '5'
        end
    ) as age_range,
    count(distinct device_num) as user_count,
    count(release_session) as total_count
from dw_release1903.dw_release_customer
group by sources, channels, device_type, gender, area_code,
    (
        case
            when age < 18 then '1'
            when age between 18 and 25 then '2'
            when age between 26 and 35 then '3'
            when age between 36 and 45 then '4'
            else '5'
        end
    ) with rollup;

select
    *
from dm_release1903.dm_customer_cube limit 2;

set hive.exec.mode.local.auto=true;


