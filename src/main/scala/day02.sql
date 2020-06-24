set hive.exec.mode.local.auto=true;
select current_database();
show databases;

/*
TODO
    < dw主题层 >
 */
/*
 sss
    目标客户主题
 */
use ods_release1903;
select * from ods_release1903.ods_01_release_session limit 10;
-- 先查询
/*
{"model_version":"V4","model_code":"ML4","idcard":"110105199401212966",
"latitude":"40.06662","area_code":"110114","matter_id":"M3",
"aid":"0802","longitude":"116.37839"}
*/
show databases;
create database dw_release1903;
/*
 sss 目标客户主题
 */
create external table if not exists dw_release1903.dw_release_customer
(
    release_session string comment '投放会话id',
    release_status  string comment '参考下面投放流程状态说明',
    device_num      string comment '设备唯一编码',
    device_type     string comment '1 android| 2 ios | 9 其他',
    sources         string comment '渠道',
    channels        string comment '通道',
    idcard          string comment '身份证',
    age             int comment '年龄',
    gender          string comment '性别',
    area_code       string comment '地区',
    longitude       string comment '经度',
    latitude        string comment '纬度',
    matter_id       string comment '物料代码',
    model_code      string comment '模型代码',
    model_version   string comment '模型版本',
    aid             string comment '广告位id',
    ct              bigint comment '创建时间'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/release1903/dw/release_customer/';
drop table dw_release1903.dw_release_customer;


use dw_release1903;
show tables;
insert into table dw_release1903.dw_release_customer partition (bdp_day='20200622')
select release_session,
       release_status,
       device_num,
       device_type,
       sources,
       channels,
       get_json_object(exts, '$.idcard') as idcard,
       cast(year(current_date()) as int) - cast(regexp_extract(get_json_object(exts, '$.idcard'), '(\\d{6})(\\d{4})', 2) as int) age,
       cast(regexp_extract(get_json_object(exts, '$.idcard'), '(\\d{16})(\\d{1})', 2) as int) % 2 as gender,
       get_json_object(exts, '$.area_code') as area_code,
       get_json_object(exts, '$.longitude') as longitude,
       get_json_object(exts, '$.latitude') as latitude,
       get_json_object(exts, '$.matter_id') as matter_id,
       get_json_object(exts, '$.model_code') as model_code,
       get_json_object(exts, '$.model_version') as model_version,
       get_json_object(exts, '$.aid') as aid,
       ct
from ods_release1903.ods_01_release_session;

select * from dw_release1903.dw_release_customer limit 10;

-- sss from_unixtime(unix_timestamp(), 'yyyy') - regexp_extract(get_json_object(exts, '$.idcard'), '(\\d{6})(\\d{4})', 2) as age
/*
 sss
    get_json_object(exts, '$.idcard') 获取json字符串中值
    substr和substring都是一样的效果截取字符串
        substr(str, startIndex) 下边从1开始,包头,一致到尾部
        substring(str, startIndex, length)
        substr(get_json_object(exts, '$.idcard'), 7, 4)
    regex_extract正则表达式解析函数
        regex_extract('foothebar', 'foo(.*?)(bar)',  1)    =>  the
        regex_extract('foothebar', 'foo(.*?)(bar)',  2)    =>  bar
        regex_extract('foothebar', 'foo(.*?)(bar)',  0)    =>  foothebar
 */

/*
 sss
    将时间戳  =>  日期
    from_unixtime(bigint time,   format 'yyyyMMdd')
    注意: time指的是1970年1月1日 00:00:00 开始到某个时刻的  秒数  而不是 毫秒数
 */
select from_unixtime(cast(1592901765575/1000 as bigint), 'yyyy-MM');

/*
 sss
    获取当前unix时间戳,返回值是bigint, 也是 秒数级别, 切记不是毫秒级别
    unix_timestamp() 不加任何参数
    1592902248   10位
    1592901765575    13位
 */
select unix_timestamp();

/*
 sss
    日期  =>  时间戳
    unix_timestamp(string date)
    转化格式为‘yyyy-MM-dd HH:mm:ss’的日期到unix时间戳,如果转化失败,则返回0
 */
select unix_timestamp('2020-06-23'); --返回为null
select unix_timestamp('2020-06-23 16:59:00'); --1592902740


/*
 sss
    指定格式日期  =>  时间戳
    unix_timestamp(string date, string pattern)
    将指定pattern格式的日期转化成时间戳
 */
select unix_timestamp("2020-06/23 00:00:01", "yyyy-MM/dd HH:mm:ss");--1592841601
                                                                    --1592841600

/*
 sss
    日期时间  =>  年月日部分
    to_date()
 */
select to_date('2020-09-09 10:10:2');
select year('2020-09-07 7:7:7');

-- 332923
select count(1) from dw_release1903.dw_release_customer;

create table if not exists tab1(
    area string,
    city string,
    day string,
    counts int
)row format delimited fields terminated by ','
stored as textfile
location '/data/tab1';

load data inpath '/data/rr.txt' overwrite into table tab1;
select * from tab1;

select
    area,
    city,
    day,
    sum(counts)
from tab1 group by area,city,day with cube;

set hive.exec.mode.local.auto=true;