#!/usr/bin/env bash
show databases;
set hive.exec.mode.local.auto=true;
-- 创建ods_release1903数据库
drop database ods_release1903;
create database ods_release1903;
select current_database();
show tables;
use ods_release1903;

/**
  TODO < ods层数据处理 >
 */
/**
  sss
    日志数据log
    广告投放原始数据
 */
-- 1.创建广告投放原始数据表
create external table if not exists ods_release1903.ods_01_release_session
(
    sid             string comment '投放请求id',
    release_session string comment '投放会话id',
    release_status  string comment '参考下面投放流程状态说明',
    device_num      string comment '设备唯一编码',
    device_type     string comment '1 android| 2 ios | 9 其他',
    sources         string comment '渠道',
    channels        string comment '通道',
    exts            string comment '扩展信息(参见下面扩展信息描述)',
    ct              bigint comment '创建时间'
) partitioned by (bdp_day string)
    stored as parquet
    location '/data/release1903/ods/release_session/';

-- 2.导入日志数据到hive表中
-- (1)load data local inpath 本地文件 copy 一份到hdfs
-- (2)load data inpath hdfs文件 move 到hdfs对应hive表中的目录下
-- hdfs dfs -mkdir ttt
-- hdfs dfs -put  /opt/file/15603638400003h4gka4h /ttt
load data inpath '/ttt' overwrite into table ods_release1903.ods_01_release_session
    partition (bdp_day="20200622");

-- 3.检查
select count(*) from ods_release1903.ods_01_release_session;
select * from ods_release1903.ods_01_release_session limit 10;

/*
sss
        hive默认创建的都是内部表managed_table;
        如果在建表的时候写上了external,则创建的是外部表
        内部表和外部表之间的区别:
            体现在删除表的时候,
            如果是内部表删除,mysql元数据和hdfs中的实际存储数据都会一并删除
            如果是外部表删除,mysql元数据会删除,但是hdfs中的实际存储数据并不会删除
 */
-- create table tttt(
--     id string
-- ) location '/data/tttt';
-- insert into table tttt values(1);
-- drop table tttt;

/**
  sss
    mysql业务数据使用sqoop导入到hive表中
 */
-- sss
--  1.创建注册用户表
create external table if not exists ods_release1903.ods_02_release_user
(
    user_id   string COMMENT '用户id：手机号',
    user_pass string COMMENT '用户密码',
    ctime     string COMMENT '创建时间'
) row format delimited fields terminated by ','
    stored as textfile
    location '/data/release1903/ods/release_user';

-- 2.使用sqoop导入到hive表中
/*
sqoop import \
--connect jdbc:mysql://10.36.142.3:3306/sz1903_dmp_datawarehouse \
--username root \
--password root \
--table users \
--hive-import \
--hive-overwrite \
--hive-table ods_release1903.ods_02_release_user \
-m 1
*/
-- sss 修改后的sqoop导入命令
/*
sqoop import \
--connect jdbc:mysql://10.36.142.3:3306/sz1903_dmp_datawarehouse \
--username root \
--password root \
--table users \
--hive-import \
--hive-overwrite \
--hive-table ods_release1903.ods_02_release_user \
--fields-terminated-by ',' \
-m 1
 */

-- 3.验证
select * from ods_release1903.ods_02_release_user limit 10;
-- sss 结果是全部数据都放在了user_id该列下面,而user_pass和ctime没有数据为null
--     原因是hive表默认的列分隔符是'\001',但是你改成了',',所以sqoop导入的时候,都放在一列
--     解决方法是 在sqoop导入的命令中加上 --fields-terminated-by ',' 告诉sqoop一声
drop table ods_release1903.ods_02_release_user;
show tables;

-- 4.改用dataxd来实现mysql导入数据到hive表中
-- sss  当然现在使用的都是全量导入,因为业务数据不会经常变化,可能一天也就更新少数个.
--      全量导入和增量导入之间的区别 ?
--      全量导入适用于数据不经常变化的表, 例如业务数据;
--      增量导入适用于数据经常变化的表, 例如用户日志数据,每时每刻都会发生很多变化
--  datax.json文件在隔壁, 在mobaxterm命令行中使用 /opt/app/datax/bin/datax.py datax.json来执行
set hive.exec.mode.local.auto=true;
select count(1) from ods_release1903.ods_02_release_user;


-- sss
--  1.创建物料信息表
create table if not exists ods_release1903.ods_02_release_materiel
(
    matter_id            string COMMENT '物料编码',
    matter_type          string COMMENT '物料类型：1 图文 2 视频',
    matter_model         string COMMENT '物料对应模型',
    matter_model_version string COMMENT '物料对应模型的版本',
    ctime                string COMMENT '创建时间'
) row format delimited fields terminated by ','
    stored as textfile
    location '/data/release1903/ods/release_materiel';

-- 2.使用sqoop导入到hive表中
/*
sqoop import \
--connect jdbc:mysql://10.36.142.3:3306/sz1903_dmp_datawarehouse \
--username root \
--password root \
--table materiel \
--hive-import \
--hive-overwrite \
--hive-table ods_release1903.ods_02_release_materiel \
--fields-terminated-by ',' \
-m 1
 */

-- 3.测试
select count(1) from ods_release1903.ods_02_release_materiel;

select * from ods_release1903.ods_02_release_materiel limit 10;


-- sss
--  1.创建渠道通道映射表
create table if not exists ods_release1903.ods_02_release_sources_mapping_channels
(
    sources           string COMMENT '渠道编码',
    sources_remark    string COMMENT '渠道描述',
    channels          string COMMENT '通道编码',
    channels_remark   string COMMENT '通道描述',
    media_type        string COMMENT '媒体类型',
    media_type_remark string COMMENT '媒体类型描述',
    ctime             string COMMENT '创建时间'
) row format delimited fields terminated by ','
    stored as textfile
    location '/data/release1903/ods/release_sources_mapping_channels';

-- 2.使用sqoop导入到hive表中
/*
sqoop import \
--connect jdbc:mysql://10.36.142.3:3306/sz1903_dmp_datawarehouse \
--username root \
--password root \
--table sources_mapping_channels \
--hive-import \
--hive-overwrite \
--hive-table ods_release1903.ods_02_release_sources_mapping_channels \
--fields-terminated-by ',' \
-m 1
 */

-- 3.测试
set hive.exec.mode.local.auto=true;
select count(*) from ods_release1903.ods_02_release_sources_mapping_channels;
select * from ods_release1903.ods_02_release_sources_mapping_channels limit 10;


show databases;
/**
  TODO

 */