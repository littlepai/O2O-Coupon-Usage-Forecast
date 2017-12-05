--DROP wepon_dataset3
DROP TABLE wepon_dataset3;
--CREATE AND INSERT wepon_dataset3
create table wepon_dataset3 as select * from prediction_stage2;
-----------------------------------------------------------------------
--DROP wepon_feature3
DROP TABLE wepon_feature3;
--CREATE AND INSERT wepon_feature3
CREATE TABLE wepon_feature3 AS select * from train_offline_stage2
where ('20160315' <= date_pay and date_pay <= '20160630') or (date_pay = 'null' and '20160315' <= date_received and date_received <= '20160630');
-----------------------------------------------------------------------
--DROP wepon_online_feature3
DROP TABLE wepon_online_feature3;
--CREATE AND INSERT wepon_online_feature3
CREATE TABLE wepon_online_feature3 AS select * from train_online_stage2
where ('20160315'<=date_pay and date_pay<='20160630') or (date_pay='null' and '20160315'<=date_received and date_received<='20160630');
-----------------------------------------------------------------------
--DROP wepon_dataset2
DROP TABLE wepon_dataset2;
--CREATE AND INSERT wepon_dataset2
create table wepon_dataset2 as select * from train_offline_stage2 where '20160515'<=date_received and date_received<='20160615';
-----------------------------------------------------------------------
--DROP wepon_feature2
DROP TABLE wepon_feature2;
--CREATE AND INSERT wepon_feature2
create table wepon_feature2 as select * from train_offline_stage2
where ('20160201'<=date_pay and date_pay<='20160514') or (date_pay='null' and '20160201'<=date_received and date_received<='20160514');
-----------------------------------------------------------------------
--DROP wepon_online_feature2
DROP TABLE wepon_online_feature2;
--CRETAE AND INSERT wepon_online_feature2
create table wepon_online_feature2 as select * from train_online_stage2
where ('20160201'<=date_pay and date_pay<='20160514') or (date_pay='null' and '20160201'<=date_received and date_received<='20160514');
-----------------------------------------------------------------------
--DROP wepon_dataset1
DROP TABLE wepon_dataset1;
--CREATE AND INSERT wepon_dataset1
create table wepon_dataset1 as select * from train_offline_stage2 where '20160414'<=date_received and date_received<='20160514';
-----------------------------------------------------------------------
--DROP wepon_feature1
DROP TABLE wepon_feature1;
--CREATE AND INSERT wepon_feature1
create table wepon_feature1 as select * from train_offline_stage2
where ('20160101'<=date_pay and date_pay<='20160413') or (date_pay='null' and '20160101'<=date_received and date_received<='20160413');
-----------------------------------------------------------------------
--DROP wepon_online_feature1
DROP TABLE wepon_online_feature1;
--CREATE AND INSERT wepon_online_feature1
create table wepon_online_feature1 as select * from train_online_stage2
where ('20160101'<=date_pay and date_pay<='20160413') or (date_pay='null' and '20160101'<=date_received and date_received<='20160413');
