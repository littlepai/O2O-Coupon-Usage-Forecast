-- 2.coupon related: 
--       discount_rate. discount_man. discount_jian. is_man_jian
--       day_of_week,day_of_month. (date_received)
--             label窗里的coupon，在特征窗中被消费过的数目  label_coupon_feature_buy_count
--             label窗里的coupon，在特征窗中被领取过的数目  label_coupon_feature_rec
--             label窗里的coupon，在特征窗中的核销率  label_coupon_feature_rate = label_coupon_feature_buy_count/label_coupon_feature_rec


-- ###################   for dataset3  ################### 
drop table wepon_d3_f2_t1;
create table wepon_d3_f2_t1 as
select t.user_id,t.coupon_id,t.merchant_id,t.date_received,t.days_distance,t.day_of_week,t.day_of_month,t.is_man_jian,t.discount_man,t.discount_jian,t.distance,
	  case when is_man_jian=1 then 1.0 - discount_jian/discount_man else discount_rate end as discount_rate
from
(
  select user_id,coupon_id,merchant_id,date_received,to_number(discount_rate) as discount_rate,
          case when distance='null' then -1 else cast(distance as number) end as distance,
		  to_date(date_received,'yyyymmdd')-to_date('20160630','yyyymmdd') as days_distance,
		  to_char(to_date(date_received,'yyyymmdd'), 'w') as day_of_week,
		  cast(substr(date_received,7,2) as number) as day_of_month,
		  case when instr(discount_rate,':')=0 then 0 else 1 end as is_man_jian,
		  case when instr(discount_rate,':')=0 then -1 else to_number(substr(discount_rate, 0, instr(discount_rate, ':')-1)) end as discount_man,
		  case when instr(discount_rate,':')=0 then -1 else to_number(substr(discount_rate, instr(discount_rate, ':')+1)) end as discount_jian
  from wepon_dataset3
)t;

drop table wepon_d3_f2_t2;
create table wepon_d3_f2_t2 as
select coupon_id,sum(cnt) as coupon_count
from (select coupon_id,1 as cnt from wepon_dataset3)t 
group by coupon_id;


drop table wepon_d3_f2_t3;
create table wepon_d3_f2_t3 as
select coupon_id,sum(cnt) as label_coupon_feature_rec from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset3)a  left outer join (select coupon_id,1 as cnt from wepon_feature3 )b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;

drop table wepon_d3_f2_t4;
create table wepon_d3_f2_t4 as
select coupon_id,sum(cnt) as label_coupon_feature_buy_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset3)a  left outer join (select coupon_id,1 as cnt from wepon_feature3 where coupon_id!='null' and date_pay!='null')b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;


drop table wepon_d3_f2;
create table wepon_d3_f2 as 
select e.*,f.label_coupon_feature_buy_count,
		  case when e.label_coupon_feature_rec=0 then -1 else f.label_coupon_feature_buy_count/e.label_coupon_feature_rec end as label_coupon_feature_rate
from
(
  select c.*,d.label_coupon_feature_rec from
  (
	select a.user_id,a.coupon_id,a.merchant_id,a.date_received,a.days_distance,a.day_of_week,a.day_of_month,a.is_man_jian,a.distance,
		  cast(a.discount_man as number) as discount_man,cast(a.discount_jian as number ) as discount_jian,cast(a.discount_rate as number) as discount_rate,b.coupon_count 
	from wepon_d3_f2_t1 a join wepon_d3_f2_t2 b 
	on a.coupon_id=b.coupon_id
  )c left outer join wepon_d3_f2_t3 d 
  on c.coupon_id=d.coupon_id
)e left outer join wepon_d3_f2_t4 f 
on e.coupon_id=f.coupon_id;

-- ###################   for dataset2  ################### 
drop table wepon_d2_f2_t1;
create table wepon_d2_f2_t1 as
select t.user_id,t.coupon_id,t.merchant_id,t.date_received,t.date_pay,t.days_distance,t.day_of_week,t.day_of_month,t.is_man_jian,t.discount_man,t.discount_jian,t.distance,
	  case when is_man_jian=1 then 1.0 - discount_jian/discount_man else discount_rate end as discount_rate
from
(
  select user_id,coupon_id,merchant_id,date_received,date_pay,to_number(discount_rate) as discount_rate,
  	      case when distance='null' then -1 else cast(distance as number) end as distance,
		  to_date(date_received,'yyyymmdd')-to_date('20160630','yyyymmdd') as days_distance,
		  to_char(to_date(date_received,'yyyymmdd'), 'w') as day_of_week,
		  cast(substr(date_received,7,2) as number) as day_of_month,
		  case when instr(discount_rate,':')=0 then 0 else 1 end as is_man_jian,
		  case when instr(discount_rate,':')=0 then -1 else to_number(substr(discount_rate, 0, instr(discount_rate, ':')-1)) end as discount_man,
		  case when instr(discount_rate,':')=0 then -1 else to_number(substr(discount_rate, instr(discount_rate, ':')+1)) end as discount_jian
  from wepon_dataset2
)t;

drop table wepon_d2_f2_t2;
create table wepon_d2_f2_t2 as
select coupon_id,sum(cnt) as coupon_count
from (select coupon_id,1 as cnt from wepon_dataset2)t 
group by coupon_id;


drop table wepon_d2_f2_t3;
create table wepon_d2_f2_t3 as
select coupon_id,sum(cnt) as label_coupon_feature_rec from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset2)a  left outer join (select coupon_id,1 as cnt from wepon_feature2 )b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;

drop table wepon_d2_f2_t4;
create table wepon_d2_f2_t4 as
select coupon_id,sum(cnt) as label_coupon_feature_buy_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset2)a  left outer join (select coupon_id,1 as cnt from wepon_feature2 where coupon_id!='null' and date_pay!='null')b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;


drop table wepon_d2_f2;
create table wepon_d2_f2 as 
select e.*,f.label_coupon_feature_buy_count,
		  case when e.label_coupon_feature_rec=0 then -1 else f.label_coupon_feature_buy_count/e.label_coupon_feature_rec end as label_coupon_feature_rate
from
(
  select c.*,d.label_coupon_feature_rec from
  (
	select a.user_id,a.coupon_id,a.merchant_id,a.date_received,a.date_pay,a.days_distance,a.day_of_week,a.day_of_month,a.is_man_jian,a.distance,
		  cast(a.discount_man as number) as discount_man,cast(a.discount_jian as number ) as discount_jian,cast(a.discount_rate as number) as discount_rate,b.coupon_count 
	from wepon_d2_f2_t1 a join wepon_d2_f2_t2 b 
	on a.coupon_id=b.coupon_id
  )c left outer join wepon_d2_f2_t3 d 
  on c.coupon_id=d.coupon_id
)e left outer join wepon_d2_f2_t4 f 
on e.coupon_id=f.coupon_id;


-- ###################   for dataset1  ################### 
drop table wepon_d1_f2_t1;
create table wepon_d1_f2_t1 as
select t.user_id,t.coupon_id,t.merchant_id,t.date_received,t.date_pay,t.days_distance,t.day_of_week,t.day_of_month,t.is_man_jian,t.discount_man,t.discount_jian,t.distance,
	  case when is_man_jian=1 then 1.0 - discount_jian/discount_man else discount_rate end as discount_rate
from
(
  select user_id,coupon_id,merchant_id,date_received,date_pay,to_number(discount_rate) as discount_rate,
          case when distance='null' then -1 else cast(distance as number) end as distance,
		  to_date(date_received,'yyyymmdd')-to_date('20160630','yyyymmdd') as days_distance,
		  to_char(to_date(date_received,'yyyymmdd'), 'w') as day_of_week,
		  cast(substr(date_received,7,2) as number) as day_of_month,
		  case when instr(discount_rate,':')=0 then 0 else 1 end as is_man_jian,
		  case when instr(discount_rate,':')=0 then -1 else to_number(substr(discount_rate, 0, instr(discount_rate, ':')-1)) end as discount_man,
		  case when instr(discount_rate,':')=0 then -1 else to_number(substr(discount_rate, instr(discount_rate, ':')+1)) end as discount_jian
  from wepon_dataset1
)t;

drop table wepon_d1_f2_t2;
create table wepon_d1_f2_t2 as
select coupon_id,sum(cnt) as coupon_count
from (select coupon_id,1 as cnt from wepon_dataset1)t 
group by coupon_id;


drop table wepon_d1_f2_t3;
create table wepon_d1_f2_t3 as
select coupon_id,sum(cnt) as label_coupon_feature_rec from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset1)a  left outer join (select coupon_id,1 as cnt from wepon_feature1 )b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;

drop table wepon_d1_f2_t4;
create table wepon_d1_f2_t4 as
select coupon_id,sum(cnt) as label_coupon_feature_buy_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset1)a  left outer join (select coupon_id,1 as cnt from wepon_feature1 where coupon_id!='null' and date_pay!='null')b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;


drop table wepon_d1_f2;
create table wepon_d1_f2 as 
select e.*,f.label_coupon_feature_buy_count,
		  case when e.label_coupon_feature_rec=0 then -1 else f.label_coupon_feature_buy_count/e.label_coupon_feature_rec end as label_coupon_feature_rate
from
(
  select c.*,d.label_coupon_feature_rec from
  (
	select a.user_id,a.coupon_id,a.merchant_id,a.date_received,a.date_pay,a.days_distance,a.day_of_week,a.day_of_month,a.is_man_jian,a.distance,
		  cast(a.discount_man as number) as discount_man,cast(a.discount_jian as number ) as discount_jian,cast(a.discount_rate as number) as discount_rate,b.coupon_count 
	from wepon_d1_f2_t1 a join wepon_d1_f2_t2 b 
	on a.coupon_id=b.coupon_id
  )c left outer join wepon_d1_f2_t3 d 
  on c.coupon_id=d.coupon_id
)e left outer join wepon_d1_f2_t4 f 
on e.coupon_id=f.coupon_id;


