-- 4.user_merchant: 
--       user_merchant_buy_total.  user_merchant_received    user_merchant_buy_use_coupon  user_merchant_any  user_merchant_buy_common
--       coupon_transform_rate = user_merchant_buy_use_coupon/user_merchant_received
--       user_merchant_coupon_buy_rate = user_merchant_buy_use_coupon/user_merchant_buy_total
--       user_merchant_common_buy_rate = user_merchant_buy_common/user_merchant_buy_total
--       user_merchant_rate = user_merchant_buy_total/user_merchant_any


-- ###################   for dataset3  ################### 
drop table wepon_d3_f4_t1;
create table wepon_d3_f4_t1 as 
select user_id,merchant_id,count(*) as user_merchant_buy_total from
( select user_id,merchant_id from wepon_feature3 where date_pay!='null' )t
group by user_id,merchant_id;

drop table wepon_d3_f4_t2;
create table wepon_d3_f4_t2 as 
select user_id,merchant_id,count(*) as user_merchant_received from
( select user_id,merchant_id from wepon_feature3 where coupon_id!='null' )t
group by user_id,merchant_id;

drop table wepon_d3_f4_t3;
create table wepon_d3_f4_t3 as 
select user_id,merchant_id,count(*) as user_merchant_buy_use_coupon from
( select user_id,merchant_id from wepon_feature3 where date_pay!='null' and date_received!='null')t
group by user_id,merchant_id;

drop table wepon_d3_f4_t4;
create table wepon_d3_f4_t4 as 
select user_id,merchant_id,count(*) as user_merchant_any from
( select user_id,merchant_id from wepon_feature3 )t
group by user_id,merchant_id;


drop table wepon_d3_f4_t5;
create table wepon_d3_f4_t5 as 
select user_id,merchant_id,count(*) as user_merchant_buy_common from
( select user_id,merchant_id from wepon_feature3 where date_pay!='null' and coupon_id='null' )t
group by user_id,merchant_id;


drop table wepon_d3_f4;
create table wepon_d3_f4 as 
select user_id,merchant_id,user_merchant_buy_use_coupon,user_merchant_buy_common,
       coupon_transform_rate,user_merchant_coupon_buy_rate,user_merchant_common_buy_rate,user_merchant_rate,
	   case when user_merchant_buy_total is null then 0.0 else user_merchant_buy_total end as user_merchant_buy_total,
	   case when user_merchant_received is null then 0.0 else user_merchant_received end as user_merchant_received,
	   case when user_merchant_any is null then 0.0 else user_merchant_any end as user_merchant_any
from
(
  select tt.*,tt.user_merchant_buy_use_coupon/tt.user_merchant_received as coupon_transform_rate,
			  tt.user_merchant_buy_use_coupon/tt.user_merchant_buy_total as user_merchant_coupon_buy_rate,
			  tt.user_merchant_buy_common/tt.user_merchant_buy_total as user_merchant_common_buy_rate,
			  tt.user_merchant_buy_total/tt.user_merchant_any as user_merchant_rate
  from
  (
	  select user_id,merchant_id,user_merchant_buy_total,user_merchant_received,user_merchant_any,
			 case when user_merchant_buy_use_coupon is null then 0.0 else user_merchant_buy_use_coupon end as user_merchant_buy_use_coupon,
			 case when user_merchant_buy_common is null then 0.0 else user_merchant_buy_common end as user_merchant_buy_common
	  from
	  (
		  select i.*,j.user_merchant_buy_common from
		  (
			select g.*,h.user_merchant_any from
			(
			  select e.*,f.user_merchant_buy_use_coupon from
			  (
				select c.*,d.user_merchant_received from
				(
				  select a.*,b.user_merchant_buy_total from
				  (select distinct user_id,merchant_id from wepon_feature3) a left outer join wepon_d3_f4_t1 b 
				  on a.user_id=b.user_id and a.merchant_id=b.merchant_id
				)c left outer join wepon_d3_f4_t2 d 
				on c.user_id=d.user_id and c.merchant_id=d.merchant_id
			  )e left outer join wepon_d3_f4_t3 f 
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g  left outer join wepon_d3_f4_t4 h 
			on g.user_id=h.user_id and g.merchant_id=h.merchant_id
		  )i left outer join wepon_d3_f4_t5 j
		  on i.user_id=j.user_id and i.merchant_id=j.merchant_id
	  )t
  )tt
)ttt;



-- ###################   for dataset2  ################### 
drop table wepon_d2_f4_t1;
create table wepon_d2_f4_t1 as 
select user_id,merchant_id,count(*) as user_merchant_buy_total from
( select user_id,merchant_id from wepon_feature2 where date_pay!='null' )t
group by user_id,merchant_id;

drop table wepon_d2_f4_t2;
create table wepon_d2_f4_t2 as 
select user_id,merchant_id,count(*) as user_merchant_received from
( select user_id,merchant_id from wepon_feature2 where coupon_id!='null' )t
group by user_id,merchant_id;

drop table wepon_d2_f4_t3;
create table wepon_d2_f4_t3 as 
select user_id,merchant_id,count(*) as user_merchant_buy_use_coupon from
( select user_id,merchant_id from wepon_feature2 where date_pay!='null' and date_received!='null')t
group by user_id,merchant_id;

drop table wepon_d2_f4_t4;
create table wepon_d2_f4_t4 as 
select user_id,merchant_id,count(*) as user_merchant_any from
( select user_id,merchant_id from wepon_feature2 )t
group by user_id,merchant_id;


drop table wepon_d2_f4_t5;
create table wepon_d2_f4_t5 as 
select user_id,merchant_id,count(*) as user_merchant_buy_common from
( select user_id,merchant_id from wepon_feature2 where date_pay!='null' and coupon_id='null' )t
group by user_id,merchant_id;


drop table wepon_d2_f4;
create table wepon_d2_f4 as 
select user_id,merchant_id,user_merchant_buy_use_coupon,user_merchant_buy_common,
       coupon_transform_rate,user_merchant_coupon_buy_rate,user_merchant_common_buy_rate,user_merchant_rate,
	   case when user_merchant_buy_total is null then 0.0 else user_merchant_buy_total end as user_merchant_buy_total,
	   case when user_merchant_received is null then 0.0 else user_merchant_received end as user_merchant_received,
	   case when user_merchant_any is null then 0.0 else user_merchant_any end as user_merchant_any
from
(
  select tt.*,tt.user_merchant_buy_use_coupon/tt.user_merchant_received as coupon_transform_rate,
			  tt.user_merchant_buy_use_coupon/tt.user_merchant_buy_total as user_merchant_coupon_buy_rate,
			  tt.user_merchant_buy_common/tt.user_merchant_buy_total as user_merchant_common_buy_rate,
			  tt.user_merchant_buy_total/tt.user_merchant_any as user_merchant_rate
  from
  (
	  select user_id,merchant_id,user_merchant_buy_total,user_merchant_received,user_merchant_any,
			 case when user_merchant_buy_use_coupon is null then 0.0 else user_merchant_buy_use_coupon end as user_merchant_buy_use_coupon,
			 case when user_merchant_buy_common is null then 0.0 else user_merchant_buy_common end as user_merchant_buy_common
	  from
	  (
		  select i.*,j.user_merchant_buy_common from
		  (
			select g.*,h.user_merchant_any from
			(
			  select e.*,f.user_merchant_buy_use_coupon from
			  (
				select c.*,d.user_merchant_received from
				(
				  select a.*,b.user_merchant_buy_total from
				  (select distinct user_id,merchant_id from wepon_feature2) a left outer join wepon_d2_f4_t1 b 
				  on a.user_id=b.user_id and a.merchant_id=b.merchant_id
				)c left outer join wepon_d2_f4_t2 d 
				on c.user_id=d.user_id and c.merchant_id=d.merchant_id
			  )e left outer join wepon_d2_f4_t3 f 
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g  left outer join wepon_d2_f4_t4 h 
			on g.user_id=h.user_id and g.merchant_id=h.merchant_id
		  )i left outer join wepon_d2_f4_t5 j
		  on i.user_id=j.user_id and i.merchant_id=j.merchant_id
	  )t
  )tt
)ttt;



-- ###################   for dataset1  ################### 
drop table wepon_d1_f4_t1;
create table wepon_d1_f4_t1 as 
select user_id,merchant_id,count(*) as user_merchant_buy_total from
( select user_id,merchant_id from wepon_feature1 where date_pay!='null' )t
group by user_id,merchant_id;

drop table wepon_d1_f4_t2;
create table wepon_d1_f4_t2 as 
select user_id,merchant_id,count(*) as user_merchant_received from
( select user_id,merchant_id from wepon_feature1 where coupon_id!='null' )t
group by user_id,merchant_id;

drop table wepon_d1_f4_t3;
create table wepon_d1_f4_t3 as 
select user_id,merchant_id,count(*) as user_merchant_buy_use_coupon from
( select user_id,merchant_id from wepon_feature1 where date_pay!='null' and date_received!='null')t
group by user_id,merchant_id;

drop table wepon_d1_f4_t4;
create table wepon_d1_f4_t4 as 
select user_id,merchant_id,count(*) as user_merchant_any from
( select user_id,merchant_id from wepon_feature1 )t
group by user_id,merchant_id;


drop table wepon_d1_f4_t5;
create table wepon_d1_f4_t5 as 
select user_id,merchant_id,count(*) as user_merchant_buy_common from
( select user_id,merchant_id from wepon_feature1 where date_pay!='null' and coupon_id='null' )t
group by user_id,merchant_id;


drop table wepon_d1_f4;
create table wepon_d1_f4 as 
select user_id,merchant_id,user_merchant_buy_use_coupon,user_merchant_buy_common,
       coupon_transform_rate,user_merchant_coupon_buy_rate,user_merchant_common_buy_rate,user_merchant_rate,
	   case when user_merchant_buy_total is null then 0.0 else user_merchant_buy_total end as user_merchant_buy_total,
	   case when user_merchant_received is null then 0.0 else user_merchant_received end as user_merchant_received,
	   case when user_merchant_any is null then 0.0 else user_merchant_any end as user_merchant_any
from
(
  select tt.*,tt.user_merchant_buy_use_coupon/tt.user_merchant_received as coupon_transform_rate,
			  tt.user_merchant_buy_use_coupon/tt.user_merchant_buy_total as user_merchant_coupon_buy_rate,
			  tt.user_merchant_buy_common/tt.user_merchant_buy_total as user_merchant_common_buy_rate,
			  tt.user_merchant_buy_total/tt.user_merchant_any as user_merchant_rate
  from
  (
	  select user_id,merchant_id,user_merchant_buy_total,user_merchant_received,user_merchant_any,
			 case when user_merchant_buy_use_coupon is null then 0.0 else user_merchant_buy_use_coupon end as user_merchant_buy_use_coupon,
			 case when user_merchant_buy_common is null then 0.0 else user_merchant_buy_common end as user_merchant_buy_common
	  from
	  (
		  select i.*,j.user_merchant_buy_common from
		  (
			select g.*,h.user_merchant_any from
			(
			  select e.*,f.user_merchant_buy_use_coupon from
			  (
				select c.*,d.user_merchant_received from
				(
				  select a.*,b.user_merchant_buy_total from
				  (select distinct user_id,merchant_id from wepon_feature1) a left outer join wepon_d1_f4_t1 b 
				  on a.user_id=b.user_id and a.merchant_id=b.merchant_id
				)c left outer join wepon_d1_f4_t2 d 
				on c.user_id=d.user_id and c.merchant_id=d.merchant_id
			  )e left outer join wepon_d1_f4_t3 f 
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g  left outer join wepon_d1_f4_t4 h 
			on g.user_id=h.user_id and g.merchant_id=h.merchant_id
		  )i left outer join wepon_d1_f4_t5 j
		  on i.user_id=j.user_id and i.merchant_id=j.merchant_id
	  )t
  )tt
)ttt;
