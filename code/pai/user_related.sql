-- 3.user related: 
--       count_merchant, distance. 
--       user_avg_distance, user_min_distance,user_max_distance. 
--       buy_use_coupon. buy_total. coupon_received.
--      avg_diff_date_datereceived. min_diff_date_datereceived. max_diff_date_datereceived.  
--      buy_use_coupon_rate = buy_use_coupon/buy_total
--       user_coupon_transform_rate = buy_use_coupon/coupon_received. 


-- ###################   for dataset3  ################### 
drop table wepon_user3;
create table wepon_user3 as select user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date_pay from wepon_feature3;

drop table wepon_d3_f3_t1;
create table wepon_d3_f3_t1 as 
select user_id,count(*) as count_merchant from
(
	select distinct user_id,merchant_id from wepon_user3 where date_pay!='null'
)t 
group by user_id;

drop table wepon_d3_f3_t2;
create table wepon_d3_f3_t2 as
select user_id,avg(distance) as user_avg_distance,min(distance) as user_min_distance,max(distance) as user_max_distance,median(decode(distance, 'null', 0, distance)) as user_median_distance from
(
	select user_id,distance from wepon_user3 where date_pay!='null' and coupon_id!='null' and distance!='null'
)t 
group by user_id;

drop table wepon_d3_f3_t3;
create table wepon_d3_f3_t3 as
select user_id,count(*) as buy_use_coupon from
(
	select user_id from wepon_user3 where date_pay!='null' and coupon_id!='null'
)t 
group by user_id;

drop table wepon_d3_f3_t4;
create table wepon_d3_f3_t4 as
select user_id,count(*) as buy_total from
(
	select user_id from wepon_user3 where date_pay!='null'
)t 
group by user_id;

drop table wepon_d3_f3_t5;
create table wepon_d3_f3_t5 as
select user_id,count(*) as coupon_received from
(
	select user_id from wepon_user3 where coupon_id!='null'
)t 
group by user_id;

drop table wepon_d3_f3_t6;
create table wepon_d3_f3_t6 as 
select user_id,avg(day_gap) as avg_diff_date_datereceived,min(day_gap) as min_diff_date_datereceived,max(day_gap) as max_diff_date_datereceived from
(
  select user_id,to_date(date_received,'yyyymmdd')-to_date(date_pay,'yyyymmdd') as day_gap 
  from wepon_user3 
  where date_pay!='null' and date_received!='null'
)t 
group by user_id;


drop table wepon_d3_f3;
create table wepon_d3_f3 as
select user_id,user_avg_distance,cast(user_min_distance as number) as user_min_distance,cast(user_max_distance as number) as user_max_distance,user_median_distance,
       abs(avg_diff_date_datereceived) as avg_diff_date_datereceived,abs(min_diff_date_datereceived) as min_diff_date_datereceived,abs(max_diff_date_datereceived) max_diff_date_datereceived,
       buy_use_coupon,buy_use_coupon_rate,user_coupon_transform_rate,
	   case when count_merchant is null then 0.0 else count_merchant end as count_merchant,
	   case when buy_total is null then 0.0 else buy_total end as buy_total,
	   case when coupon_received is null then 0.0 else coupon_received end as coupon_received
from
(
	select tt.*,tt.buy_use_coupon/tt.buy_total as buy_use_coupon_rate,tt.buy_use_coupon/tt.coupon_received as user_coupon_transform_rate from

	(
	  select user_id,count_merchant,user_avg_distance,user_min_distance,user_max_distance,user_median_distance,buy_total,coupon_received,avg_diff_date_datereceived,min_diff_date_datereceived,
			 max_diff_date_datereceived,case when buy_use_coupon is null then 0.0 else buy_use_coupon end as buy_use_coupon
	  from
	  (
		  select k.*,l.avg_diff_date_datereceived,l.min_diff_date_datereceived,l.max_diff_date_datereceived from
		  (
			select i.*,j.coupon_received from
			(
			  select g.*,h.buy_total from
			  (
				select e.*,f.buy_use_coupon from
				(
				  select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance from
				  (
					select a.*,b.count_merchant from
					(select distinct user_id from wepon_user3) a left outer join wepon_d3_f3_t1 b
					on a.user_id=b.user_id
				  )c left outer join wepon_d3_f3_t2 d
				  on c.user_id=d.user_id
				)e left outer join wepon_d3_f3_t3 f
				on e.user_id=f.user_id
			  )g left outer join wepon_d3_f3_t4 h
			  on g.user_id=h.user_id
			)i left outer join wepon_d3_f3_t5 j 
			on i.user_id=j.user_id
		  )k left outer join wepon_d3_f3_t6 l
		  on k.user_id=l.user_id
	  )t
	)tt
)ttt;





-- ###################   for dataset2  ################### 
drop table wepon_user2;
create table wepon_user2 as select user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date_pay from wepon_feature2;

drop table wepon_d2_f3_t1;
create table wepon_d2_f3_t1 as 
select user_id,count(*) as count_merchant from
(
	select distinct user_id,merchant_id from wepon_user2 where date_pay!='null'
)t 
group by user_id;

drop table wepon_d2_f3_t2;
create table wepon_d2_f3_t2 as
select user_id,avg(distance) as user_avg_distance,min(distance) as user_min_distance,max(distance) as user_max_distance,median(decode(distance, 'null', 0, distance)) as user_median_distance from
(
	select user_id,distance from wepon_user2 where date_pay!='null' and coupon_id!='null' and distance!='null'
)t 
group by user_id;

drop table wepon_d2_f3_t3;
create table wepon_d2_f3_t3 as
select user_id,count(*) as buy_use_coupon from
(
	select user_id from wepon_user2 where date_pay!='null' and coupon_id!='null'
)t 
group by user_id;

drop table wepon_d2_f3_t4;
create table wepon_d2_f3_t4 as
select user_id,count(*) as buy_total from
(
	select user_id from wepon_user2 where date_pay!='null'
)t 
group by user_id;

drop table wepon_d2_f3_t5;
create table wepon_d2_f3_t5 as
select user_id,count(*) as coupon_received from
(
	select user_id from wepon_user2 where coupon_id!='null'
)t 
group by user_id;

drop table wepon_d2_f3_t6;
create table wepon_d2_f3_t6 as 
select user_id,avg(day_gap) as avg_diff_date_datereceived,min(day_gap) as min_diff_date_datereceived,max(day_gap) as max_diff_date_datereceived from
(
  select user_id,to_date(date_received,'yyyymmdd')-to_date(date_pay,'yyyymmdd') as day_gap 
  from wepon_user2
  where date_pay!='null' and date_received!='null'
)t 
group by user_id;


drop table wepon_d2_f3;
create table wepon_d2_f3 as
select user_id,user_avg_distance,cast(user_min_distance as number) as user_min_distance,cast(user_max_distance as number) as user_max_distance,user_median_distance,
       abs(avg_diff_date_datereceived) as avg_diff_date_datereceived,abs(min_diff_date_datereceived) as min_diff_date_datereceived,abs(max_diff_date_datereceived) max_diff_date_datereceived,
       buy_use_coupon,buy_use_coupon_rate,user_coupon_transform_rate,
	   case when count_merchant is null then 0.0 else count_merchant end as count_merchant,
	   case when buy_total is null then 0.0 else buy_total end as buy_total,
	   case when coupon_received is null then 0.0 else coupon_received end as coupon_received
from
(
	select tt.*,tt.buy_use_coupon/tt.buy_total as buy_use_coupon_rate,tt.buy_use_coupon/tt.coupon_received as user_coupon_transform_rate from

	(
	  select user_id,count_merchant,user_avg_distance,user_min_distance,user_max_distance,user_median_distance,buy_total,coupon_received,avg_diff_date_datereceived,min_diff_date_datereceived,
			 max_diff_date_datereceived,case when buy_use_coupon is null then 0.0 else buy_use_coupon end as buy_use_coupon
	  from
	  (
		  select k.*,l.avg_diff_date_datereceived,l.min_diff_date_datereceived,l.max_diff_date_datereceived from
		  (
			select i.*,j.coupon_received from
			(
			  select g.*,h.buy_total from
			  (
				select e.*,f.buy_use_coupon from
				(
				  select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance from
				  (
					select a.*,b.count_merchant from
					(select distinct user_id from wepon_user2) a left outer join wepon_d2_f3_t1 b
					on a.user_id=b.user_id
				  )c left outer join wepon_d2_f3_t2 d
				  on c.user_id=d.user_id
				)e left outer join wepon_d2_f3_t3 f
				on e.user_id=f.user_id
			  )g left outer join wepon_d2_f3_t4 h
			  on g.user_id=h.user_id
			)i left outer join wepon_d2_f3_t5 j 
			on i.user_id=j.user_id
		  )k left outer join wepon_d2_f3_t6 l
		  on k.user_id=l.user_id
	  )t
	)tt
)ttt;


-- ###################   for dataset1  ################### 
drop table wepon_user1;
create table wepon_user1 as select user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date_pay from wepon_feature1;

drop table wepon_d1_f3_t1;
create table wepon_d1_f3_t1 as 
select user_id,count(*) as count_merchant from
(
	select distinct user_id,merchant_id from wepon_user1 where date_pay!='null'
)t 
group by user_id;

drop table wepon_d1_f3_t2;
create table wepon_d1_f3_t2 as
select user_id,avg(distance) as user_avg_distance,min(distance) as user_min_distance,max(distance) as user_max_distance,median(decode(distance, 'null', 0, distance)) as user_median_distance from
(
	select user_id,distance from wepon_user1 where date_pay!='null' and coupon_id!='null' and distance!='null'
)t 
group by user_id;

drop table wepon_d1_f3_t3;
create table wepon_d1_f3_t3 as
select user_id,count(*) as buy_use_coupon from
(
	select user_id from wepon_user1 where date_pay!='null' and coupon_id!='null'
)t 
group by user_id;

drop table wepon_d1_f3_t4;
create table wepon_d1_f3_t4 as
select user_id,count(*) as buy_total from
(
	select user_id from wepon_user1 where date_pay!='null'
)t 
group by user_id;

drop table wepon_d1_f3_t5;
create table wepon_d1_f3_t5 as
select user_id,count(*) as coupon_received from
(
	select user_id from wepon_user1 where coupon_id!='null'
)t 
group by user_id;

drop table wepon_d1_f3_t6;
create table wepon_d1_f3_t6 as 
select user_id,avg(day_gap) as avg_diff_date_datereceived,min(day_gap) as min_diff_date_datereceived,max(day_gap) as max_diff_date_datereceived from
(
  select user_id,to_date(date_received,'yyyymmdd')-to_date(date_pay,'yyyymmdd') as day_gap 
  from wepon_user1
  where date_pay!='null' and date_received!='null'
)t 
group by user_id;


drop table wepon_d1_f3;
create table wepon_d1_f3 as
select user_id,user_avg_distance,cast(user_min_distance as number) as user_min_distance,cast(user_max_distance as number) as user_max_distance,user_median_distance,
       abs(avg_diff_date_datereceived) as avg_diff_date_datereceived,abs(min_diff_date_datereceived) as min_diff_date_datereceived,abs(max_diff_date_datereceived) max_diff_date_datereceived,
       buy_use_coupon,buy_use_coupon_rate,user_coupon_transform_rate,
	   case when count_merchant is null then 0.0 else count_merchant end as count_merchant,
	   case when buy_total is null then 0.0 else buy_total end as buy_total,
	   case when coupon_received is null then 0.0 else coupon_received end as coupon_received
from
(
	select tt.*,tt.buy_use_coupon/tt.buy_total as buy_use_coupon_rate,tt.buy_use_coupon/tt.coupon_received as user_coupon_transform_rate from

	(
	  select user_id,count_merchant,user_avg_distance,user_min_distance,user_max_distance,user_median_distance,buy_total,coupon_received,avg_diff_date_datereceived,min_diff_date_datereceived,
			 max_diff_date_datereceived,case when buy_use_coupon is null then 0.0 else buy_use_coupon end as buy_use_coupon
	  from
	  (
		  select k.*,l.avg_diff_date_datereceived,l.min_diff_date_datereceived,l.max_diff_date_datereceived from
		  (
			select i.*,j.coupon_received from
			(
			  select g.*,h.buy_total from
			  (
				select e.*,f.buy_use_coupon from
				(
				  select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance from
				  (
					select a.*,b.count_merchant from
					(select distinct user_id from wepon_user1) a left outer join wepon_d1_f3_t1 b
					on a.user_id=b.user_id
				  )c left outer join wepon_d1_f3_t2 d
				  on c.user_id=d.user_id
				)e left outer join wepon_d1_f3_t3 f
				on e.user_id=f.user_id
			  )g left outer join wepon_d1_f3_t4 h
			  on g.user_id=h.user_id
			)i left outer join wepon_d1_f3_t5 j 
			on i.user_id=j.user_id
		  )k left outer join wepon_d1_f3_t6 l
		  on k.user_id=l.user_id
	  )t
	)tt
)ttt;
