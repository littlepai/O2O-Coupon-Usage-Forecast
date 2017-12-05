-- 1.merchant related: 
--       sales_use_coupon. total_coupon  distinct_coupon_count
--       transform_rate = sales_use_coupon/total_coupon.
--       merchant_avg_distance,merchant_min_distance,merchant_max_distance of those use coupon 
--       total_sales.  coupon_rate = sales_use_coupon/total_sales.  
--       消费过该商家的不同用户数量   merchant_user_buy_count

-- ##############  for dataset3  ################### 
drop table wepon_merchant3;
create table wepon_merchant3 as select merchant_id,user_id,coupon_id,distance,date_received,date_pay from wepon_feature3;

drop table wepon_d3_f1_t1;
create table wepon_d3_f1_t1 as 
select merchant_id,sum(cnt) as total_sales from
(
	select merchant_id,1 as cnt from wepon_merchant3 where date_pay!='null'
)t 
group by merchant_id;

drop table wepon_d3_f1_t2;
create table wepon_d3_f1_t2 as 
select merchant_id,sum(cnt) as sales_use_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant3 where date_pay!='null' and coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d3_f1_t3;
create table wepon_d3_f1_t3 as 
select merchant_id,sum(cnt) as total_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant3 where coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d3_f1_t4;
create table wepon_d3_f1_t4 as 
select merchant_id,count(*) as distinct_coupon_count from
(
	select distinct merchant_id,coupon_id from wepon_merchant3 where coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d3_f1_t5;
create table wepon_d3_f1_t5 as 
select merchant_id,avg(distance) as merchant_avg_distance,median(decode(distance, 'null', 0, distance)) as merchant_median_distance,
       max(distance) as merchant_max_distance,min(distance) as merchant_min_distance from
(
  select merchant_id,to_number(distance) as distance from wepon_merchant3 where date_pay!='null' and coupon_id!='null' and distance!='null'
)t
group by merchant_id;


drop table wepon_d3_f1_t6;
create table wepon_d3_f1_t6 as 
select merchant_id,count(*) as merchant_user_buy_count from
(
	  select distinct merchant_id,user_id from wepon_merchant3 where date_pay!='null'
)t 
group by merchant_id;



drop table wepon_d3_f1;
create table wepon_d3_f1 as
select merchant_id,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,cast(merchant_max_distance as number) as merchant_max_distance,cast(merchant_min_distance as number ) as merchant_min_distance,
	  merchant_user_buy_count,sales_use_coupon,transform_rate,coupon_rate,case when total_coupon is null then 0.0 else total_coupon end as total_coupon,case when total_sales is null then 0.0 else total_sales end as total_sales
from
(
	select tt.*,tt.sales_use_coupon/tt.total_coupon as transform_rate,tt.sales_use_coupon/tt.total_sales as coupon_rate from
	(
	  select merchant_id,total_sales,total_coupon,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,merchant_max_distance,merchant_min_distance,merchant_user_buy_count,
			 case when sales_use_coupon is null then 0.0 else sales_use_coupon end as sales_use_coupon
	  from
	  (
	      select k.*,l.merchant_user_buy_count from
		  (
			select i.*,j.merchant_avg_distance,j.merchant_median_distance,j.merchant_max_distance,j.merchant_min_distance from
			(
			  select g.*,h.distinct_coupon_count from
			  (
				select e.*,f.total_coupon from
				(
				  select c.*,d.sales_use_coupon from
				  (
					select a.*,b.total_sales from
					(select distinct merchant_id from wepon_merchant3) a left outer join wepon_d3_f1_t1 b 
					on a.merchant_id=b.merchant_id
				  )c left outer join wepon_d3_f1_t2 d 
				  on c.merchant_id=d.merchant_id
				)e left outer join wepon_d3_f1_t3 f 
				on e.merchant_id=f.merchant_id
			  )g left outer join wepon_d3_f1_t4 h 
			  on g.merchant_id=h.merchant_id
			)i left outer join wepon_d3_f1_t5 j 
			on i.merchant_id=j.merchant_id
		  )k left outer join wepon_d3_f1_t6 l 
		  on k.merchant_id=l.merchant_id
	  )t
	)tt
)ttt;





-- ##############  for dataset2  ################### 
drop table wepon_merchant2;
create table wepon_merchant2 as select merchant_id,user_id,coupon_id,distance,date_received,date_pay from wepon_feature2;

drop table wepon_d2_f1_t1;
create table wepon_d2_f1_t1 as 
select merchant_id,sum(cnt) as total_sales from
(
	select merchant_id,1 as cnt from wepon_merchant2 where date_pay!='null'
)t 
group by merchant_id;

drop table wepon_d2_f1_t2;
create table wepon_d2_f1_t2 as 
select merchant_id,sum(cnt) as sales_use_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant2 where date_pay!='null' and coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d2_f1_t3;
create table wepon_d2_f1_t3 as 
select merchant_id,sum(cnt) as total_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant2 where coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d2_f1_t4;
create table wepon_d2_f1_t4 as 
select merchant_id,count(*) as distinct_coupon_count from
(
	select distinct merchant_id,coupon_id from wepon_merchant2 where coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d2_f1_t5;
create table wepon_d2_f1_t5 as 
select merchant_id,avg(distance) as merchant_avg_distance,median(decode(distance, 'null', 0, distance)) as merchant_median_distance,
       max(distance) as merchant_max_distance,min(distance) as merchant_min_distance from
(
	select merchant_id,distance from wepon_merchant2 where date_pay!='null' and coupon_id!='null' and distance!='null'
)t
group by merchant_id;


drop table wepon_d2_f1_t6;
create table wepon_d2_f1_t6 as 
select merchant_id,count(*) as merchant_user_buy_count from
(
	  select distinct merchant_id,user_id from wepon_merchant2 where date_pay!='null'
)t 
group by merchant_id;


drop table wepon_d2_f1;
create table wepon_d2_f1 as
select merchant_id,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,cast(merchant_max_distance as number) as merchant_max_distance,cast(merchant_min_distance as number ) as merchant_min_distance,
	  merchant_user_buy_count,sales_use_coupon,transform_rate,coupon_rate,case when total_coupon is null then 0.0 else total_coupon end as total_coupon,case when total_sales is null then 0.0 else total_sales end as total_sales
from
(
	select tt.*,tt.sales_use_coupon/tt.total_coupon as transform_rate,tt.sales_use_coupon/tt.total_sales as coupon_rate from
	(
	  select merchant_id,total_sales,total_coupon,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,merchant_max_distance,merchant_min_distance,merchant_user_buy_count,
			 case when sales_use_coupon is null then 0.0 else sales_use_coupon end as sales_use_coupon
	  from
	  (
	      select k.*,l.merchant_user_buy_count from
		  (
			select i.*,j.merchant_avg_distance,j.merchant_median_distance,j.merchant_max_distance,j.merchant_min_distance from
			(
			  select g.*,h.distinct_coupon_count from
			  (
				select e.*,f.total_coupon from
				(
				  select c.*,d.sales_use_coupon from
				  (
					select a.*,b.total_sales from
					(select distinct merchant_id from wepon_merchant2) a left outer join wepon_d2_f1_t1 b 
					on a.merchant_id=b.merchant_id
				  )c left outer join wepon_d2_f1_t2 d 
				  on c.merchant_id=d.merchant_id
				)e left outer join wepon_d2_f1_t3 f 
				on e.merchant_id=f.merchant_id
			  )g left outer join wepon_d2_f1_t4 h 
			  on g.merchant_id=h.merchant_id
			)i left outer join wepon_d2_f1_t5 j 
			on i.merchant_id=j.merchant_id
		  )k left outer join wepon_d2_f1_t6 l 
		  on k.merchant_id=l.merchant_id
	  )t
	)tt
)ttt;


-- ##############  for dataset1  ################### 
drop table wepon_merchant1;
create table wepon_merchant1 as select merchant_id,user_id,coupon_id,distance,date_received,date_pay from wepon_feature1;

drop table wepon_d1_f1_t1;
create table wepon_d1_f1_t1 as 
select merchant_id,sum(cnt) as total_sales from
(
	select merchant_id,1 as cnt from wepon_merchant1 where date_pay!='null'
)t 
group by merchant_id;

drop table wepon_d1_f1_t2;
create table wepon_d1_f1_t2 as 
select merchant_id,sum(cnt) as sales_use_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant1 where date_pay!='null' and coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d1_f1_t3;
create table wepon_d1_f1_t3 as 
select merchant_id,sum(cnt) as total_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant1 where coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d1_f1_t4;
create table wepon_d1_f1_t4 as 
select merchant_id,count(*) as distinct_coupon_count from
(
	select distinct merchant_id,coupon_id from wepon_merchant1 where coupon_id!='null'
)t 
group by merchant_id;

drop table wepon_d1_f1_t5;
create table wepon_d1_f1_t5 as 
select merchant_id,avg(distance) as merchant_avg_distance,median(decode(distance, 'null', 0, distance)) as merchant_median_distance,
       max(distance) as merchant_max_distance,min(distance) as merchant_min_distance from
(
	select merchant_id,distance from wepon_merchant1 where date_pay!='null' and coupon_id!='null' and distance!='null'
)t
group by merchant_id;


drop table wepon_d1_f1_t6;
create table wepon_d1_f1_t6 as 
select merchant_id,count(*) as merchant_user_buy_count from
(
	  select distinct merchant_id,user_id from wepon_merchant1 where date_pay!='null'
)t 
group by merchant_id;


drop table wepon_d1_f1;
create table wepon_d1_f1 as
select merchant_id,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,cast(merchant_max_distance as number) as merchant_max_distance,cast(merchant_min_distance as number ) as merchant_min_distance,
	  merchant_user_buy_count,sales_use_coupon,transform_rate,coupon_rate,case when total_coupon is null then 0.0 else total_coupon end as total_coupon,case when total_sales is null then 0.0 else total_sales end as total_sales
from
(
	select tt.*,tt.sales_use_coupon/tt.total_coupon as transform_rate,tt.sales_use_coupon/tt.total_sales as coupon_rate from
	(
	  select merchant_id,total_sales,total_coupon,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,merchant_max_distance,merchant_min_distance,merchant_user_buy_count,
			 case when sales_use_coupon is null then 0.0 else sales_use_coupon end as sales_use_coupon
	  from
	  (
	      select k.*,l.merchant_user_buy_count from
		  (
			select i.*,j.merchant_avg_distance,j.merchant_median_distance,j.merchant_max_distance,j.merchant_min_distance from
			(
			  select g.*,h.distinct_coupon_count from
			  (
				select e.*,f.total_coupon from
				(
				  select c.*,d.sales_use_coupon from
				  (
					select a.*,b.total_sales from
					(select distinct merchant_id from wepon_merchant1) a left outer join wepon_d1_f1_t1 b 
					on a.merchant_id=b.merchant_id
				  )c left outer join wepon_d1_f1_t2 d 
				  on c.merchant_id=d.merchant_id
				)e left outer join wepon_d1_f1_t3 f 
				on e.merchant_id=f.merchant_id
			  )g left outer join wepon_d1_f1_t4 h 
			  on g.merchant_id=h.merchant_id
			)i left outer join wepon_d1_f1_t5 j 
			on i.merchant_id=j.merchant_id
		  )k left outer join wepon_d1_f1_t6 l 
		  on k.merchant_id=l.merchant_id
	  )t
	)tt
)ttt;
