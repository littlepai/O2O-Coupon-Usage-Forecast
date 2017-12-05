-- 6. user_coupon:
--       对label窗里的user_coupon，特征窗里用户领取过该coupon几次   label_user_coupon_feature_rec
--       对label窗里的user_coupon，特征窗里用户用该coupon消费过几次   label_user_cp_feature_buy
--       对label窗里的user_coupon，特征窗里用户对该coupon的核销率   label_user_coupon_feature_rate = label_user_cp_feature_buy/label_user_coupon_feature_rec

-- ###################   for dataset3  ################### 
drop table wepon_d3_f6_t1;
create table wepon_d3_f6_t1 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_rec from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset3)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature3 )b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

drop table wepon_d3_f6_t2;
create table wepon_d3_f6_t2 as
select user_id,coupon_id,sum(cnt) as label_user_cp_feature_buy from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset3)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature3 where date_pay!='null' and coupon_id!='null')b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

drop table wepon_d3_f6;
create table wepon_d3_f6 as 
select a.*,b.label_user_cp_feature_buy,
       case when a.label_user_coupon_feature_rec=0 then -1 else b.label_user_cp_feature_buy/a.label_user_coupon_feature_rec end as label_user_coupon_feature_rate
from wepon_d3_f6_t1 a left outer join wepon_d3_f6_t2 b 
on a.user_id=b.user_id and a.coupon_id=b.coupon_id;


-- ###################   for dataset2  ################### 
drop table wepon_d2_f6_t1;
create table wepon_d2_f6_t1 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_rec from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset2)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature2 )b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

drop table wepon_d2_f6_t2;
create table wepon_d2_f6_t2 as
select user_id,coupon_id,sum(cnt) as label_user_cp_feature_buy from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset2)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature2 where date_pay!='null' and coupon_id!='null')b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

drop table wepon_d2_f6;
create table wepon_d2_f6 as 
select a.*,b.label_user_cp_feature_buy,
       case when a.label_user_coupon_feature_rec=0 then -1 else b.label_user_cp_feature_buy/a.label_user_coupon_feature_rec end as label_user_coupon_feature_rate
from wepon_d2_f6_t1 a left outer join wepon_d2_f6_t2 b 
on a.user_id=b.user_id and a.coupon_id=b.coupon_id;


-- ###################   for dataset1  ################### 
drop table wepon_d1_f6_t1;
create table wepon_d1_f6_t1 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_rec from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset1)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature1 )b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

drop table wepon_d1_f6_t2;
create table wepon_d1_f6_t2 as
select user_id,coupon_id,sum(cnt) as label_user_cp_feature_buy from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset1)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature1 where date_pay!='null' and coupon_id!='null')b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

drop table wepon_d1_f6;
create table wepon_d1_f6 as 
select a.*,b.label_user_cp_feature_buy,
       case when a.label_user_coupon_feature_rec=0 then -1 else b.label_user_cp_feature_buy/a.label_user_coupon_feature_rec end as label_user_coupon_feature_rate
from wepon_d1_f6_t1 a left outer join wepon_d1_f6_t2 b 
on a.user_id=b.user_id and a.coupon_id=b.coupon_id;



-- ##############################################  合并各种特征文件，生成训练集测试集  ######################################
drop table wepon_d3;
create table wepon_d3 as
select t.*, case to_number(day_of_week) when 0 then 1 else 0 end as weekday1,case to_number(day_of_week) when 1 then 1 else 0 end as weekday2,
            case to_number(day_of_week) when 2 then 1 else 0 end as weekday3,case to_number(day_of_week) when 3 then 1 else 0 end as weekday4,
			case to_number(day_of_week) when 4 then 1 else 0 end as weekday5,case to_number(day_of_week) when 5 then 1 else 0 end as weekday6,
			case to_number(day_of_week) when 6 then 1 else 0 end as weekday7 
from
(
    select k.*,l.online_buy_total,l.online_buy_use_coupon ,l.online_buy_use_fixed ,l.online_coupon_received ,l.online_buy_merchant_count ,l.online_action_merchant_count ,
				l.online_buy_use_coupon_fixed ,l.online_buy_use_coupon_rate ,l.online_buy_use_fixed_rate ,l.online_buy_use_coupon_fixed_r ,l.online_coupon_transform_rate
	from
	(
		select i.*,j.label_user_coupon_feature_rec ,j.label_user_cp_feature_buy ,j.label_user_coupon_feature_rate  from
		(
		  select g.*,h.mu_all_coupon,h.mu_same_coupon,h.du_all_coupon,
					 h.du_same_coupon,h.mu_same_coupon_last,h.mu_same_coupon_first,
					 h.label_merchant_user_count,h.label_user_merchant_count,h.label_merchant_coupon_count,h.label_merchant_coupon_type,
					 h.label_user_merchant_coupon,h.label_same_coupon_count_later,h.label_coupon_count_later
		  from
		  (
			  select e.*,f.user_merchant_buy_total,f.user_merchant_received,f.user_merchant_any,f.user_merchant_buy_use_coupon,f.user_merchant_buy_common,
						 f.coupon_transform_rate,f.user_merchant_coupon_buy_rate,f.user_merchant_common_buy_rate,f.user_merchant_rate from
			  (
				select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance,d.avg_diff_date_datereceived,d.min_diff_date_datereceived,
					   d.max_diff_date_datereceived,d.buy_use_coupon,d.buy_use_coupon_rate,d.user_coupon_transform_rate,d.count_merchant,d.buy_total,d.coupon_received from
				(
				  select a.*,b.distinct_coupon_count, b.merchant_avg_distance, b.merchant_median_distance, b.merchant_max_distance,b.merchant_user_buy_count,
						 b.merchant_min_distance, b.sales_use_coupon, b.transform_rate,b.coupon_rate,b.total_coupon,b.total_sales from
				  wepon_d3_f2 a left outer join wepon_d3_f1 b 
				  on a.merchant_id=b.merchant_id
				)c left outer join wepon_d3_f3 d
				on c.user_id=d.user_id
			  )e left outer join wepon_d3_f4 f
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
		  )g  left outer join wepon_d3_f5 h
		  on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
		)i left outer join wepon_d3_f6 j 
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id
	)k left outer join wepon_d3_f7 l 
	on k.user_id=l.user_id
)t;




drop table wepon_d2;
create table wepon_d2 as
select t.*, case to_number(day_of_week) when 0 then 1 else 0 end as weekday1,case to_number(day_of_week) when 1 then 1 else 0 end as weekday2,
            case to_number(day_of_week) when 2 then 1 else 0 end as weekday3,case to_number(day_of_week) when 3 then 1 else 0 end as weekday4,
			case to_number(day_of_week) when 4 then 1 else 0 end as weekday5,case to_number(day_of_week) when 5 then 1 else 0 end as weekday6,
			case to_number(day_of_week) when 6 then 1 else 0 end as weekday7 
from
(
	select k.*,l.online_buy_total,l.online_buy_use_coupon ,l.online_buy_use_fixed ,l.online_coupon_received ,l.online_buy_merchant_count ,l.online_action_merchant_count ,
				l.online_buy_use_coupon_fixed ,l.online_buy_use_coupon_rate ,l.online_buy_use_fixed_rate ,l.online_buy_use_coupon_fixed_r ,l.online_coupon_transform_rate 
	from
	(
		select i.*,j.label_user_coupon_feature_rec ,j.label_user_cp_feature_buy ,j.label_user_coupon_feature_rate  from
		(
		  select g.*,h.mu_all_coupon,h.mu_same_coupon,h.du_all_coupon,
					 h.du_same_coupon,h.mu_same_coupon_last,h.mu_same_coupon_first,
					 h.label_merchant_user_count,h.label_user_merchant_count,h.label_merchant_coupon_count,h.label_merchant_coupon_type,
					 h.label_user_merchant_coupon,h.label_same_coupon_count_later,h.label_coupon_count_later,
					 case when g.date_pay='null' then 0 when abs(to_date(g.date_pay,'yyyymmdd')-to_date(g.date_received,'yyyymmdd'))<=15 then 1  else 0 end as label	 
		  from
		  (
			  select e.*,f.user_merchant_buy_total,f.user_merchant_received,f.user_merchant_any,f.user_merchant_buy_use_coupon,f.user_merchant_buy_common,
						 f.coupon_transform_rate,f.user_merchant_coupon_buy_rate,f.user_merchant_common_buy_rate,f.user_merchant_rate from
			  (
				select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance,d.avg_diff_date_datereceived,d.min_diff_date_datereceived,
					   d.max_diff_date_datereceived,d.buy_use_coupon,d.buy_use_coupon_rate,d.user_coupon_transform_rate,d.count_merchant,d.buy_total,d.coupon_received from
				(
				  select a.*,b.distinct_coupon_count, b.merchant_avg_distance, b.merchant_median_distance, b.merchant_max_distance,b.merchant_user_buy_count,
						 b.merchant_min_distance, b.sales_use_coupon, b.transform_rate,b.coupon_rate,b.total_coupon,b.total_sales from
				  wepon_d2_f2 a left outer join wepon_d2_f1 b 
				  on a.merchant_id=b.merchant_id
				)c left outer join wepon_d2_f3 d
				on c.user_id=d.user_id
			  )e left outer join wepon_d2_f4 f
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
		  )g  left outer join wepon_d2_f5 h
		  on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
		)i left outer join wepon_d2_f6 j 
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id
	)k left outer join wepon_d2_f7 l 
	on k.user_id=l.user_id
)t;



drop table wepon_d1;
create table wepon_d1 as
select t.*, case to_number(day_of_week) when 0 then 1 else 0 end as weekday1,case to_number(day_of_week) when 1 then 1 else 0 end as weekday2,
            case to_number(day_of_week) when 2 then 1 else 0 end as weekday3,case to_number(day_of_week) when 3 then 1 else 0 end as weekday4,
			case to_number(day_of_week) when 4 then 1 else 0 end as weekday5,case to_number(day_of_week) when 5 then 1 else 0 end as weekday6,
			case to_number(day_of_week) when 6 then 1 else 0 end as weekday7 
from
(
	select k.*,l.online_buy_total,l.online_buy_use_coupon ,l.online_buy_use_fixed ,l.online_coupon_received ,l.online_buy_merchant_count ,l.online_action_merchant_count ,
				l.online_buy_use_coupon_fixed ,l.online_buy_use_coupon_rate ,l.online_buy_use_fixed_rate ,l.online_buy_use_coupon_fixed_r ,l.online_coupon_transform_rate 
	from	
	(
		select i.*,j.label_user_coupon_feature_rec ,j.label_user_cp_feature_buy ,j.label_user_coupon_feature_rate  from
		(
		  select g.*,h.mu_all_coupon,h.mu_same_coupon,h.du_all_coupon,
					 h.du_same_coupon,h.mu_same_coupon_last,h.mu_same_coupon_first,
					 h.label_merchant_user_count,h.label_user_merchant_count,h.label_merchant_coupon_count,h.label_merchant_coupon_type,
					 h.label_user_merchant_coupon,h.label_same_coupon_count_later,h.label_coupon_count_later,
					 case when g.date_pay='null' then 0 when abs(to_date(g.date_pay,'yyyymmdd')-to_date(g.date_received,'yyyymmdd'))<=15 then 1  else 0 end as label	 
		  from
		  (
			  select e.*,f.user_merchant_buy_total,f.user_merchant_received,f.user_merchant_any,f.user_merchant_buy_use_coupon,f.user_merchant_buy_common,
						 f.coupon_transform_rate,f.user_merchant_coupon_buy_rate,f.user_merchant_common_buy_rate,f.user_merchant_rate from
			  (
				select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance,d.avg_diff_date_datereceived,d.min_diff_date_datereceived,
					   d.max_diff_date_datereceived,d.buy_use_coupon,d.buy_use_coupon_rate,d.user_coupon_transform_rate,d.count_merchant,d.buy_total,d.coupon_received from
				(
				  select a.*,b.distinct_coupon_count, b.merchant_avg_distance, b.merchant_median_distance, b.merchant_max_distance,b.merchant_user_buy_count,
						 b.merchant_min_distance, b.sales_use_coupon, b.transform_rate,b.coupon_rate,b.total_coupon,b.total_sales from
				  wepon_d1_f2 a left outer join wepon_d1_f1 b 
				  on a.merchant_id=b.merchant_id
				)c left outer join wepon_d1_f3 d
				on c.user_id=d.user_id
			  )e left outer join wepon_d1_f4 f
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
		  )g  left outer join wepon_d1_f5 h
		  on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
		)i left outer join wepon_d1_f6 j 
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id
	)k left outer join wepon_d1_f7 l 
	on k.user_id=l.user_id
)t;
