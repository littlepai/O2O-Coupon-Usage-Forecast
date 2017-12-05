-- 7. online表的特征（都是用户相关）
--       用户线上购买总次数  online_buy_total
--       用户线上用coupon购买的总次数 online_buy_use_coupon
--       用户线上用fixed购买的总次数  online_buy_use_fixed
--       用户线上收到的coupon总次数   online_coupon_received
--       用户线上有发生购买的merchant个数  online_buy_merchant_count
--       用户线上有action的merchant个数      online_action_merchant_count
--       online_buy_use_coupon_fixed = online_buy_use_coupon+online_buy_use_fixed
--       online_buy_use_coupon_rate = online_buy_use_coupon/online_buy_total
--       online_buy_use_fixed_rate = online_buy_use_fixed/online_buy_total
--       online_buy_use_coupon_fixed_rate = online_buy_use_coupon_fixed/online_buy_total
--       online_coupon_transform_rate = online_buy_use_coupon/online_coupon_received



-- ##############  for dataset3  ###################
drop table wepon_d3_f7_t1;
create table wepon_d3_f7_t1 as
  select user_id, count(1) as online_buy_total from wepon_online_feature3 t where action=1
group by user_id;


drop table wepon_d3_f7_t2;
create table wepon_d3_f7_t2 as
select user_id,count(*) as online_buy_use_coupon from wepon_online_feature3 t where action=1 and coupon_id!='null' and coupon_id!='fixed'
group by user_id;


drop table wepon_d3_f7_t3;
create table wepon_d3_f7_t3 as
select user_id,count(*) as online_buy_use_fixed from wepon_online_feature3 t where action=1 and coupon_id='fixed'
group by user_id;


drop table wepon_d3_f7_t4;
create table wepon_d3_f7_t4 as
select user_id,count(*) as online_coupon_received from wepon_online_feature3 t where coupon_id!='null' and coupon_id!='fixed'
group by user_id;


drop table wepon_d3_f7_t5;
create table wepon_d3_f7_t5 as
select user_id,count(distinct t.merchant_id) as online_buy_merchant_count from wepon_online_feature3 t where action=1
group by user_id;

drop table wepon_d3_f7_t6;
create table wepon_d3_f7_t6 as
select user_id,count(distinct t.merchant_id) as online_action_merchant_count from wepon_online_feature3 t
group by user_id;

drop table wepon_d3_f7;
create table wepon_d3_f7 as
select t.*,t.online_buy_use_coupon+t.online_buy_use_fixed as online_buy_use_coupon_fixed,
           case when t.online_buy_total=0 then -1 else t.online_buy_use_coupon/t.online_buy_total end as online_buy_use_coupon_rate,
       case when t.online_buy_total=0 then -1 else t.online_buy_use_fixed/t.online_buy_total  end as online_buy_use_fixed_rate,
       case when t.online_buy_total=0 then -1 else (t.online_buy_use_coupon+t.online_buy_use_fixed)/t.online_buy_total end as  online_buy_use_coupon_fixed_r,
       case when t.online_coupon_received=0 then -1 else t.online_buy_use_coupon/t.online_coupon_received end as online_coupon_transform_rate
from
(
  select a.user_id,case when b.online_buy_total is null then 0 else b.online_buy_total end as online_buy_total,
          case when c.online_buy_use_coupon is null then 0 else c.online_buy_use_coupon end as online_buy_use_coupon,
          case when d.online_buy_use_fixed is null then 0 else d.online_buy_use_fixed end as online_buy_use_fixed,
          case when e.online_coupon_received is null then 0 else e.online_coupon_received end as online_coupon_received,
          case when f.online_buy_merchant_count is null then 0 else f.online_buy_merchant_count end as online_buy_merchant_count,
          case when g.online_action_merchant_count is null then 0 else g.online_action_merchant_count end as online_action_merchant_count
  from
    (select distinct user_id from wepon_online_feature3) a
  left outer join
    wepon_d3_f7_t1 b
    on a.user_id=b.user_id
  left outer join
    wepon_d3_f7_t2 c
    on a.user_id=c.user_id
  left outer join
    wepon_d3_f7_t3 d
    on a.user_id=d.user_id
  left outer join
    wepon_d3_f7_t4 e
    on a.user_id=e.user_id
  left outer join
    wepon_d3_f7_t5 f
    on a.user_id=f.user_id
  left outer join
    wepon_d3_f7_t6 g
    on a.user_id=g.user_id
)t;


-- ##############  for dataset2  ###################
drop table wepon_d2_f7_t1;
create table wepon_d2_f7_t1 as
  select user_id, count(1) as online_buy_total from wepon_online_feature3 t where action=1
group by user_id;


drop table wepon_d2_f7_t2;
create table wepon_d2_f7_t2 as
select user_id,count(*) as online_buy_use_coupon from wepon_online_feature3 t where action=1 and coupon_id!='null' and coupon_id!='fixed'
group by user_id;


drop table wepon_d2_f7_t3;
create table wepon_d2_f7_t3 as
select user_id,count(*) as online_buy_use_fixed from wepon_online_feature3 t where action=1 and coupon_id='fixed'
group by user_id;


drop table wepon_d2_f7_t4;
create table wepon_d2_f7_t4 as
select user_id,count(*) as online_coupon_received from wepon_online_feature3 t where coupon_id!='null' and coupon_id!='fixed'
group by user_id;


drop table wepon_d2_f7_t5;
create table wepon_d2_f7_t5 as
select user_id,count(distinct t.merchant_id) as online_buy_merchant_count from wepon_online_feature3 t where action=1
group by user_id;

drop table wepon_d2_f7_t6;
create table wepon_d2_f7_t6 as
select user_id,count(distinct t.merchant_id) as online_action_merchant_count from wepon_online_feature3 t
group by user_id;

drop table wepon_d2_f7;
create table wepon_d2_f7 as
select t.*,t.online_buy_use_coupon+t.online_buy_use_fixed as online_buy_use_coupon_fixed,
           case when t.online_buy_total=0 then -1 else t.online_buy_use_coupon/t.online_buy_total end as online_buy_use_coupon_rate,
       case when t.online_buy_total=0 then -1 else t.online_buy_use_fixed/t.online_buy_total  end as online_buy_use_fixed_rate,
       case when t.online_buy_total=0 then -1 else (t.online_buy_use_coupon+t.online_buy_use_fixed)/t.online_buy_total end as  online_buy_use_coupon_fixed_r,
       case when t.online_coupon_received=0 then -1 else t.online_buy_use_coupon/t.online_coupon_received end as online_coupon_transform_rate
from
(
  select a.user_id,case when b.online_buy_total is null then 0 else b.online_buy_total end as online_buy_total,
          case when c.online_buy_use_coupon is null then 0 else c.online_buy_use_coupon end as online_buy_use_coupon,
          case when d.online_buy_use_fixed is null then 0 else d.online_buy_use_fixed end as online_buy_use_fixed,
          case when e.online_coupon_received is null then 0 else e.online_coupon_received end as online_coupon_received,
          case when f.online_buy_merchant_count is null then 0 else f.online_buy_merchant_count end as online_buy_merchant_count,
          case when g.online_action_merchant_count is null then 0 else g.online_action_merchant_count end as online_action_merchant_count
  from
    (select distinct user_id from wepon_online_feature3) a
  left outer join
    wepon_d2_f7_t1 b
    on a.user_id=b.user_id
  left outer join
    wepon_d2_f7_t2 c
    on a.user_id=c.user_id
  left outer join
    wepon_d2_f7_t3 d
    on a.user_id=d.user_id
  left outer join
    wepon_d2_f7_t4 e
    on a.user_id=e.user_id
  left outer join
    wepon_d2_f7_t5 f
    on a.user_id=f.user_id
  left outer join
    wepon_d2_f7_t6 g
    on a.user_id=g.user_id
)t;



-- ##############  for dataset1  ###################
drop table wepon_d1_f7_t1;
create table wepon_d1_f7_t1 as
  select user_id, count(1) as online_buy_total from wepon_online_feature3 t where action=1
group by user_id;


drop table wepon_d1_f7_t2;
create table wepon_d1_f7_t2 as
select user_id,count(*) as online_buy_use_coupon from wepon_online_feature3 t where action=1 and coupon_id!='null' and coupon_id!='fixed'
group by user_id;


drop table wepon_d1_f7_t3;
create table wepon_d1_f7_t3 as
select user_id,count(*) as online_buy_use_fixed from wepon_online_feature3 t where action=1 and coupon_id='fixed'
group by user_id;


drop table wepon_d1_f7_t4;
create table wepon_d1_f7_t4 as
select user_id,count(*) as online_coupon_received from wepon_online_feature3 t where coupon_id!='null' and coupon_id!='fixed'
group by user_id;


drop table wepon_d1_f7_t5;
create table wepon_d1_f7_t5 as
select user_id,count(distinct t.merchant_id) as online_buy_merchant_count from wepon_online_feature3 t where action=1
group by user_id;

drop table wepon_d1_f7_t6;
create table wepon_d1_f7_t6 as
select user_id,count(distinct t.merchant_id) as online_action_merchant_count from wepon_online_feature3 t
group by user_id;

drop table wepon_d1_f7;
create table wepon_d1_f7 as
select t.*,t.online_buy_use_coupon+t.online_buy_use_fixed as online_buy_use_coupon_fixed,
           case when t.online_buy_total=0 then -1 else t.online_buy_use_coupon/t.online_buy_total end as online_buy_use_coupon_rate,
       case when t.online_buy_total=0 then -1 else t.online_buy_use_fixed/t.online_buy_total  end as online_buy_use_fixed_rate,
       case when t.online_buy_total=0 then -1 else (t.online_buy_use_coupon+t.online_buy_use_fixed)/t.online_buy_total end as  online_buy_use_coupon_fixed_r,
       case when t.online_coupon_received=0 then -1 else t.online_buy_use_coupon/t.online_coupon_received end as online_coupon_transform_rate
from
(
  select a.user_id,case when b.online_buy_total is null then 0 else b.online_buy_total end as online_buy_total,
          case when c.online_buy_use_coupon is null then 0 else c.online_buy_use_coupon end as online_buy_use_coupon,
          case when d.online_buy_use_fixed is null then 0 else d.online_buy_use_fixed end as online_buy_use_fixed,
          case when e.online_coupon_received is null then 0 else e.online_coupon_received end as online_coupon_received,
          case when f.online_buy_merchant_count is null then 0 else f.online_buy_merchant_count end as online_buy_merchant_count,
          case when g.online_action_merchant_count is null then 0 else g.online_action_merchant_count end as online_action_merchant_count
  from
    (select distinct user_id from wepon_online_feature3) a
  left outer join
    wepon_d1_f7_t1 b
    on a.user_id=b.user_id
  left outer join
    wepon_d1_f7_t2 c
    on a.user_id=c.user_id
  left outer join
    wepon_d1_f7_t3 d
    on a.user_id=d.user_id
  left outer join
    wepon_d1_f7_t4 e
    on a.user_id=e.user_id
  left outer join
    wepon_d1_f7_t5 f
    on a.user_id=f.user_id
  left outer join
    wepon_d1_f7_t6 g
    on a.user_id=g.user_id
)t;
