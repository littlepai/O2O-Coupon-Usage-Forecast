-- 5. other feature:��label ������ȡ��������
--       mu_all_coupon
--       mu_same_coupon
--       du_all_coupon
--       du_same_coupon
--       du_same_coupon_last
--       du_same_coupon_first
--       �̼��н������û���Ŀ label_merchant_user_count
--       �û��н������̼���Ŀ     label_user_merchant_count
--       �̼ҷ����������Ż�ȯ��Ŀ  label_merchant_coupon_count
--       �̼ҷ����������Ż�ȯ������Ŀ  label_merchant_coupon_type
--       �û���ȡ���̼ҵ������Ż�ȯ��Ŀ  label_user_merchant_coupon
--       �û��ڴ˴��Ż�ȯ֮����ȡ�˶��ٸ��Ż�ȯ   label_same_coupon_count_later
--       �û��ڴ˴��Ż�ȯ֮����ȡ�˶����Ż�ȯ     label_coupon_count_later


-- ##############  for dataset3  ###################
drop table wepon_d3_f5_t1;
create table wepon_d3_f5_t1 as
select user_id,sum(cnt) as mu_all_coupon from
(
  select user_id,1 as cnt from wepon_dataset3
)t
group by user_id;

drop table wepon_d3_f5_t2;
create table wepon_d3_f5_t2 as
select user_id,coupon_id,sum(cnt) as mu_same_coupon from
(
  select user_id,coupon_id,1 as cnt from wepon_dataset3
)t
group by user_id,coupon_id;

drop table wepon_d3_f5_t3;
create table wepon_d3_f5_t3 as
select user_id,date_received,sum(cnt) as du_all_coupon from
(
  select user_id,date_received,1 as cnt from wepon_dataset3
)t
group by user_id,date_received;

drop table wepon_d3_f5_t4;
create table wepon_d3_f5_t4 as
select user_id,coupon_id,date_received,sum(cnt) as du_same_coupon from
(
  select user_id,coupon_id,date_received,1 as cnt from wepon_dataset3
)t
group by user_id,coupon_id,date_received;

drop table wepon_d3_f5_temp;
create table wepon_d3_f5_temp as
select user_id,coupon_id,max(date_received) as max_date_received, min(date_received) as min_date_received from
(
  select a.user_id,a.coupon_id,a.date_received from
  (select user_id,coupon_id,date_received from wepon_dataset3)a
  join
  (select user_id,coupon_id from wepon_d3_f5_t2 where mu_same_coupon>1)b --��ȡ��ͬ���Żݾ��ε�
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;


drop table wepon_d3_f5_t5;
create table wepon_d3_f5_t5 as
select user_id,coupon_id,merchant_id,date_received,
       case when date_received=max_date_received then 1
          when max_date_received is null then -1  -- ֻ��ȡ��һ�ε�
      else 0 end as mu_same_coupon_last,
    case when date_received=min_date_received then 1
          when min_date_received is null then -1  -- ֻ��ȡ��һ�ε�
      else 0 end as mu_same_coupon_first
from
(
  select a.user_id,a.coupon_id,a.merchant_id,a.date_received,b.max_date_received,b.min_date_received
  from wepon_dataset3 a left outer join wepon_d3_f5_temp b
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t;

drop table wepon_d3_f5_t6;
create table wepon_d3_f5_t6 as
select merchant_id,count(*) as label_merchant_user_count from
(
  select distinct merchant_id,user_id from wepon_dataset3
)t
group by merchant_id;

drop table wepon_d3_f5_t7;
create table wepon_d3_f5_t7 as
select user_id,count(*) as label_user_merchant_count from
(
  select distinct merchant_id,user_id from wepon_dataset3
)t
group by user_id;


drop table wepon_d3_f5_t8;
create table wepon_d3_f5_t8 as select merchant_id,count(*) as label_merchant_coupon_count from wepon_dataset3 group by merchant_id;

drop table wepon_d3_f5_t9;
create table wepon_d3_f5_t9 as select merchant_id,count(*) as label_merchant_coupon_type from
(select distinct merchant_id,coupon_id from wepon_dataset3)t
group by merchant_id;

drop table wepon_d3_f5_t10;
create table wepon_d3_f5_t10 as
select user_id,count(*) as label_user_merchant_coupon from
(
  select merchant_id,user_id from wepon_dataset3
)t
group by user_id;


drop table wepon_d3_f5_t11;
create table wepon_d3_f5_t11 as    -- �û��ڴ˴��Ż�ȯ֮����ȡ�˶��ٸ��Ż�ȯ   label_same_coupon_count_later  ��ʵ��ʱ���ȶ�ÿ�조ͬ�û�ͬ�Żݾ�ȥ�أ�
select user_id,coupon_id,date_received,label_same_coupon_count_later-1 as label_same_coupon_count_later from
(
  select user_id,coupon_id,date_received,row_number() over (partition by user_id,coupon_id order by date_received desc) as label_same_coupon_count_later from
  (
    select distinct user_id,coupon_id,date_received from wepon_dataset3
  )t
)tt;


drop table wepon_d3_f5_t12;
create table wepon_d3_f5_t12 as    --�û��ڴ˴��Ż�ȯ֮����ȡ�˶����Ż�ȯ     label_coupon_count_later  ��ʵ��ʱ���ȶ�ÿ�조ͬ�û�ͬ�족ȥ�أ�
select user_id,date_received,label_coupon_count_later-1 as label_coupon_count_later from
(
  select user_id,date_received,row_number() over (partition by user_id order by date_received desc) as label_coupon_count_later from
  (
    select distinct user_id,date_received from wepon_dataset3
  )t
)tt;



-- �ϲ���������
drop table wepon_d3_f5;
create table wepon_d3_f5 as
select u.*,v.label_coupon_count_later from
(
  select s.*,t.label_same_coupon_count_later from
  (
    select q.*,r.label_user_merchant_coupon from
    (
      select o.*,p.label_merchant_coupon_type from
      (
        select m.*,n.label_merchant_coupon_count from
        (
          select k.*,l.label_user_merchant_count from
          (
            select i.*,j.label_merchant_user_count from
            (
            select g.*,h.du_same_coupon from
            (
              select e.*,f.du_all_coupon from
              (
                select c.*,d.mu_same_coupon from
                (
                select a.*,b.mu_all_coupon from
                wepon_d3_f5_t5 a join wepon_d3_f5_t1 b
                on a.user_id=b.user_id
                )c join wepon_d3_f5_t2 d
                on c.user_id=d.user_id and c.coupon_id=d.coupon_id
              )e join wepon_d3_f5_t3 f
              on e.user_id=f.user_id and e.date_received=f.date_received
            )g join wepon_d3_f5_t4 h
            on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
            )i left outer join wepon_d3_f5_t6 j
            on i.merchant_id=j.merchant_id
          )k left outer join wepon_d3_f5_t7 l
          on k.user_id=l.user_id
        )m left outer join wepon_d3_f5_t8 n
        on m.merchant_id=n.merchant_id
      )o left outer join wepon_d3_f5_t9 p
      on o.merchant_id=p.merchant_id
    )q left outer join wepon_d3_f5_t10 r
    on q.user_id=r.user_id
  )s left outer join wepon_d3_f5_t11 t
  on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received
)u  left outer join wepon_d3_f5_t12 v
on u.user_id=v.user_id and u.date_received=v.date_received;




-- ##############  for dataset2  ###################
drop table wepon_d2_f5_t1;
create table wepon_d2_f5_t1 as
select user_id,sum(cnt) as mu_all_coupon from
(
  select user_id,1 as cnt from wepon_dataset3
)t
group by user_id;

drop table wepon_d2_f5_t2;
create table wepon_d2_f5_t2 as
select user_id,coupon_id,sum(cnt) as mu_same_coupon from
(
  select user_id,coupon_id,1 as cnt from wepon_dataset3
)t
group by user_id,coupon_id;

drop table wepon_d2_f5_t3;
create table wepon_d2_f5_t3 as
select user_id,date_received,sum(cnt) as du_all_coupon from
(
  select user_id,date_received,1 as cnt from wepon_dataset3
)t
group by user_id,date_received;

drop table wepon_d2_f5_t4;
create table wepon_d2_f5_t4 as
select user_id,coupon_id,date_received,sum(cnt) as du_same_coupon from
(
  select user_id,coupon_id,date_received,1 as cnt from wepon_dataset3
)t
group by user_id,coupon_id,date_received;

drop table wepon_d2_f5_temp;
create table wepon_d2_f5_temp as
select user_id,coupon_id,max(date_received) as max_date_received, min(date_received) as min_date_received from
(
  select a.user_id,a.coupon_id,a.date_received from
  (select user_id,coupon_id,date_received from wepon_dataset3)a
  join
  (select user_id,coupon_id from wepon_d2_f5_t2 where mu_same_coupon>1)b --��ȡ��ͬ���Żݾ��ε�
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;


drop table wepon_d2_f5_t5;
create table wepon_d2_f5_t5 as
select user_id,coupon_id,merchant_id,date_received,
       case when date_received=max_date_received then 1
          when max_date_received is null then -1  -- ֻ��ȡ��һ�ε�
      else 0 end as mu_same_coupon_last,
    case when date_received=min_date_received then 1
          when min_date_received is null then -1  -- ֻ��ȡ��һ�ε�
      else 0 end as mu_same_coupon_first
from
(
  select a.user_id,a.coupon_id,a.merchant_id,a.date_received,b.max_date_received,b.min_date_received
  from wepon_dataset3 a left outer join wepon_d2_f5_temp b
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t;

drop table wepon_d2_f5_t6;
create table wepon_d2_f5_t6 as
select merchant_id,count(*) as label_merchant_user_count from
(
  select distinct merchant_id,user_id from wepon_dataset3
)t
group by merchant_id;

drop table wepon_d2_f5_t7;
create table wepon_d2_f5_t7 as
select user_id,count(*) as label_user_merchant_count from
(
  select distinct merchant_id,user_id from wepon_dataset3
)t
group by user_id;


drop table wepon_d2_f5_t8;
create table wepon_d2_f5_t8 as select merchant_id,count(*) as label_merchant_coupon_count from wepon_dataset3 group by merchant_id;

drop table wepon_d2_f5_t9;
create table wepon_d2_f5_t9 as select merchant_id,count(*) as label_merchant_coupon_type from
(select distinct merchant_id,coupon_id from wepon_dataset3)t
group by merchant_id;

drop table wepon_d2_f5_t10;
create table wepon_d2_f5_t10 as
select user_id,count(*) as label_user_merchant_coupon from
(
  select merchant_id,user_id from wepon_dataset3
)t
group by user_id;


drop table wepon_d2_f5_t11;
create table wepon_d2_f5_t11 as    -- �û��ڴ˴��Ż�ȯ֮����ȡ�˶��ٸ��Ż�ȯ   label_same_coupon_count_later  ��ʵ��ʱ���ȶ�ÿ�조ͬ�û�ͬ�Żݾ�ȥ�أ�
select user_id,coupon_id,date_received,label_same_coupon_count_later-1 as label_same_coupon_count_later from
(
  select user_id,coupon_id,date_received,row_number() over (partition by user_id,coupon_id order by date_received desc) as label_same_coupon_count_later from
  (
    select distinct user_id,coupon_id,date_received from wepon_dataset3
  )t
)tt;


drop table wepon_d2_f5_t12;
create table wepon_d2_f5_t12 as    --�û��ڴ˴��Ż�ȯ֮����ȡ�˶����Ż�ȯ     label_coupon_count_later  ��ʵ��ʱ���ȶ�ÿ�조ͬ�û�ͬ�족ȥ�أ�
select user_id,date_received,label_coupon_count_later-1 as label_coupon_count_later from
(
  select user_id,date_received,row_number() over (partition by user_id order by date_received desc) as label_coupon_count_later from
  (
    select distinct user_id,date_received from wepon_dataset3
  )t
)tt;



-- �ϲ���������
drop table wepon_d2_f5;
create table wepon_d2_f5 as
select u.*,v.label_coupon_count_later from
(
  select s.*,t.label_same_coupon_count_later from
  (
    select q.*,r.label_user_merchant_coupon from
    (
      select o.*,p.label_merchant_coupon_type from
      (
        select m.*,n.label_merchant_coupon_count from
        (
          select k.*,l.label_user_merchant_count from
          (
            select i.*,j.label_merchant_user_count from
            (
            select g.*,h.du_same_coupon from
            (
              select e.*,f.du_all_coupon from
              (
                select c.*,d.mu_same_coupon from
                (
                select a.*,b.mu_all_coupon from
                wepon_d2_f5_t5 a join wepon_d2_f5_t1 b
                on a.user_id=b.user_id
                )c join wepon_d2_f5_t2 d
                on c.user_id=d.user_id and c.coupon_id=d.coupon_id
              )e join wepon_d2_f5_t3 f
              on e.user_id=f.user_id and e.date_received=f.date_received
            )g join wepon_d2_f5_t4 h
            on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
            )i left outer join wepon_d2_f5_t6 j
            on i.merchant_id=j.merchant_id
          )k left outer join wepon_d2_f5_t7 l
          on k.user_id=l.user_id
        )m left outer join wepon_d2_f5_t8 n
        on m.merchant_id=n.merchant_id
      )o left outer join wepon_d2_f5_t9 p
      on o.merchant_id=p.merchant_id
    )q left outer join wepon_d2_f5_t10 r
    on q.user_id=r.user_id
  )s left outer join wepon_d2_f5_t11 t
  on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received
)u  left outer join wepon_d2_f5_t12 v
on u.user_id=v.user_id and u.date_received=v.date_received;




-- ##############  for dataset1  ###################
drop table wepon_d1_f5_t1;
create table wepon_d1_f5_t1 as
select user_id,sum(cnt) as mu_all_coupon from
(
  select user_id,1 as cnt from wepon_dataset3
)t
group by user_id;

drop table wepon_d1_f5_t2;
create table wepon_d1_f5_t2 as
select user_id,coupon_id,sum(cnt) as mu_same_coupon from
(
  select user_id,coupon_id,1 as cnt from wepon_dataset3
)t
group by user_id,coupon_id;

drop table wepon_d1_f5_t3;
create table wepon_d1_f5_t3 as
select user_id,date_received,sum(cnt) as du_all_coupon from
(
  select user_id,date_received,1 as cnt from wepon_dataset3
)t
group by user_id,date_received;

drop table wepon_d1_f5_t4;
create table wepon_d1_f5_t4 as
select user_id,coupon_id,date_received,sum(cnt) as du_same_coupon from
(
  select user_id,coupon_id,date_received,1 as cnt from wepon_dataset3
)t
group by user_id,coupon_id,date_received;

drop table wepon_d1_f5_temp;
create table wepon_d1_f5_temp as
select user_id,coupon_id,max(date_received) as max_date_received, min(date_received) as min_date_received from
(
  select a.user_id,a.coupon_id,a.date_received from
  (select user_id,coupon_id,date_received from wepon_dataset3)a
  join
  (select user_id,coupon_id from wepon_d1_f5_t2 where mu_same_coupon>1)b --��ȡ��ͬ���Żݾ��ε�
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;


drop table wepon_d1_f5_t5;
create table wepon_d1_f5_t5 as
select user_id,coupon_id,merchant_id,date_received,
       case when date_received=max_date_received then 1
          when max_date_received is null then -1  -- ֻ��ȡ��һ�ε�
      else 0 end as mu_same_coupon_last,
    case when date_received=min_date_received then 1
          when min_date_received is null then -1  -- ֻ��ȡ��һ�ε�
      else 0 end as mu_same_coupon_first
from
(
  select a.user_id,a.coupon_id,a.merchant_id,a.date_received,b.max_date_received,b.min_date_received
  from wepon_dataset3 a left outer join wepon_d1_f5_temp b
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t;

drop table wepon_d1_f5_t6;
create table wepon_d1_f5_t6 as
select merchant_id,count(*) as label_merchant_user_count from
(
  select distinct merchant_id,user_id from wepon_dataset3
)t
group by merchant_id;

drop table wepon_d1_f5_t7;
create table wepon_d1_f5_t7 as
select user_id,count(*) as label_user_merchant_count from
(
  select distinct merchant_id,user_id from wepon_dataset3
)t
group by user_id;


drop table wepon_d1_f5_t8;
create table wepon_d1_f5_t8 as select merchant_id,count(*) as label_merchant_coupon_count from wepon_dataset3 group by merchant_id;

drop table wepon_d1_f5_t9;
create table wepon_d1_f5_t9 as select merchant_id,count(*) as label_merchant_coupon_type from
(select distinct merchant_id,coupon_id from wepon_dataset3)t
group by merchant_id;

drop table wepon_d1_f5_t10;
create table wepon_d1_f5_t10 as
select user_id,count(*) as label_user_merchant_coupon from
(
  select merchant_id,user_id from wepon_dataset3
)t
group by user_id;


drop table wepon_d1_f5_t11;
create table wepon_d1_f5_t11 as    -- �û��ڴ˴��Ż�ȯ֮����ȡ�˶��ٸ��Ż�ȯ   label_same_coupon_count_later  ��ʵ��ʱ���ȶ�ÿ�조ͬ�û�ͬ�Żݾ�ȥ�أ�
select user_id,coupon_id,date_received,label_same_coupon_count_later-1 as label_same_coupon_count_later from
(
  select user_id,coupon_id,date_received,row_number() over (partition by user_id,coupon_id order by date_received desc) as label_same_coupon_count_later from
  (
    select distinct user_id,coupon_id,date_received from wepon_dataset3
  )t
)tt;


drop table wepon_d1_f5_t12;
create table wepon_d1_f5_t12 as    --�û��ڴ˴��Ż�ȯ֮����ȡ�˶����Ż�ȯ     label_coupon_count_later  ��ʵ��ʱ���ȶ�ÿ�조ͬ�û�ͬ�족ȥ�أ�
select user_id,date_received,label_coupon_count_later-1 as label_coupon_count_later from
(
  select user_id,date_received,row_number() over (partition by user_id order by date_received desc) as label_coupon_count_later from
  (
    select distinct user_id,date_received from wepon_dataset3
  )t
)tt;



-- �ϲ���������
drop table wepon_d1_f5;
create table wepon_d1_f5 as
select u.*,v.label_coupon_count_later from
(
  select s.*,t.label_same_coupon_count_later from
  (
    select q.*,r.label_user_merchant_coupon from
    (
      select o.*,p.label_merchant_coupon_type from
      (
        select m.*,n.label_merchant_coupon_count from
        (
          select k.*,l.label_user_merchant_count from
          (
            select i.*,j.label_merchant_user_count from
            (
            select g.*,h.du_same_coupon from
            (
              select e.*,f.du_all_coupon from
              (
                select c.*,d.mu_same_coupon from
                (
                select a.*,b.mu_all_coupon from
                wepon_d1_f5_t5 a join wepon_d1_f5_t1 b
                on a.user_id=b.user_id
                )c join wepon_d1_f5_t2 d
                on c.user_id=d.user_id and c.coupon_id=d.coupon_id
              )e join wepon_d1_f5_t3 f
              on e.user_id=f.user_id and e.date_received=f.date_received
            )g join wepon_d1_f5_t4 h
            on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
            )i left outer join wepon_d1_f5_t6 j
            on i.merchant_id=j.merchant_id
          )k left outer join wepon_d1_f5_t7 l
          on k.user_id=l.user_id
        )m left outer join wepon_d1_f5_t8 n
        on m.merchant_id=n.merchant_id
      )o left outer join wepon_d1_f5_t9 p
      on o.merchant_id=p.merchant_id
    )q left outer join wepon_d1_f5_t10 r
    on q.user_id=r.user_id
  )s left outer join wepon_d1_f5_t11 t
  on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received
)u  left outer join wepon_d1_f5_t12 v
on u.user_id=v.user_id and u.date_received=v.date_received;
