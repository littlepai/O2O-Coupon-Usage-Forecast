--drop 线上训练数据表
drop table train_online_stage2;

--create 线上训练数据表
create table train_online_stage2
(
  user_id varchar2(100),
  merchant_id varchar2(100),
  action varchar2(100),
  coupon_id varchar2(100),
  discount_rate varchar2(100),
  date_received varchar2(100),
  date_pay varchar2(100)
);

comment on column train_online_stage2.user_id is '用户ID';
comment on column train_online_stage2.merchant_id is '商户ID';
comment on column train_online_stage2.action is '0 点击， 1购买，2领取优惠券 ';
comment on column train_online_stage2.coupon_id is '优惠券ID：null表示无优惠券消费，此时Discount_rate和Date_received字段无意义。“fixed”表示该交易是限时低价活动。';
comment on column train_online_stage2.discount_rate is '优惠率：x \in [0,1]代表折扣率；x:y表示满x减y；“fixed”表示低价限时优惠； ';
comment on column train_online_stage2.date_received is '领取优惠券日期 ';
comment on column train_online_stage2.date_pay is '消费日期：如果Date=null & Coupon_id != null，该记录表示领取优惠券但没有使用；如果Date!=null & Coupon_id = null，则表示普通消费日期；如果Date!=null & Coupon_id != null，则表示用优惠券消费日期；';

--drop 线下训练数据表
drop table train_offline_stage2;

--create 线下训练数据表
create table train_offline_stage2
(
  user_id varchar2(100),
  merchant_id varchar2(100),
  coupon_id varchar2(100),
  discount_rate varchar2(100),
  distance varchar2(100),
  date_received varchar2(100),
  date_pay varchar2(100)
);

comment on column train_offline_stage2.user_id is '用户ID';
comment on column train_offline_stage2.merchant_id is '商户ID';
comment on column train_offline_stage2.coupon_id is '优惠券ID：null表示无优惠券消费，此时Discount_rate和Date_received字段无意义。“fixed”表示该交易是限时低价活动。';
comment on column train_offline_stage2.discount_rate is '优惠率：x \in [0,1]代表折扣率；x:y表示满x减y；“fixed”表示低价限时优惠； ';
comment on column train_offline_stage2.distance is 'user经常活动的地点离该merchant的最近门店距离是x*500米（如果是连锁店，则取最近的一家门店），x\in[0,10]；null表示无此信息，0表示低于500米，10表示大于5公里； ';
comment on column train_offline_stage2.date_received is '领取优惠券日期 ';
comment on column train_offline_stage2.date_pay is '消费日期：如果Date=null & Coupon_id != null，该记录表示领取优惠券但没有使用；如果Date!=null & Coupon_id = null，则表示普通消费日期；如果Date!=null & Coupon_id != null，则表示用优惠券消费日期；';


--drop 线下预测数据表
drop table prediction_stage2;

--create 线下预测数据表
create table prediction_stage2
(
  user_id varchar2(100),
  merchant_id varchar2(100),
  coupon_id varchar2(100),
  discount_rate varchar2(100),
  distance varchar2(100),
  date_received varchar2(100)
);

comment on column train_offline_stage2.user_id is '用户ID';
comment on column train_offline_stage2.merchant_id is '商户ID';
comment on column train_offline_stage2.coupon_id is '优惠券ID：null表示无优惠券消费，此时Discount_rate和Date_received字段无意义。“fixed”表示该交易是限时低价活动。';
comment on column train_offline_stage2.discount_rate is '优惠率：x \in [0,1]代表折扣率；x:y表示满x减y；“fixed”表示低价限时优惠； ';
comment on column train_offline_stage2.distance is 'user经常活动的地点离该merchant的最近门店距离是x*500米（如果是连锁店，则取最近的一家门店），x\in[0,10]；null表示无此信息，0表示低于500米，10表示大于5公里； ';
comment on column train_offline_stage2.date_received is '领取优惠券日期 ';
