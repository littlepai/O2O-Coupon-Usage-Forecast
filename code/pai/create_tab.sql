--drop ����ѵ�����ݱ�
drop table train_online_stage2;

--create ����ѵ�����ݱ�
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

comment on column train_online_stage2.user_id is '�û�ID';
comment on column train_online_stage2.merchant_id is '�̻�ID';
comment on column train_online_stage2.action is '0 ����� 1����2��ȡ�Ż�ȯ ';
comment on column train_online_stage2.coupon_id is '�Ż�ȯID��null��ʾ���Ż�ȯ���ѣ���ʱDiscount_rate��Date_received�ֶ������塣��fixed����ʾ�ý�������ʱ�ͼۻ��';
comment on column train_online_stage2.discount_rate is '�Ż��ʣ�x \in [0,1]�����ۿ��ʣ�x:y��ʾ��x��y����fixed����ʾ�ͼ���ʱ�Żݣ� ';
comment on column train_online_stage2.date_received is '��ȡ�Ż�ȯ���� ';
comment on column train_online_stage2.date_pay is '�������ڣ����Date=null & Coupon_id != null���ü�¼��ʾ��ȡ�Ż�ȯ��û��ʹ�ã����Date!=null & Coupon_id = null�����ʾ��ͨ�������ڣ����Date!=null & Coupon_id != null�����ʾ���Ż�ȯ�������ڣ�';

--drop ����ѵ�����ݱ�
drop table train_offline_stage2;

--create ����ѵ�����ݱ�
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

comment on column train_offline_stage2.user_id is '�û�ID';
comment on column train_offline_stage2.merchant_id is '�̻�ID';
comment on column train_offline_stage2.coupon_id is '�Ż�ȯID��null��ʾ���Ż�ȯ���ѣ���ʱDiscount_rate��Date_received�ֶ������塣��fixed����ʾ�ý�������ʱ�ͼۻ��';
comment on column train_offline_stage2.discount_rate is '�Ż��ʣ�x \in [0,1]�����ۿ��ʣ�x:y��ʾ��x��y����fixed����ʾ�ͼ���ʱ�Żݣ� ';
comment on column train_offline_stage2.distance is 'user������ĵص����merchant������ŵ������x*500�ף�����������꣬��ȡ�����һ���ŵ꣩��x\in[0,10]��null��ʾ�޴���Ϣ��0��ʾ����500�ף�10��ʾ����5��� ';
comment on column train_offline_stage2.date_received is '��ȡ�Ż�ȯ���� ';
comment on column train_offline_stage2.date_pay is '�������ڣ����Date=null & Coupon_id != null���ü�¼��ʾ��ȡ�Ż�ȯ��û��ʹ�ã����Date!=null & Coupon_id = null�����ʾ��ͨ�������ڣ����Date!=null & Coupon_id != null�����ʾ���Ż�ȯ�������ڣ�';


--drop ����Ԥ�����ݱ�
drop table prediction_stage2;

--create ����Ԥ�����ݱ�
create table prediction_stage2
(
  user_id varchar2(100),
  merchant_id varchar2(100),
  coupon_id varchar2(100),
  discount_rate varchar2(100),
  distance varchar2(100),
  date_received varchar2(100)
);

comment on column train_offline_stage2.user_id is '�û�ID';
comment on column train_offline_stage2.merchant_id is '�̻�ID';
comment on column train_offline_stage2.coupon_id is '�Ż�ȯID��null��ʾ���Ż�ȯ���ѣ���ʱDiscount_rate��Date_received�ֶ������塣��fixed����ʾ�ý�������ʱ�ͼۻ��';
comment on column train_offline_stage2.discount_rate is '�Ż��ʣ�x \in [0,1]�����ۿ��ʣ�x:y��ʾ��x��y����fixed����ʾ�ͼ���ʱ�Żݣ� ';
comment on column train_offline_stage2.distance is 'user������ĵص����merchant������ŵ������x*500�ף�����������꣬��ȡ�����һ���ŵ꣩��x\in[0,10]��null��ʾ�޴���Ϣ��0��ʾ����500�ף�10��ʾ����5��� ';
comment on column train_offline_stage2.date_received is '��ȡ�Ż�ȯ���� ';
