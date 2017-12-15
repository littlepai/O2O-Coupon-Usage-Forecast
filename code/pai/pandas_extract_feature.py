import pandas as pd
import numpy as np
import cx_Oracle as cx

conn=cx.connect('coupon/coupon@pai_db')

sql_code="select * from wepon_online_feature1"
df=pd.read_sql(sql_code, conn)

# 根据USER_ID分组计算每个用户的记录数
main_user=df.groupby(['USER_ID']).size()
uf=main_user.to_frame('online_action_merchant_count')

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>线上用户特征>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 特征 online_buy_total ：在线购买的总次数
uf=df.groupby('USER_ID')[['COUPON_ID']].count()
uf.rename(columns={"COUPON_ID":"online_buy_total"}, inplace=True) #列名修改
uf['online_buy_total']=df[(df["ACTION"]=="1")].groupby("USER_ID").size()

# 用户线上用优惠券购买的总次数 online_buy_use_coupon
uf['online_buy_use_coupon']=df[(df['COUPON_ID']!="null") & (df["COUPON_ID"]!="fixed") & (df["ACTION"]=="1")].groupby("USER_ID").size()

# 用户线上用 fixed 购买的总次数  online_buy_use_fixed
uf['online_buy_use_fixed']=df[(df["COUPON_ID"]=="fixed")&(df["ACTION"]=="1")].groupby("USER_ID").size()

# 用户线上收到的优惠券的总次数   （包括当前数据期间没领取但以前领取的、重复领取的算多次）
uf['online_coupon_received']=df[(df['COUPON_ID']!="null") & (df["COUPON_ID"]!="fixed")].groupby("USER_ID").size()

# 用户线上有发生购买的merchant个数  online_buy_merchant_count
uf['online_buy_merchant_count']=df[(df['ACTION']=="1")].drop_duplicates(subset=["USER_ID", "MERCHANT_ID"]).groupby("USER_ID").size()

# fillna with zero
uf.fillna(0, inplace=True)

# 用户线上用券+用fixed 总数
uf['online_buy_use_coupon_fixed']=uf["online_buy_use_coupon"]+uf["online_buy_use_fixed"]

# 用户线上用券率
uf['online_buy_use_coupon_rate']=uf.apply(lambda row: -1 if row["online_buy_total"]==0 else row["online_buy_use_coupon"]/row["online_buy_total"], axis=1)

# 线上用户用fixed率
uf['online_buy_use_fixed_rate']=uf.apply(lambda row: -1 if row["online_buy_total"]==0 else row["online_buy_use_fixed"]/row["online_buy_total"], axis=1)

# 用户线上用优惠券或fixed购买率
uf['online_buy_use_coupon_fixed_rate']=uf.apply(lambda row: -1 if row["online_buy_total"]==0 else row["online_buy_use_coupon_fixed"]/row["online_buy_total"], axis=1)

# 优惠券的核销率
uf['online_coupon_transform_rate']=uf.apply(lambda row: -1 if row["online_coupon_received"]==0 else row["online_buy_use_coupon"]/row["online_coupon_received"], axis=1)
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<线上用户特征<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>线下预测窗其他特征>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 线下特征（预测窗）
sql_code_d1_f5="select * from wepon_dataset1"
d1=pd.read_sql(sql_code_d1_f5, conn)

# 预测窗口内用户收到的优惠券总数
d1_f5 = d1.groupby('USER_ID', as_index=False)["DATE_PAY"].count().rename(columns={"DATE_PAY":"this_month_user_receive_all_coupon_count"})

# 预测窗口内用户收到的相同的优惠券数
this_month_user_receive_same_coupon_count=d1.groupby(['USER_ID', 'COUPON_ID'], as_index=False)["DATE_PAY"].count().rename(columns={"DATE_PAY":"this_month_user_receive_same_coupon_count"})


# 预测窗口内一天用户收到的优惠券总数
this_day_user_receive_all_coupon_count=d1.groupby(['USER_ID', 'DATE_RECEIVED'], as_index=False)["DATE_PAY"].count().rename(columns={"DATE_PAY":"this_day_user_receive_all_coupon_count"})


# 预测窗口内一天用户收到的相同的优惠券数
this_day_user_receive_same_coupon_count=d1.groupby(['USER_ID', 'COUPON_ID', 'DATE_RECEIVED'], as_index=False)["DATE_PAY"].count().rename(columns={"DATE_PAY":"this_day_user_receive_same_coupon_count"})


# 该优惠券是否是当月的第一张、最后一张（只有一张的为-1）
this_month_user_receive_same_coupon_lastone_or_firstone=d1.groupby(['USER_ID', 'COUPON_ID'], as_index=False)["DATE_RECEIVED"].agg(["max", "min"])
this_month_user_receive_same_coupon_lastone_or_firstone.rename(columns={"max":"this_month_user_receive_same_coupon_lastone","min":"this_month_user_receive_same_coupon_firstone"}, inplace=True)

this_month_user_receive_same_coupon_lastone_or_firstone=this_month_user_receive_same_coupon_lastone_or_firstone.reset_index(['USER_ID', 'COUPON_ID']).merge(this_month_user_receive_same_coupon_count, on=['USER_ID', 'COUPON_ID'])

this_month_user_receive_same_coupon_lastone_or_firstone=d1.merge(this_month_user_receive_same_coupon_lastone_or_firstone, on=['USER_ID', 'COUPON_ID'])

this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_firstone']=np.where(this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_count'] == 1, -1, this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_firstone'])


this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_lastone']=np.where(this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_count'] == 1, -1, this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_lastone'])

this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_lastone']=np.where(this_month_user_receive_same_coupon_lastone_or_firstone['DATE_RECEIVED'] == this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_lastone'],1, 0)

this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_firstone']=np.where(this_month_user_receive_same_coupon_lastone_or_firstone['DATE_RECEIVED'] == this_month_user_receive_same_coupon_lastone_or_firstone['this_month_user_receive_same_coupon_firstone'],1, 0)


this_month_user_receive_same_coupon_lastone_or_firstone=this_month_user_receive_same_coupon_lastone_or_firstone[['USER_ID','COUPON_ID','MERCHANT_ID','DATE_RECEIVED','this_month_user_receive_same_coupon_lastone','this_month_user_receive_same_coupon_firstone']]

# 商家有交集的用户数目
label_merchant_user_count=d1.drop_duplicates(subset=["MERCHANT_ID","USER_ID"]).groupby(["MERCHANT_ID"], as_index=False)["DATE_PAY"].count()
label_merchant_user_count.rename(columns={"DATE_PAY":"label_merchant_user_count"}, inplace=True)

# 用户有交集的商家数目
label_user_merchant_count=d1.drop_duplicates(subset=["MERCHANT_ID","USER_ID"]).groupby(["USER_ID"], as_index=False)["DATE_PAY"].count()
label_user_merchant_count.rename(columns={"DATE_PAY":"label_user_merchant_count"}, inplace=True)

# 商家发出的所有优惠券数目
label_merchant_coupon_count=d1.groupby(["MERCHANT_ID"], as_index=False)["DATE_PAY"].count()
label_merchant_coupon_count.rename(columns={"DATE_PAY":"label_merchant_coupon_count"}, inplace=True)

# 商家发出的所有优惠券种类数目
label_merchant_coupon_type_count=d1.drop_duplicates(subset=["MERCHANT_ID","COUPON_ID"]).groupby("MERCHANT_ID", as_index=False)["DATE_PAY"].count()
label_merchant_coupon_type_count.rename(columns={"DATE_PAY":"label_merchant_coupon_type_count"}, inplace=True)

# 用户领取该商家的所有优惠券数目
label_user_merchant_coupon_count=d1.groupby("USER_ID", as_index=False)["DATE_PAY"].count()
label_user_merchant_coupon_count.rename(columns={"DATE_PAY":"label_user_merchant_coupon_count"}, inplace=True)

# 用户在此次优惠券之后还领取了多少该优惠券
label_same_coupon_count_later=d1.drop_duplicates(subset=["USER_ID", "COUPON_ID", "DATE_RECEIVED"])[["USER_ID","COUPON_ID","DATE_RECEIVED"]]
label_same_coupon_count_later["DATE_RECEIVED_DT"]=pd.to_datetime(label_same_coupon_count_later["DATE_RECEIVED"],format="%Y%m%d")
label_same_coupon_count_later['label_same_coupon_count_later']=label_same_coupon_count_later.groupby(["USER_ID","COUPON_ID"])["DATE_RECEIVED_DT"].rank(ascending=0, method='first')-1
del label_same_coupon_count_later["DATE_RECEIVED_DT"]

# 用户在此次优惠券之后还领取了多少优惠券
label_coupon_count_later=d1.drop_duplicates(subset=["USER_ID", "DATE_RECEIVED"])[["USER_ID","DATE_RECEIVED"]]
label_coupon_count_later["DATE_RECEIVED_DT"]=pd.to_datetime(label_coupon_count_later["DATE_RECEIVED"],format="%Y%m%d")
label_coupon_count_later['label_coupon_count_later']=label_coupon_count_later.groupby(["USER_ID"])["DATE_RECEIVED_DT"].rank(ascending=0, method='first')-1
del label_coupon_count_later["DATE_RECEIVED_DT"]


# 合并
d1_f5=this_month_user_receive_same_coupon_lastone_or_firstone.merge(d1_f5, on=["USER_ID"])
d1_f5=d1_f5.merge(this_month_user_receive_same_coupon_count, on=["USER_ID","COUPON_ID"])
d1_f5=d1_f5.merge(this_day_user_receive_all_coupon_count, on=["USER_ID","DATE_RECEIVED"])
d1_f5=d1_f5.merge(this_day_user_receive_same_coupon_count, on=['USER_ID', 'COUPON_ID', 'DATE_RECEIVED'])
d1_f5=d1_f5.merge(label_merchant_user_count, on=["MERCHANT_ID"])
d1_f5=d1_f5.merge(label_user_merchant_count, on=["USER_ID"])
d1_f5=d1_f5.merge(label_merchant_coupon_count, on=["MERCHANT_ID"])
d1_f5=d1_f5.merge(label_merchant_coupon_type_count, on=["MERCHANT_ID"])
d1_f5=d1_f5.merge(label_user_merchant_coupon_count, on=["USER_ID"])
d1_f5=d1_f5.merge(label_same_coupon_count_later, on=["USER_ID", "COUPON_ID", "DATE_RECEIVED"])
d1_f5=d1_f5.merge(label_coupon_count_later, on=["USER_ID", "DATE_RECEIVED"])

#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<线下预测窗其他特征<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<




