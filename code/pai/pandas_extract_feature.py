import pandas as pd
import cx_Oracle as cx

conn=cx.connect('coupon/coupon@pai_db')

sql_code="select * from wepon_online_feature1"
df=pd.read_sql(sql_code, conn)

# 根据USER_ID分组计算每个用户的记录数
main_user=df.groupby(['USER_ID']).size()
uf=main_user.to_frame('online_action_merchant_count')

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


# 线下特征（label窗）
sql_code_d1_f5="select * from wepon_dataset1"
d1=pd.read_sql(sql_code_d1_f5, conn)

# 预测窗口内用户收到的优惠券总数
d1_f5 = d1.groupby('USER_ID').size().to_frame(name='this_month_user_receive_all_coupon_count')

# 预测窗口内用户收到的相同的优惠券数
d1.groupby(['USER_ID', 'COUPON_ID']).size()

# 预测窗口内一天用户收到的优惠券总数
d1.groupby(['USER_ID', 'COUPON_ID', 'DATE_RECEIVED']).size()

