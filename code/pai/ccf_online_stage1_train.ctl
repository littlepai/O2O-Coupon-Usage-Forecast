Load data
characterset AL32UTF8
infile './ccf_online_stage1_train.csv'
TRUNCATE
into table TRAIN_ONLINE_STAGE2
FIELDS TERMINATED BY ','
TRAILING NULLCOLS
(
user_id         "trim(:user_id)",
merchant_id     "trim(:merchant_id)",
action          "trim(:action)",
coupon_id       "trim(:coupon_id)",
discount_rate   "trim(:discount_rate)",
date_received   "trim(:date_received)",
date_pay            "trim(:date_pay)"
)
