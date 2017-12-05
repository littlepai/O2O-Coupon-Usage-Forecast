Load data
characterset AL32UTF8
infile './ccf_offline_stage1_test_revised.csv'
TRUNCATE
into table prediction_stage2
FIELDS TERMINATED BY ','
TRAILING NULLCOLS
(
user_id         "trim(:user_id)",
merchant_id     "trim(:merchant_id)",
coupon_id       "trim(:coupon_id)",
discount_rate   "trim(:discount_rate)",
distance        "trim(:distance)",
date_received   "trim(:date_received)"
)
