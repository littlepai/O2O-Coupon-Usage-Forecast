echo off

echo 加载线上训练数据
sqlldr coupon/coupon@pai_db control=ccf_online_stage1_train.ctl log=ccf_online_stage1_train.log bad=ccf_online_stage1_train.bad rows=100000 skip=1 bindsize=900000000 readsize=900000000
echo 线上训练数据加载完成
echo.
echo.


echo 加载线下训练数据
sqlldr coupon/coupon@pai_db control=ccf_offline_stage1_train.ctl log=ccf_offline_stage1_train.log bad=ccf_offline_stage1_train.bad rows=100000 skip=1 bindsize=900000000 readsize=900000000
echo 线下训练数据加载完成
echo.
echo.


echo 加载线下预测数据
sqlldr coupon/coupon@pai_db control=ccf_offline_stage1_test_revised.ctl log=ccf_offline_stage1_test_revised.log bad=ccf_offline_stage1_test_revised.bad rows=100000 skip=1 bindsize=900000000 readsize=900000000
echo 线下预测数据加载完成
echo.
echo.

