import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.python.training import moving_averages
import cx_Oracle as cx

x_columns = ['DAYS_DISTANCE',
 'DAY_OF_MONTH',
 'IS_MAN_JIAN',
 'DISTANCE',
 'DISCOUNT_MAN',
 'DISCOUNT_JIAN',
 'DISCOUNT_RATE',
 'COUPON_COUNT',
 'LABEL_COUPON_FEATURE_REC',
 'LABEL_COUPON_FEATURE_BUY_COUNT',
 'LABEL_COUPON_FEATURE_RATE',
 'DISTINCT_COUPON_COUNT',
 'MERCHANT_AVG_DISTANCE',
 'MERCHANT_MEDIAN_DISTANCE',
 'MERCHANT_MAX_DISTANCE',
 'MERCHANT_USER_BUY_COUNT',
 'MERCHANT_MIN_DISTANCE',
 'SALES_USE_COUPON',
 'TRANSFORM_RATE',
 'COUPON_RATE',
 'TOTAL_COUPON',
 'TOTAL_SALES',
 'USER_AVG_DISTANCE',
 'USER_MIN_DISTANCE',
 'USER_MAX_DISTANCE',
 'USER_MEDIAN_DISTANCE',
 'AVG_DIFF_DATE_DATERECEIVED',
 'MIN_DIFF_DATE_DATERECEIVED',
 'MAX_DIFF_DATE_DATERECEIVED',
 'BUY_USE_COUPON',
 'BUY_USE_COUPON_RATE',
 'USER_COUPON_TRANSFORM_RATE',
 'COUNT_MERCHANT',
 'BUY_TOTAL',
 'COUPON_RECEIVED',
 'USER_MERCHANT_BUY_TOTAL',
 'USER_MERCHANT_RECEIVED',
 'USER_MERCHANT_ANY',
 'USER_MERCHANT_BUY_USE_COUPON',
 'USER_MERCHANT_BUY_COMMON',
 'COUPON_TRANSFORM_RATE',
 'USER_MERCHANT_COUPON_BUY_RATE',
 'USER_MERCHANT_COMMON_BUY_RATE',
 'USER_MERCHANT_RATE',
 'MU_ALL_COUPON',
 'MU_SAME_COUPON',
 'DU_ALL_COUPON',
 'DU_SAME_COUPON',
 'MU_SAME_COUPON_LAST',
 'MU_SAME_COUPON_FIRST',
 'LABEL_MERCHANT_USER_COUNT',
 'LABEL_USER_MERCHANT_COUNT',
 'LABEL_MERCHANT_COUPON_COUNT',
 'LABEL_MERCHANT_COUPON_TYPE',
 'LABEL_USER_MERCHANT_COUPON',
 'LABEL_SAME_COUPON_COUNT_LATER',
 'LABEL_COUPON_COUNT_LATER',
 'LABEL_USER_COUPON_FEATURE_REC',
 'LABEL_USER_CP_FEATURE_BUY',
 'LABEL_USER_COUPON_FEATURE_RATE',
 'WEEKDAY1',
 'WEEKDAY2',
 'WEEKDAY3',
 'WEEKDAY4',
 'WEEKDAY5',
 'WEEKDAY6',
 'WEEKDAY7']
target='LABEL'


mode='train'
_extra_train_ops=[]

def _batch_norm(name, x):
        """批标准化."""
        with tf.variable_scope(name):
            params_shape = [x.get_shape()[-1]] #获取tensor的最后一个维度，后面的均值，方差都是这个维度
            
            # 标准化数据为均值为0方差为1之后，还有一个x=x*gamma+beta的调整
            # 这个会随着训练不断调整
            beta = tf.get_variable(
                'beta', params_shape, tf.float32,
                initializer=tf.constant_initializer(0.0, tf.float32))
            gamma = tf.get_variable(
                'gamma', params_shape, tf.float32,
                initializer=tf.constant_initializer(1.0, tf.float32))
            
            # 训练的时候不断调整平滑均值，平滑方差
            # 预测的时候，回复权重使用的是训练过程中调整出来的平滑方差均值去做标准化
            if mode == 'train':
                mean, variance = tf.nn.moments(x, [0], name='moments') #获取批均值和方差，size[最后一个维度]
                
                # moving_mean, moving_variance 这两个name一定要让训练和预测的时候都相等，不然就没法恢复训练好的值了
                moving_mean = tf.get_variable(
                    'moving_mean', params_shape, tf.float32,
                    initializer=tf.constant_initializer(0.0, tf.float32),
                    trainable=False)
                moving_variance = tf.get_variable(
                    'moving_variance', params_shape, tf.float32,
                    initializer=tf.constant_initializer(1.0, tf.float32),
                    trainable=False)

                _extra_train_ops.append(moving_averages.assign_moving_average(
                    moving_mean, mean, 0.9))
                _extra_train_ops.append(moving_averages.assign_moving_average(
                    moving_variance, variance, 0.9))
            else:
                # mean的name一定要跟train的时候的一样 moving_mean, 这样在restore的时候才可以加载训练的时候的值
                mean = tf.get_variable(
                    'moving_mean', params_shape, tf.float32,
                    initializer=tf.constant_initializer(0.0, tf.float32),
                    trainable=False)
                # variance的name一定要跟train的时候的一样 moving_variance
                variance = tf.get_variable(
                    'moving_variance', params_shape, tf.float32,
                    initializer=tf.constant_initializer(1.0, tf.float32),
                    trainable=False)
                
                # 可视化
                # tf.summary.histogram(mean.op.name, mean)
                # tf.summary.histogram(variance.op.name, variance)
            
            # 计算，标准化，最后一个值为误差，一般设置很小即可
            x_bn = tf.nn.batch_normalization(x, mean, variance, beta, gamma, 0.001)
            x_bn.set_shape(x.get_shape())

            return x_bn

def _max_pool(x, ksize=2, strides=1):
    return tf.nn.max_pool(x,ksize=[1, ksize, ksize, 1],strides=[1, strides, strides, 1],padding='SAME',name='max_pool')

def _leaky_relu(x, leakiness=0.0):
    return tf.where(tf.less(x, 0.0), leakiness * x, x, name='leaky_relu')



inputs = tf.placeholder(tf.float32, [None, 67])
y = tf.placeholder(tf.float32, [None, 2])


w={
'w1':tf.get_variable('w1', shape=(67, 64), dtype=tf.float32, initializer=tf.contrib.layers.xavier_initializer()),
'w2':tf.get_variable('w2', shape=(64, 128), dtype=tf.float32, initializer=tf.contrib.layers.xavier_initializer()),
'w3':tf.get_variable('w3', shape=(128, 32), dtype=tf.float32, initializer=tf.contrib.layers.xavier_initializer()),
'w4':tf.get_variable('w4', shape=(32, 2), dtype=tf.float32, initializer=tf.contrib.layers.xavier_initializer())
}

b={
'b1':tf.get_variable('b1', shape=(64), dtype=tf.float32, initializer=tf.contrib.layers.xavier_initializer()),
'b2':tf.get_variable('b2', shape=(128), dtype=tf.float32, initializer=tf.contrib.layers.xavier_initializer()),
'b3':tf.get_variable('b3', shape=(32), dtype=tf.float32, initializer=tf.contrib.layers.xavier_initializer()),
'b4':tf.get_variable('b4', shape=(2), dtype=tf.float32, initializer=tf.contrib.layers.xavier_initializer())
}

x=tf.matmul(inputs, w['w1'])+b['b1']
x=_leaky_relu(x, 0.01)
x=tf.matmul(x, w['w2'])+b['b2']
x=_leaky_relu(x, 0.01)
x=tf.matmul(x, w['w3'])+b['b3']
x=_leaky_relu(x, 0.01)
x=tf.matmul(x, w['w4'])+b['b4']
output = tf.nn.softmax(x)
pred=tf.argmax(output, axis=1)
loss=tf.nn.softmax_cross_entropy_with_logits(logits=x, labels=y)
cost=tf.reduce_mean(loss)

conn=cx.connect('coupon/coupon@pai_db')
sql_code1='select * from wepon_d1'
df1=pd.read_sql(sql_code1, conn)
X1=df1[x_columns]
y1=df1[target]

X1=X1.applymap(lambda x: 0 if x is None else x)
X1=X1.applymap(lambda x: 0 if np.isnan(x) else x)

X1=X1.applymap(lambda x: x*1.0)
X=np.asanyarray(X1, np.float32)
label=np.zeros([len(y1), 2])
label[:,0]=np.where(y1==0,1,0)
label[:,1]=np.where(y1==1,1,0)


global_step = tf.Variable(0, trainable=False)
optimizer = tf.train.AdamOptimizer(learning_rate=1e-5,beta1=0.9,beta2=0.999).minimize(loss,global_step=global_step)
train_ops = [global_step, cost, optimizer] + _extra_train_ops
train_op = tf.group(*train_ops)

sess=tf.Session()
init=tf.global_variables_initializer()
sess.run(init)
#sess.run(train_op, feed_dict={inputs:X, y:label})
#sess.run(loss, feed_dict={inputs:X, y:label})