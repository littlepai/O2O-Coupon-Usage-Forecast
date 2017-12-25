import pandas as pd
import numpy as np
import math
import tensorflow as tf
from tensorflow.python.training import moving_averages
import cx_Oracle as cx
import smote

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

tf.app.flags.DEFINE_string('mode', 'train', "模式，可选训练train，其他")
tf.app.flags.DEFINE_float('beta1', 0.9, "AdamOptimizer 参数")
tf.app.flags.DEFINE_float('beta2', 0.999, "AdamOptimizer 参数")
tf.app.flags.DEFINE_float('learning_rate', 1e-5, "学习率")
tf.app.flags.DEFINE_integer('batch_size', 128, "最小批大小")

FLAGS=tf.app.flags.FLAGS

class O2Omodel():
	def __init__(self):
		self.inputs = tf.placeholder(tf.float32, [None, 67])
		self.labels = tf.placeholder(tf.float32, [None, 2])
		self.mode=FLAGS.mode
		self._extra_train_ops=[]
	
	def build_graph(self):
		self._build_model()
		self._build_train_op()
		
	
	def _build_model(self):
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
		
		x=self._batch_norm('bn1', self.inputs)
		x=tf.matmul(x, w['w1'])+b['b1']
		x=self._leaky_relu(x, 0.01)
		x=tf.nn.dropout(x, keep_prob=0.75)
		
		x=self._batch_norm('bn2', x)
		x=tf.matmul(x, w['w2'])+b['b2']
		x=self._leaky_relu(x, 0.01)
		x=tf.nn.dropout(x, keep_prob=0.75)
		
		x=self._batch_norm('bn3', x)
		x=tf.matmul(x, w['w3'])+b['b3']
		x=self._leaky_relu(x, 0.01)
		x=tf.nn.dropout(x, keep_prob=0.75)
		
		x=self._batch_norm('bn4', x)
		self.x=tf.matmul(x, w['w4'])+b['b4']
		
		
		
	def _build_train_op(self):
		self.output = tf.nn.softmax(self.x)
		self.pred=tf.argmax(self.output, axis=1)
		self.loss=tf.nn.softmax_cross_entropy_with_logits(logits=self.x, labels=self.labels) #这里的logits参数要接收“未经过激活函数的输出”
		self.cost=tf.reduce_mean(self.loss)
		
		self.global_step = tf.Variable(0, trainable=False)
		self.optimizer = tf.train.AdamOptimizer(learning_rate=FLAGS.learning_rate,beta1=FLAGS.learning_rate,beta2=FLAGS.learning_rate).minimize(self.loss,global_step=self.global_step)
		self.train_ops = [self.global_step, self.cost, self.optimizer] + self._extra_train_ops
		self.train_op = tf.group(*self.train_ops)
	
	def _batch_norm(self, name, x):
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
			if self.mode == 'train':
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
				self._extra_train_ops.append(moving_averages.assign_moving_average(
					moving_mean, mean, 0.9))
				self._extra_train_ops.append(moving_averages.assign_moving_average(
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

	def _max_pool(self, x, ksize=2, strides=1):
		return tf.nn.max_pool(x,ksize=[1, ksize, ksize, 1],strides=[1, strides, strides, 1],padding='SAME',name='max_pool')

	def _leaky_relu(self, x, leakiness=0.0):
		return tf.where(tf.less(x, 0.0), leakiness * x, x, name='leaky_relu')

model=O2Omodel()
model.build_graph()

class DataIterator():
	def __init__(self, sql_code='select * from wepon_d1', smote_pro=True, smote_k=5):
		conn=cx.connect('coupon/coupon@pai_db')
		self.sql_code=sql_code
		df=pd.read_sql(sql_code, conn)
		X1=df[x_columns]
		y1=df[target]
		s_label_count=[(i,y1[y1==i].count()) for i in y1.unique()]
		y1=np.asarray(y1)

		X1=X1.applymap(lambda x: 0 if x is None else x)
		X1=X1.applymap(lambda x: 0 if np.isnan(x) else x)
		X1=X1.applymap(lambda x: x*1.0)
		datas=np.asanyarray(X1, np.float32)

		if smote_pro:
			s_label_count.sort(key=lambda x:x[1], reverse=True)
			_, max_label_count=s_label_count[0]
			for s_label, s_count in s_label_count[1:]:
				s=smote.Smote(N=math.ceil(max_label_count/s_count), k=smote_k)
				smotedata=s.fit_transform(np.asarray(datas[y1==s_label]))
				datas=np.vstack([datas,smotedata[:max_label_count-s_count]])
				add_y1=np.zeros(max_label_count-s_count, dtype='int32')
				add_y1[:]=s_label
				y1=np.hstack([y1, add_y1])
		
		self.datas=datas
		self.labels=np.zeros([len(y1), 2])
		self.labels[:,0]=np.where(y1==0,1,0)
		self.labels[:,1]=np.where(y1==1,1,0)

	@property
	def size(self):
		return len(self.labels)

	
	def get_data_by_indexs(self, indexs):
		return self.datas[indexs], self.labels[indexs]

	def gen_shuffle_batch(self, batch_size):
		while(True):
			self.shuffle_idx=np.random.permutation(self.size)
			self.total_epoch=math.ceil(self.size/batch_size)
			for i in range(self.total_epoch):
				yield self.get_data_by_indexs(self.shuffle_idx[i*batch_size: (i+1)*batch_size if (i+1)*batch_size < self.size else self.size])

def main(_):
	sess=tf.Session()
	init=tf.global_variables_initializer()
	sess.run(init)
	data=DataIterator()
	gen=data.gen_shuffle_batch(FLAGS.batch_size)
	for i in range(100000):
		inputs, labels=next(gen)
		global_step, cost, *_ =sess.run(model.train_ops, feed_dict={model.inputs:inputs, model.labels:labels})
		if global_step%100==0:
			print(global_step, "------", cost)

if __name__ == "__main__":
	tf.app.run()

#sess.run(model.train_op, feed_dict={model.inputs:X, model.labels:label})
#sess.run(model.loss, feed_dict={model.inputs:X, model.labels:label})