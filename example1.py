from pyspark import SparkConf, SparkContext
con=SparkConf()
sc=SparkContext(conf=con)
list1=[1,2,3,45,6]
rdd=sc.parallelize(list1)
data=rdd.collect()

def mike(x):
	Message= "Mike is " +str(x) + " times awesomer than you"
	return(Message)

rdd2=rdd.map(mike)
moreData=rdd2.collect()
for a in moreData:
	print(a)


