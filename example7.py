from pyspark import SparkConf, SparkContext

con=SparkConf()
sc=SparkContext(conf=con)
rdd=sc.parallelize([6,66,666,6666,66666,4])
rdd2=sc.parallelize({"Mike":"4","tom":"3"})
#rdd3=rdd2.mapByValue(lambda x: x+2)
data=rdd2.collect()
print(data[1])
def sum(a,b):
	return a+b
def max(a,b):
	c=c+1
	print(c)
	if a>b:
		return a
	else:
		return b

mike=rdd.reduce(sum)/rdd.count()
print(mike)

