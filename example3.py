from pyspark import SparkConf, SparkContext
con=SparkConf()
sc=SparkContext(conf=con)
bob = ""
rdd=sc.parallelize(bob)
data=rdd.collect()
print(data)
