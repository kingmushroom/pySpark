from pyspark import SparkConf, SparkContext
con=SparkConf()
sc=SparkContext(conf=con)
list1=[1,2,3,45,6]
list2=[6,7,8,3,9]
rdd=sc.parallelize(list1)
rdd2=sc.parallelize(list2)

print(rdd.union(rdd2).collect())
print(rdd.intersection(rdd2).collect())
print(rdd.subtract(rdd2).collect())

rdd.saveAsTextFile("mike.txt")
