from pyspark import SparkConf, SparkContext
con=SparkConf()
sc=SparkContext(conf=con)
dic1=[1,2,3]
dic2=[5]
rdd=sc.parallelize(dic1)
rdd2=sc.parallelize(dic2)
rdd3=rdd.union(rdd2)
def toWords(x):
	abc={1:"One",2:"Two",3:"Three",4:"Four"}
	return(abc[x])
x2=rdd.map(toWords)
thisData=x2.collect()
print(thisData)
