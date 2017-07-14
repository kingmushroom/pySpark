from pyspark import SparkConf, SparkContext
con=SparkConf()
sc=SparkContext(conf=con)
list1=[1,2,3,3,4,2,4,5,5,6,6,6]
rdd=sc.parallelize(list1)
data=rdd.countByValue()
print(data)
for k in data:
	print(k, "-" , data[k])
rdd2=rdd.filter(lambda x: x>3)
print(rdd.map(lambda x:x**2).filter(lambda x:x%2 ==0).collect())
print("MIKE")
thisanswer= rdd2.collect()
print thisanswer


def mike(x):
	Message= "Mike is " +str(x) + " times awesomer than you"
	return(Message)

rdd2=rdd.map(mike)
moreData=rdd2.collect()
for a in moreData:
	print(a)


