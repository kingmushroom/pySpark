from pyspark import SparkConf, SparkContext
con=SparkConf()
sc=SparkContext(conf=con)
rdd=sc.textFile("file:///home/cloudera/Users.txt")
data=rdd.collect()
def breakRecord(rec):
	record=rec.split("|")
	print("Mike", record)
	if(record[2]=='M'):
		return True
	else:	
		return False
Male=rdd.filter(breakRecord).collect()
listofprofessions=rdd.countByValue()
print(listofprofessions)
print(len(Male)," Male's out of ", len(data), " records")

