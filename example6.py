from pyspark import SparkConf, SparkContext

con=SparkConf()
sc=SparkContext(conf=con)
rdd=sc.textFile("file:///home/cloudera/Users.txt")
data=rdd.collect()

def breakRecord(rec):
	record=rec.split("|")
	return(record[3])
	
def breakRecord2(rec):
	record=rec.split("|")
	if("Occopation" in record[3]):
		return False
	return(record[2],record[3])
rdd2=rdd.filter(lambda x:"Occupation" not in x)
data=rdd2.map(breakRecord2).countByValue()



print(data)

for a in data:
	print(a,"-",data[a])

data=str(data).replace(',','\n')
print(data)
