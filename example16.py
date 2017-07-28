from pyspark import SparkConf, SparkContext,SQLContext
import pyspark.sql
from pyspark.sql.types import *

con=SparkConf()
sc=SparkContext(conf=con)
rdd=sc.textFile("File:///home/cloudera/employees.txt")
rdd=rdd.map(lambda x: x.split('|'))



header=rdd.first()
rdd=rdd.filter(lambda x:x!=header)



sql=SQLContext(sc)
df=sql.createDataFrame(rdd,("mik","mike","mikk"))
df.show()
df.printSchema()
df.select("mike").show()

df2 = rdd.toDF(['RegNo','Subject','Marks'])
df2.show() 
newNames=['mike','mike','mike']
#df=df.toDF(newNames:_*)
#df.take(1)
#df.show()
#df2.select("Marks").show()
#df2.groupBy("Subject").count().show()
myList = df2.select("Marks").collect()
print(myList)
df2.filter(df2.Marks>75).select((df2.Subject),"Marks").show()
df2.filter(df2.Marks.between(80,90)).select((df2.Subject.alias("MIKE")),"Marks").show()
df2.sort(df2.Marks.desc()).select("Marks","Subject").show()
df2.sort(df2.Marks.asc()).show()
schema=StructType([
StructField("Regno",StringType(),True),
StructField("Subject",StringType(),True),
StructField("Marks",IntegerType(),True)
])
rdd=rdd.map(lambda x:(x[0],x[1], int(x[2])))
print(rdd.collect())
dfo=sql.createDataFrame(rdd,schema)
dfo.show()

