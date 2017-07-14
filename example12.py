from pyspark import SparkConf, SparkContext

con=SparkConf()
sc=SparkContext(conf=con)
Movies=sc.textFile("File:///home/cloudera/Movies.item")
movieRatings=sc.textFile("File:///home/cloudera/Moving-Ratings-Done.data")
movieRecords=[]
movieslist=Movies.collect()
#This method does things better
def sortRatings(line):
        cols=line.split("\t")
	if(str(cols[2])==str(5)):
        	return cols[1],cols[2]
twoCols=movieRatings.map(sortRatings)
totValues=twoCols.countByValue()
HighestFive=0
TopID=0
Tops={}
for a in totValues:
	if(a!=None):
		if(totValues[a]>HighestFive):
			HighestFive=totValues[a]
			#print(a,"-",totValues[a])
			topID=a[0]
			Tops[a[0]]=totValues[a]

#print(sorted(totValues.items(), key=lambda x:x[1]))
def getNameFromID(rdd):
	lines=rdd.collect()
	for a in lines:
		cols=a.split('|')
		#print(cols[1])
		if(str(cols[0])==str(topID)):
			return cols[1]	
#print(Tops)	
print("The best film is: " + str(getNameFromID(Movies))+ " with " + str(HighestFive) + " ratings")	
