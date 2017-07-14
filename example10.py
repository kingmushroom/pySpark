from pyspark import SparkConf, SparkContext

con=SparkConf()
sc=SparkContext(conf=con)
Movies=sc.textFile("File:///home/cloudera/Movies.item")
movieRatings=sc.textFile("File:///home/cloudera/Moving-Ratings-Done.data")
movieRecords=[]
movieslist=Movies.collect()
#This method completes the task(get number of 5 star ratings for Goldeneye), but it is a bit of a poor method. just grabs all info from both files, splits them, then searches them. A better method would be to program this as part of a reduce.

for line in movieslist:
	cols=line.split('|')
	if("GoldenEye" in cols[1].encode("utf-8")):
		movieID=cols[0]
		break
movieRatingsList=movieRatings.collect()
count=0
for line in movieRatingsList:
	cols=line.split('\t')
	if((str(cols[1])==str(movieID)) & (int(cols[2])==5)):
		count=count+1
	
print("Goldeneye has "+str(count)+" 5* ratings")
