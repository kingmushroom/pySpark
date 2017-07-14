from pyspark import SparkConf, SparkContext

con=SparkConf()
sc=SparkContext(conf=con)
Movies=sc.textFile("File:///home/cloudera/Movies.item")
movieRatings=sc.textFile("File:///home/cloudera/Moving-Ratings-Done.data")
movieRecords=[]
movieslist=Movies.collect()
#This method does things better

filmToSearchFor='GoldenEye'
#make my movie rdd only contain goleneye for easier searching
Movie=Movies.filter(lambda x:filmToSearchFor in x)
print(Movie.collect())
def getID(rdd):
	for line in rdd.collect():
		cols=line.split('|')
		if(filmToSearchFor in cols[1].encode("utf-8")):
			return(cols[0])

movieID=getID(Movie)
def getReviews(line):
	cols=line.split("\t")
	if(cols[1]==movieID):
		return True
def sortRatings(line):
	cols=line.split("\t")
	return cols[2]
reviews=movieRatings.filter(getReviews)
ratings=reviews.map(sortRatings).countByValue()
for k in ratings:
	print("There are "+str(ratings[k])+ " "+str(k)+" reviews of " + filmToSearchFor)

