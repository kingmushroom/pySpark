from pyspark import SparkConf, SparkContext

con=SparkConf()
sc=SparkContext(conf=con)
Movies=sc.textFile("File:///home/cloudera/Movies.item")
movieRatings=sc.textFile("File:///home/cloudera/Moving-Ratings-Done.data")
lines=Movies.collect()
currentGenre=5
genres={5:"Unknown",6:"Action",7:"Adventure",8:"Animation",9:"Childrens",10:"Comedy",11:"Crime",12:"Documentary",13:"Drama",14:"Fantasy",15:"Noir",16:"Horror",17:"Musical",18:"Mystery",19:"Romance",20:"SciFi",21:"Thriller",22:"War",23:"Western"}
def getGenreList(line):
	cols=line.split('|')
	global currentGenre
	return cols[currentGenre]

for i in range(currentGenre,24):
	print(genres[i], " count: ",Movies.map(getGenreList).filter(lambda x: x==str(1)).count())
	currentGenre=currentGenre+1
#unknown=Movies.map(getGenreList)
#UnknownCount=unknown.filter(lambda x: x==str(1)).count()
#print("Unknown: "+ str(UnknownCount))
