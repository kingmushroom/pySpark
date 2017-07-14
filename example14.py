from pyspark import SparkConf, SparkContext
#This program will get the most popular film for under 18s
con=SparkConf()
sc=SparkContext(conf=con)
Movies=sc.textFile("File:///home/cloudera/Movies.item")
movieRatings=sc.textFile("File:///home/cloudera/Moving-Ratings-Done.data")
users = sc.textFile("File:///home/cloudera/Users.txt")
header=users.first()
users=users.filter(lambda x:x!=header)

def ages(line):
	cols=line.split('|')
	return(cols[0],cols[1])

def getunder18s(line):
	if(int(line[1])<19):
		return True
	else:
		return False
def usersGet(line):
	cols=line.split('|')
def splitRatings(line):
	cols=line.split('\t')
	return(cols[0],cols[1],cols[2])	
def justIDs(line):
	return(line[0])
def under185ratings(line):
	if(line[0] in under18Users):
		return True
def stripIDs(line):
	return(line[1],line[2])
ratings = movieRatings.map(splitRatings)

def getNameFromID(rdd):
        lines=rdd.collect()
        for a in lines:
                cols=a.split('|')
                if(str(cols[0])==str(maxID)):
                        return cols[1]

under18Users = users.map(ages).filter(getunder18s).map(justIDs).collect()
under18FiveRatings=ratings.filter(under185ratings).map(stripIDs).filter(lambda x:int(x[1])>4)
filmRatings=under18FiveRatings.countByValue()

maxID=''
maxRatings=0
for a in filmRatings:
	if(filmRatings[a]>maxRatings):
		maxRatings=filmRatings[a]
		maxID=a[0]

print("Best film for 18 and under is " + str(getNameFromID(Movies))+ " with "+str(maxRatings)+ " 5* ratings")

