from pyspark import SparkConf, SparkContext

con=SparkConf()
sc=SparkContext(conf=con)
rdd=sc.textFile("File:///home/cloudera/employees.txt")
def breakRecord2(rec):
        record=rec.split("|")
	return(record[0],record[2])

header = rdd.first()
rdd=rdd.filter(lambda x:x!=header)
def sum(a,b):
	a=int(a)
	b=int(b)
	return a+b
rdd3=rdd.map(breakRecord2).reduceByKey(sum)
#rdd4=rdd.map(breakRecord2).countByKey()
data= rdd3.collect()
print(data)
def loadfile():
	rdd=sc.textFile('file:///home/cloudera/employees.txt')
	header = rdd.first()
	rdd=rdd.filter(lambda x:x!=header)
	return rdd

def splitfile(rdd):
	lines=rdd.collect()
	thisID=''
	passed=0
	changedID=False
	for a in lines:
		cols=a.split('|')
		if(thisID!=cols[0]):
			if(changedID):
                        	print("You passed ",passed," exams")
                        	if(passed>2):
                                	"You pass the course"
                        	else:
                                	print("You Fail now")
					for a in range(1,10):
                                     		print("I Must do Better")
				changedID=False
			thisID=cols[0]
			print("Scores for ", thisID)
			changedID=True
		print(str(cols[1])+' Score:'+str(cols[2])+ '/150' + " Percentage:"+str((float(cols[2])/150)*100))
		if((int(cols[2])*100)/150>70):
			passed=passed+1	
	if(passed>2):
        	print("You pass the course")
	else:	
		print("You fail now")
		for a in range(1,10):
			print("I Must do Better")
			





def doStuff(x,x2):
	record=str(x).split("'")
	x=str(record[1])
	x2=x2.replace(')','')
	percentage=str(int(x2)/int(rdd4[x]))
	note=''
	if(percentage<70):
		note=" You Fail now"
	if(rdd4[x]>2):
		print("Score for:" + x + " = Marks:"+x2+ " Percentage:" +percentage+note)
	else:
		print(x + " Still needs to complete "+ str(3-rdd4[x])+ " Exams")
#for dat in data:
 #       data=str(dat).split(',')
  #      doStuff(data[0],data[1])

rdd=loadfile()
splitfile(rdd)
