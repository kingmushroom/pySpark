from pyspark import SparkConf, SparkContext

con=SparkConf()
sc=SparkContext(conf=con)
personalInfo=sc.textFile("File:///home/cloudera/personal.csv")
resultInfo=sc.textFile("File:///home/cloudera/results.csv")

class person():
	
	def __init__(self,regNo,name,address):
		self.regNo=regNo
		self.name=name
		self.address=address

	def addMaths(self,mark):
		self.maths=mark	
	def addChem(self,mark):
		self.chem=mark
	def addPhys(self,mark):
		self.phys=mark
	def printEverything(self):
		print("__________")
		print("Reg Number:" + str(self.regNo))
		print("Name:"+str(self.name))
		print("Address:"+str(self.address))
		print("Total:"+str(self.total)+"/450")
		print("Percentage:"+str(int(self.percentage))+"%")
		print("Grade:"+self.grade)
		print("__________")
	def makeTotals(self):
                self.total =int(self.maths)+int(self.chem)+int(self.phys)
                self.percentage=(float(self.total)/450)*100

	def getGrade(self):
		if(self.percentage<60):
			self.grade='F'
		elif(self.percentage<70):
			self.grade='C'
		elif(self.percentage<80):
			self.grade='B'
		elif(self.percentage<90):
			self.grade='A'
		else:
			self.grade='A+'	
def getBiggest(people):
	highestID=''
	highestScore=0
	secHighestScore=0
	secHighestName=''
	thiHighestScore=0
	thiHighestName=''
	highestName=''
	for peep in people:
		if(peep.percentage>highestScore):
			highestScore=peep.percentage
			highestID=peep.regNo
			highestName=peep.name
		if((peep.percentage>secHighestScore)&(peep.percentage<highestScore)):
			secHighestScore=peep.percentage
			secHighestName=peep.name
		if((peep.percentage>thiHighestScore)&(peep.percentage<secHighestScore)):
			thiHighestScore=peep.percentage
			thiHighestName=peep.name
	print("Highest Mark was "+str(highestScore)+ ", achieved by "+ highestName)
	print("2nd Highest Mark was "+str(secHighestScore)+ ", achieved by "+ secHighestName)
	print("3rd Highest Mark was "+str(thiHighestScore)+ ", achieved by "+ thiHighestName)




def makePerson(a):
	cols=a.split(',')
	x = person(cols[0],cols[1],cols[2])
	return x

def split(rdd):
	header=rdd.first()
	rdd=rdd.filter(lambda x: x!=header)
	file=rdd.collect()
	return file

personalInfo=split(personalInfo)
people=[]
for a in personalInfo:
	people.append(makePerson(a))


resultInfo=split(resultInfo)
for a in resultInfo:
	cols=a.split(',')
	
	for peep in people:
		if(peep.regNo == cols[0]):
			if(cols[1]=='maths'):
				peep.addMaths(cols[2])
			if(cols[1]=='chemistry'):
                                peep.addChem(cols[2])
			if(cols[1]=='physics'):
                                peep.addPhys(cols[2])


for peep in people:
	peep.makeTotals()
	peep.getGrade()
	peep.printEverything()

getBiggest(people)
