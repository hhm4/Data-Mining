class AssociationRule:

	def __init__(self,left,right,support,confidence):
		self.left=left
		self.right=right
		self.support=support
		self.confidence=confidence

	def printRule(self):
		print str(self.left) + ' -> ' + str(self.right) + '   ['+str(self.support)+'%,'+str(self.confidence)+'%]' 

class Apriori:

	def __init__(self,fileName,support,confidence):
		self.fileName=fileName
		self.minSupport=support
		self.minConfidence=confidence
		self.transactions={}
		self.itemSets={}
		self.totalNoOfTrans=0
		self.supportValues={}
		self.confidenceValues={}
		self.uniqueItems=set()


	def getUniqueItems(self):
		print '\n Unique Items\n'
		for i in self.uniqueItems:
			print i,

	def getFrequentItemSets(self):
		print '\n Frequent Item Sets\n'
		deleteElements=[]
		for k,v in self.supportValues.items():
			if self.supportValues[k]<self.minSupport:
				deleteElements.append(k)

		for i in deleteElements:
			del self.supportValues[i]

		for k,v in self.supportValues.items():
			print str(k)+' '+str(v)+'%'

	def readTransactions(self):
		with open(self.fileName) as fp:
		    for line in fp:
		    	transNo=line.split('\t')[0]
		    	items=line.split('\t')[1][:-1]
		    	itemList=items.split(';')
		    	for i in itemList:
		    		self.uniqueItems.add(i)
		    	itemList[len(itemList)-1]=itemList[len(itemList)-1]
		    	self.transactions[transNo]=itemList

		self.totalNoOfTrans=len(self.transactions.keys())

		print '\n Transactions \n'    	
		for k,v in sorted(self.transactions.items()):
			print k,v    	

		for value in self.transactions.values():
			self.findItemSets(value)

		#print '\n Item Sets \n' 
		#for k,v in sorted(self.itemSets.items()):
		#	print k,v 

	def findItemSets(self,items):
		numOfComb = 2**len(items);
		combi=[]
		for i in range(numOfComb): 
		    itemSet= []
		    for j in range(len(items)):
		        if i & (2**j):
		            itemSet.append(items[j])

		    if len(itemSet)>0:
		    	itemSet.sort()
		    	temp=tuple(itemSet)
		    	if temp in self.itemSets.keys():
		    		self.itemSets[temp]=self.itemSets[temp]+1
		    	else:
		    		self.itemSets[temp]=1

	def findSupportValues(self):   		
		for k,v in self.itemSets.items():
			self.supportValues[k]=(v*100)/self.totalNoOfTrans

		print '\n Support Values \n'
		for k,v in self.supportValues.items():
			print str(k)+' '+str(v)+'%'

	def generateRulesForItemSet(self,k,leftList,rightList):
		numOfComb = 2**len(k);
		combi=[]
		for i in range(numOfComb):
			left=[]
			right=[]
			for j in range(len(k)):
				if i & (2**j):
					left.append(k[j])

			if len(left)<len(k) and len(left)>0:
				left.sort()
				leftList.append(left)
				for item in k:
					if item not in left:
						right.append(item)

				right.sort()
				rightList.append(right)

	def findAllAssociationRules(self):
		print '\n Association Rules \n'
		for k,v in self.supportValues.items():
			if len(k)>1 and v>=self.minSupport:
				leftList=[]
				rightList=[]
				self.generateRulesForItemSet(k, leftList, rightList)
				for i in range(len(leftList)):
					left=tuple(leftList[i])
					right=tuple(rightList[i])
					confidence = self.getConfidenceValue(left,v)
					if confidence >= self.minConfidence:
						associationRule = AssociationRule(left, right, v, confidence)
						associationRule.printRule()
					#self.associationRules.append(associationRule)

	def getConfidenceValue(self,left,fullSuppValue):
		x=float(fullSuppValue)
		y=float(self.supportValues[left])
		return (x/y)*100
		

def main():

	for i in range(1,6):
		print '\n Running Apriori Algorithm for Database '+str(i)+'\n'
		support=raw_input('\nEnter the Minimum Support Value : ')
		confidence=raw_input('\nEnter the Minimum Confidence Value: ')
		apriori=Apriori('input'+str(i)+'.txt',int(support),int(confidence))
		apriori.readTransactions()
		apriori.getUniqueItems()
		apriori.findSupportValues()
		apriori.getFrequentItemSets()
		apriori.findAllAssociationRules()
	

if __name__=="__main__":
	main()

