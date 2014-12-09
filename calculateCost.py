import MySQLdb
import re


def connect():
	db = MySQLdb.connect(host="localhost",user="root",
                	      passwd="hello123",db="global_schedular") 
	return db

################################# finding sides which has tables ######################################
def getLocalSites(cur,tables):
	sites = []
	newSites = []
	tableCountAtSite={}
#	print "tables=",tables
	for table in tables:
		try:
#			print "table=",table
			cur.execute("SELECT nodeId FROM table_site_info where tables = '%s'" % (table))
			for row in cur.fetchall() :
				if row[0] not in sites:
					sites.append(row[0])
					tableCountAtSite[row[0]] = 0
				tableCountAtSite[row[0]] += 1
				#print "****table count is ",tableCountAtSite[row[0]],row[0]
		except Exception as e:
        		print "Error",e
	for site in sites:
#		print "table count is after",tableCountAtSite[site],site
		if tableCountAtSite[site] >= len(tables):
			newSites.append(site)	
	return newSites

################################# calculating CPU cost ######################################
def calculateCPUcost(cur,siteId,tables):
	try:
		noOfJoin = 1
		cur.execute("SELECT joinCost FROM node_info where nodeId = %d" % (siteId))
		val = cur.fetchone() 
		joinCost = val[0] 
		for table in tables:
			cur.execute("SELECT noOfTuples FROM table_info where tableName = '%s'" % (table))
			val= cur.fetchone()
			noOfJoin *= val[0]
		val = noOfJoin * joinCost
	#	print "joincost =",val
		return val
	except Exception as e:
        	print "Error",e
		return -1
	
################################# calculating disk cost ######################################
def calculateDiskCost(cur,siteId,tables):
	try:
		noOfTuples = 0
		cur.execute("SELECT diskSeekCost FROM node_info where nodeId = %d" % (siteId))
		val = cur.fetchone() 
		diskAccessCost = val[0]
		for table in tables:
			cur.execute("SELECT noOfTuples FROM table_info where tableName = '%s'" % (table))
			val= cur.fetchone()
			noOfTuples += val[0]
		val = noOfTuples * diskAccessCost
	#	print "diskcost",val
		return val
	except Exception as e:
        	print "Error",e
		return -1

################################# calculating queuing cost ######################################
def calculateWaitingTime(cur,siteId):
	try:
		cur.execute("SELECT queueSize FROM node_info where nodeId = %d" % (siteId))
		waitingTime = cur.fetchone()
		return waitingTime[0]
	except Exception as e:
        	print "Error",e
		return -1
	
################################# reading the table names into tables[] ######################################
def getTableNames(cur,query):
	try:
		tables = []
		table_number = 0 
		match = re.search(r"from", query)
		i = match.start(0)
		newQuery = query[i+5:len(query)]
		tokens = newQuery.split(" ")
		if(tokens[0] == 'where' or tokens[0] == 'order' or tokens[0] == 'group'):
			print "No tables specified"
			sys.exit(0)
		for token in tokens:
			for t in token.split(","):
				if t != '':
					t = t.replace(";", "")
					tables.append(t)
		print "tables taking part in query = ",tables
		return tables
	except Exception as e:
        	print "Error",e
		return -1

################################# getting CPU usage ######################################
def getCPUUtilization(cur,nodeId):
	try:
		cur.execute("SELECT CPUUtilization FROM node_info where nodeId = %d" % (nodeId))
		val = cur.fetchone()
		return val[0]
	except Exception as e:
        	print "Error",e
		return -1

################################# getting disk usage ######################################
def getDiskUtilization(cur,nodeId):
	try:
		cur.execute("SELECT diskUtilization FROM node_info where nodeId = %d" % (nodeId))
		val = cur.fetchone()
		return val[0]
	except Exception as e:
        	print "Error",e
		return -1

################################# calculating transfer cost ######################################
def calculateTransferCost(cur,sourceId,destId):
	try:
		cur.execute("SELECT noOfIntermediateHop FROM node_distance where sourceNodeId = %d and destNodeId = %d" % (sourceId,destId))
		val = cur.fetchone()
	#	print "val = ",val
		return 3*val[0]
	except Exception as e:
        	print "Error",e
		return -1

def findNeighbourNode(cur,siteId):
	neighbourNode = []
	try:
		cur.execute("SELECT destNodeId FROM node_distance where sourceNodeId = %d order by noOfIntermediateHop" % (siteId))
		for row in cur.fetchall() :
			neighbourNode.append(row[0])
		#	print "neighbout node is",neighbourNode
		return neighbourNode
	except Exception as e:
        	print "Error",e
		return -1
