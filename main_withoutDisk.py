from calculateCost import *
from random import randint
from transfer import *
from collections import namedtuple
import time

db = connect()
cur = db.cursor() 
listOfQueries = []
listOfWaititngQueries = []
f = open('query.txt')
queueSize = 0
q = open('queueLog','w')
c = open('cpuLog','w')
m = open('memoryLog','w')
while(1 and f):
	time.sleep(2)
	print "queueSize",str(queueSize)
	q.write(str(queueSize)) # python will convert \n to os.linesep
	q.write("\n") # python will convert \n to os.linesep
	cur.execute("SELECT avg(CPUUtilization) from node_info")
	val = cur.fetchone() 
	c.write(str(val[0])) # python will convert \n to os.linesep
	c.write("\n") # python will convert \n to os.linesep
	cur.execute("SELECT avg(diskUtilization) from node_info")
	val = cur.fetchone() 
	m.write(str(val[0])) # python will convert \n to os.linesep
	m.write("\n") # python will convert \n to os.linesep
	for query in listOfQueries:
		query["time"] -= 1
		if query["time"] == 0:
			print "*********************query execution completed at site",query["siteId"]
			removeQuery(cur,db,query["siteId"],query["cpuUtilization"],query["diskUtilization"],query["parentId"],query["tables"],query["executionTime"])
	if(randint(0,1) == 1):
		queryGot = 0
		for query in listOfWaititngQueries:
			if getCPUUtilization(cur,query["site"])<= 100 and getDiskUtilization(cur,query["site"])<= 100 and query["isServed"] == 0:
				tables=query["tables"]
				sites=[]
				query["isServed"] = 1
				sites.append(query["site"])
				queryGot = 1
				queueSize-= 1
				print "query got",query["site"],sites
				break
		if queryGot != 1: 	
			query = f.readline()
			if not query:
				break
			query = query.rstrip('\n')
			print "query = ",query
			tables = getTableNames(cur,query)
			sites = getLocalSites(cur,tables)
		cpuThreshold = 60
		diskThreshold = 70
		waitingThreshold = 15
		childNode = []

		for site in sites:
			print "site is",site
			cpuCost = (calculateCPUcost(cur,site,tables))/1000
			diskCost = 0 # (calculateDiskCost(cur,site,tables))/100
			waitingTime = calculateWaitingTime(cur,site)
			cpuUsage = getCPUUtilization(cur,site)
			diskUsage = getDiskUtilization(cur,site)
			totalCostAtParent = cpuCost + diskCost + waitingTime

			if (cpuUsage > cpuThreshold) or (diskUsage > diskThreshold) or (waitingTime > waitingThreshold):
				print "**********cannot be executed in local node*********"
				nodeWithData = findNodeWithData(cur,tables,site)
				for node in nodeWithData:
					waitingTime = calculateWaitingTime(cur,node)
					cpuUsage = getCPUUtilization(cur,node)
					diskUsage = getDiskUtilization(cur,node)
					if cpuUsage<cpuThreshold and diskUsage<diskThreshold and waitingTime<waitingThreshold:
						#print "child found"
						childNode.append(node)
				flag = 0	

				###################### when a child node with the data is found ###############
				for child in childNode:
					if flag == 0:
						cpuCost = (calculateCPUcost(cur,child,tables))/1000
						diskCost =0# (calculateDiskCost(cur,child,tables))/100
						waitingTime = calculateWaitingTime(cur,child)
						totalCostAtChild = cpuCost + diskCost + waitingTime
						cpuUsage = getCPUUtilization(cur,child)
						diskUsage = getDiskUtilization(cur,child)
						if totalCostAtParent > totalCostAtChild and cpuUsage+cpuCost < 100 and diskUsage+diskCost < 100:
							print "Parent cost",totalCostAtParent,"Child cost",totalCostAtChild
							print "************* executing at child",child,"***************"
							transfer(cur,db,child,site,cpuCost,diskCost,tables,totalCostAtChild)
							listOfQueries.append({"siteId":child, "cpuUtilization":cpuCost,
									"diskUtilization":diskCost,  "parentId":site, 
									"tables":tables, "time":int(totalCostAtChild/10)+15,
									"executionTime":int(totalCostAtChild/10)+1})
							flag = 1
				
				###################### search the neighbouring node to execute the query  ###############
				neighbourNode = findNeighbourNode(cur,site)
				for neighbour in neighbourNode:
				   if flag == 0:
					cpuCost = (calculateCPUcost(cur,neighbour,tables))/1000
					diskCost = 0# (calculateDiskCost(cur,neighbour,tables))/100
					waitingTime = calculateWaitingTime(cur,neighbour)
					cpuUsage = getCPUUtilization(cur,neighbour)
					diskUsage = getDiskUtilization(cur,neighbour)
					transferCost = calculateTransferCost(cur,site,neighbour)
					totalCostAtNeighbour = cpuCost + diskCost + waitingTime + transferCost
					if totalCostAtParent > totalCostAtNeighbour and cpuUsage+cpuCost < 100 and diskUsage+diskCost < 100:
						print "****************** transfering tables to node ",neighbour,"***************"
						print "Parent cost",totalCostAtParent,"Neighbour cost",totalCostAtNeighbour
						transfer(cur,db,neighbour,site,cpuCost,diskCost,tables,totalCostAtNeighbour)
						listOfQueries.append({"siteId":neighbour, "cpuUtilization":cpuCost,
								"diskUtilization":diskCost,  "parentId":site, 
								"tables":tables, "time":int(totalCostAtNeighbour/10)+15,
									"executionTime":int(totalCostAtNeighbour/10)+1})
						flag = 1
				cpuUsage = getCPUUtilization(cur,site)
				diskUsage = getDiskUtilization(cur,site)
				cpuCost = (calculateCPUcost(cur,site,tables))/1000
				diskCost = 0 #(calculateDiskCost(cur,site,tables))/100
				if flag == 0 and cpuUsage+cpuCost < 100 and diskUsage+diskCost < 100:
					waitingTime = calculateWaitingTime(cur,site)
					print "******************Executing in the same node",site,"***************"
					print "Parent cost",totalCostAtParent
					transfer(cur,db,site,None,cpuCost,diskCost,tables,totalCostAtParent)
					listOfQueries.append({"siteId":site, "cpuUtilization":cpuCost,
							"diskUtilization":diskCost,  "parentId":None, 
							"tables":tables, "time":int(totalCostAtParent/10)+15,
							"executionTime":int(totalCostAtParent/10)+1})
				else:
					print "******************ALL ARE 100",site,"***************"
					listOfWaititngQueries.append({"tables":tables,"site":site,"isServed":0})
					queueSize+= 1
			elif cpuUsage+cpuCost < 100 and diskUsage+diskCost < 100:
				print "******************Executing in the same node",site,"***************"
				print "Parent cost",totalCostAtParent
				transfer(cur,db,site,None,cpuCost,diskCost,tables,totalCostAtParent)
				listOfQueries.append({"siteId":site, "cpuUtilization":cpuCost,
						"diskUtilization":diskCost,  "parentId":None, 
						"tables":tables, "time":int(totalCostAtParent/10)+15,
						"executionTime":int(totalCostAtParent/10)+1})
			else:
				print "******************ALL ARE 100",site,"***************"
				listOfWaititngQueries.append({"tables":tables,"site":site,"isServed":0})
				queueSize+= 1
				
f.close()
q.close()
c.close()
m.close()
cur.close()
db.close()
