from calculateCost import *
from random import randint
from transfer import *
from collections import namedtuple
import time

db = connect()
cur = db.cursor() 
#nodeInfo = namedtuple("nodeInfo", "siteId cpuUtilization diskUtilization parentId tables time")
listOfQueries = []
f = open('query.txt')
while(1 and f):
	time.sleep(5)
	for query in listOfQueries:
		query["time"] -= 1
		if query["time"] == 0:
			print "*********************query execution completed at site",query["siteId"]
			removeQuery(cur,db,query["siteId"],query["cpuUtilization"],query["diskUtilization"],query["parentId"],query["tables"])
	if(randint(0,1) == 1):	
		query = f.readline()
		if not query:
			break
		query = query.rstrip('\n')
	#	query = raw_input()
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
			diskCost = (calculateDiskCost(cur,site,tables))/100
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
						diskCost = (calculateDiskCost(cur,child,tables))/100
						waitingTime = calculateWaitingTime(cur,child)
						totalCostAtChild = cpuCost + diskCost + waitingTime
						if totalCostAtParent > totalCostAtChild:
							print "Parent cost",totalCostAtParent,"Child cost",totalCostAtChild
							print "************* executing at child",child,"***************"
							transfer(cur,db,child,site,cpuCost,diskCost,tables)
							#q = nodeInfo(child,cpuCost,diskCost,site,tables,3)
							listOfQueries.append({"siteId":child, "cpuUtilization":cpuCost,
									"diskUtilization":diskCost,  "parentId":site, 
									"tables":tables, "time":2})
							flag = 1
				
				###################### search the neighbouring node to execute the query  ###############
				neighbourNode = findNeighbourNode(cur,site)
				for neighbour in neighbourNode:
				   if flag == 0:
					cpuCost = (calculateCPUcost(cur,neighbour,tables))/1000
					diskCost = (calculateDiskCost(cur,neighbour,tables))/100
					waitingTime = calculateWaitingTime(cur,neighbour)
					transferCost = calculateTransferCost(cur,site,neighbour)
					totalCostAtNeighbour = cpuCost + diskCost + waitingTime + transferCost
					if totalCostAtParent > totalCostAtNeighbour:
						print "****************** transfering tables to node ",neighbour,"***************"
						print "Parent cost",totalCostAtParent,"Neighbour cost",totalCostAtNeighbour
						transfer(cur,db,neighbour,site,cpuCost,diskCost,tables)
						listOfQueries.append({"siteId":neighbour, "cpuUtilization":cpuCost,
								"diskUtilization":diskCost,  "parentId":site, 
								"tables":tables, "time":1})
						#q = nodeInfo(neighbour,cpuCost,diskCost,site,tables,3)
						#listOfQueries.append(q)
						flag = 1
				if flag == 0:
					print "******************Executing in the same node",site,"***************"
					transfer(cur,db,site,None,cpuCost,diskCost,tables)
					listOfQueries.append({"siteId":site, "cpuUtilization":cpuCost,
							"diskUtilization":diskCost,  "parentId":None, 
							"tables":tables, "time":3})
					#q = nodeInfo(site,cpuCost,diskCost,None,tables,1)
					#listOfQueries.append(q)
			else:
				print "******************Executing in the same node",site,"***************"
				transfer(cur,db,site,None,cpuCost,diskCost,tables)
				listOfQueries.append({"siteId":site, "cpuUtilization":cpuCost,
						"diskUtilization":diskCost,  "parentId":None, 
						"tables":tables, "time":5})
				#q = nodeInfo(site,cpuCost,diskCost,None,tables,5)
				#listOfQueries.append(q)
cur.close()
db.close()
