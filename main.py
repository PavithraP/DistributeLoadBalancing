from calculateCost import *
from transfer import *
from collections import namedtuple

db = connect()
cur = db.cursor() 
query = raw_input()
tables = getTableNames(cur,query)
sites = getLocalSites(cur,tables)
cpuThreshold = 60
diskThreshold = 50
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
		print "cannot be executed in local node"
		nodeWithData = findNodeWithData(cur,tables,site)
		for node in nodeWithData:
			waitingTime = calculateWaitingTime(cur,node)
			cpuUsage = getCPUUtilization(cur,node)
			diskUsage = getDiskUtilization(cur,node)
			if cpuUsage<cpuThreshold and diskUsage<diskThreshold and waitingTime<waitingThreshold:
				print "child found"
				childNode.append(node)
		flag = 0	

		###################### when a child node with the data is found ###############
		for child in childNode:
			if flag == 0:
				cpuCost = (calculateCPUcost(cur,child,tables))/1000
				diskCost = (calculateDiskCost(cur,child,tables))/100
				waitingTime = calculateWaitingTime(cur,child)
				totalCostAtChild = cpuCost + diskCost + waitingTime
				print "Parent cost",totalCostAtParent,"Child cost",totalCostAtChild
				if totalCostAtParent > totalCostAtChild:
					print "************* executing at child",child,"***************"
					transfer(cur,db,child,site,cpuCost,diskCost,tables)
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
                        print "Parent cost",totalCostAtParent,"Neighbour cost",totalCostAtNeighbour
                        if totalCostAtParent > totalCostAtNeighbour:
				print "****************** transfering tables to node ",neighbour,"***************"
                                transfer(cur,db,neighbour,site,cpuCost,diskCost,tables)
                                flag = 1
		if flag == 0:
			print "******************Executing in the same node***************"
                	transfer(cur,db,site,None,cpuCost,diskCost,tables)
	else:
		print "******************Executing in the same node***************"
                transfer(cur,db,site,None,cpuCost,diskCost,tables)

cur.close()
db.close()
