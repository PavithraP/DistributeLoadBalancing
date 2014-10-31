from calculateCost import *

db = connect()
cur = db.cursor() 
query = raw_input()
tables = getTableNames(cur,query)
sites = getLocalSites(cur,tables)
for site in sites:
	print "site is",site
	cpuCost = calculateCPUcost(cur,site,tables)
# 	print "costs are",cpuCost
	diskCost = calculateDiskCost(cur,site,tables)
 #	print "costs are",cpuCost
	waitingTime = calculateWaitingTime(cur,site)
print cpuCost,diskCost,waitingTime
cur.close()
db.close()
