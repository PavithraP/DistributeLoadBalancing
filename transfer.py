import MySQLdb
import re


def connect():
	db = MySQLdb.connect(host="localhost",user="root",
                	      passwd="hello123",db="global_schedular") 
	return db

def findNodeWithData(cur,tables,site):
	tableCount={}
	nodeWithData = []
	try:
		cur.execute("SELECT tableName,childNodeId FROM table_copy_info where parentNodeId = %d" % (site))
		for row in cur.fetchall():
			if row[0] in tables:
				if row[1] in tableCount:
					tableCount[row[1]] += 1	
				else:
					tableCount[row[1]] = 0
		for child in tableCount:
			if tableCount[child] >= len(tables)-1:
				print "selecting",child
				nodeWithData.append(child)
		return nodeWithData
	except Exception as e:
        	print "Error is",e
		return -1
	
################################# finding sides which has tables ######################################
def transfer(cur,db,child,parent,cpuCost,diskCost,tables,totalCost):
	queueSize = (totalCost/10)+1
	try:
		cur.execute("UPDATE node_info SET CPUUtilization=CPUUtilization + %d where nodeId= %d" % (cpuCost,child))
		cur.execute("UPDATE node_info SET diskUtilization=diskUtilization + %d where nodeId = %d" % (diskCost,child))
		cur.execute("UPDATE node_info SET queueSize=queueSize + %d where nodeId = %d" % (queueSize,child))
		if parent != None:
			for table in tables:
				cur.execute("INSERT INTO table_copy_info values(%d,%d,'%s')" % (parent,child,table))
		db.commit()
	except Exception as e:
        		print "Error",e
			return -1
def removeQuery(cur,db,child,cpuCost,diskCost,parentId,tables,time):
	print parentId,child
	try:
		cur.execute("UPDATE node_info SET CPUUtilization=CPUUtilization - %d where nodeId= %d" % (cpuCost,child))
		cur.execute("UPDATE node_info SET diskUtilization=diskUtilization - %d where nodeId = %d" % (diskCost,child))
		cur.execute("UPDATE node_info SET queueSize=queueSize - %d where nodeId = %d" % (time,child))
		if parentId != None:
			for table in tables:
				print parentId,child,table
				cur.execute("delete from table_copy_info where parentNodeId=%d and childNodeId = %d and tableName = '%s'" % (parentId,child,table))
		db.commit()
	except Exception as e:
        		print "Error here",e
			return -1
