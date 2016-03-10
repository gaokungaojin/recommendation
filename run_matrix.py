from __future__ import print_function
import traceback
import sys
from operator import add

from pyspark import SparkContext

#from pagerank.simple_page_rank import SimplePageRank
#from pagerank.backedges_page_rank import BackedgesPageRank
def mapRate(a):
	return (a[0], a[1], int(a[2]))

def map0(a):
	return (a[1], a[2])
def map1(a):
	return (a[0], (a[1], a[2]))
def map2(a):
	return (a[1], (a[0], a[2])) 
def map3(a):
	T=0
	for i, j in a[1]:
		T=T+j
	return (a[0], T/len(a[1]), a[1])
def map4(a):
	A=[]
	for i, j in a[2]:
		if(j>=a[1]):
			b=(i, 1)
		else:
			b=(i, 0)
		A.append(b)
	return (a[0], A)
			
def map5(a):
	A=[]
	for i, j in a[1]:
		for m, k in a[1]:
			if(j==k):
				B=((i, m), 1)
			else:
				B=((i, m), 0)
			A.append(B)
	return A

def filter1(a):
	if(a[1]==1):
		return a

def map6(a):
	return(a[0][0], a[0][1]+ "-" + str(a[1]))

def reduce1(v1, v2):
	return v1 + "," + v2





if  __name__ == "__main__":
	import time
	start_time = time.time()
	sc = SparkContext(appName="Recomm")
        try:
	    	input_rdd = sc.textFile(sys.argv[1], 1)
	    	#input_u1 = sc.textFile(sys.argv[2], 1)
        except:
            print >> sys.stderr, "Unable to load file"
            sys.exit(0)
        
        input_v1= input_rdd.map(lambda s: s.split("\t"))
		#input_u1= input_use.map(lambda s: s.split("\t")).map(mapRate).map(map0)
	    	
        
	input_v2 = input_v1.map(mapRate)


	input_v3 = input_v2.map(map1).groupByKey()

	input_v4 = input_v2.map(map2).groupByKey()
#
	
	
	input_v5 = input_v3.map(map3).map(map4).flatMap(map5).filter(filter1)
#	print input_v5.collect()
	#print "==================== item by item matrix"
	input_v6 = input_v5.reduceByKey(lambda x,y: x+y).map(map6).reduceByKey(reduce1)
	input_v6.map(lambda x: x[0]+ ": "+ x[1]).coalesce(1).saveAsTextFile(sys.argv[2])

	sc.stop()
	print("--- %s seconds ---" % (time.time() - start_time))
