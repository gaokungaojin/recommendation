from __future__ import print_function
import traceback
import sys
from operator import add

from pyspark import SparkContext

#from pagerank.simple_page_rank import SimplePageRank
#from pagerank.backedges_page_rank import BackedgesPageRank
def MatrixSplit(a):
	sp= a.split(": ")
	return (sp[0], sp[1])

def flat_matrix(a):
    return (a[0], a[1].rstrip( ).split(','))

def factor_map(a):
	for i in range (0, len(keys), 1):
		if(keys[i]==a[0]):
			return (values[i], a[1])

def map1(a):
	A=[];
	for i in a[1]:
		sp = i.split("-")
		A.append(sp)
	return (a[0], A )

def map2(a):
	A=[];
	for i in a[1]:
		sp =(i[0], int(i[1])* int(a[0]))
		A.append(sp)
	return A


def userSplit(a):
	sp= a.split("\t")
	return (sp[1], int (sp[2]))


			




if  __name__ == "__main__":
	import time
	start_time = time.time()
	sc = SparkContext(appName="Recomm")

        try:
	    	input_rdd = sc.textFile(sys.argv[1], 1)
	    	input_udd = sc.textFile(sys.argv[2], 1)
	    	
        except:
            print ("fail")
            sys.exit(0)
        input_u1= input_udd.map(userSplit).filter(lambda x: x[1]!=0)
        ################# PRINT OUT THE USER VECT
        #input_u1.coalesce(1).saveAsTextFile(sys.argv[3])
        keys = input_u1.keys().collect()
        values = input_u1.values().collect()
        input_v1= input_rdd.map(MatrixSplit).filter(lambda x: x[0] in keys).map(flat_matrix).map(factor_map).map(map1).flatMap(map2)
        input_v2= input_v1.reduceByKey(lambda x, y: x+y).filter(lambda x: x[0] not in keys).takeOrdered(20, key=lambda x: -x[1])
        ################# PRINT OUT THE RESULT
        #input_v1.coalesce(1).saveAsTextFile(sys.argv[4])
        #input_v2.coalesce(1).saveAsTextFile(sys.argv[4])
       	for i in input_v2:
       		print (i)

	sc.stop()
	print("--- %s seconds ---" % (time.time() - start_time))
