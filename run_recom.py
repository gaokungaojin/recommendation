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


def userRank(a):
	if(a[1]>100 or a[1]< 30):
		return (a[0], 0 )
	elif (a[1]>=30 and a[1]<60):
		return (a[0], 2)
	elif (a[1]>=60 and a[1]< 80):
		return (a[0], 5)
	elif (a[1]>=80 and a[1]<90):
		return (a[0], 8)
	elif (a[1]>=90 and a[1]<=100):
		return (a[0], 10)




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
        input_uBF= input_udd.map(userSplit).map(userRank)
        input_uF= input_uBF.filter(lambda x: x[1]!=0)
        ################# PRINT OUT THE USER VECT
        #input_uBF.coalesce(1).saveAsTextFile(sys.argv[3])

        # Collecting user key before filtering
        keys_BF = input_uBF.keys().collect()
        # Collecting user key after filtering
        keys = input_uF.keys().collect()
        # Collecting user value afoter filtering 
        values = input_uF.values().collect()
        input_v1= input_rdd.map(MatrixSplit).filter(lambda x: x[0] in keys).map(flat_matrix).map(factor_map).map(map1).flatMap(map2)
        
       # calculate user recom verctor
        input_v2= input_v1.reduceByKey(lambda x, y: x+y).filter(lambda x: x[0] not in keys_BF).takeOrdered(20, key=lambda x: -x[1])
        
        
        ################# PRINT OUT THE RESULT

       	for i in input_v2:
       		print (str (i[0])+"\t"+str (i[1]))

	sc.stop()
	#print("--- %s seconds ---" % (time.time() - start_time))
