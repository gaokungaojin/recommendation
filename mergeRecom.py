from __future__ import print_function
import traceback
import sys
from operator import add

from pyspark import SparkContext



def map1(a):
	return (a[0], int (a[1]))

def map2(a):
	T=0
	for i in a[1]:
		T=T+i
	return (a[0], T/len(a[1]))


if  __name__ == "__main__":
	sc = SparkContext(appName="Recomm")

        try:
	    	input_recom1 = sc.textFile(sys.argv[1], 1)
	    	input_recom2 = sc.textFile(sys.argv[2], 1)
	    	input_recom3 = sc.textFile(sys.argv[3], 1)
	    	input_recom4 = sc.textFile(sys.argv[4], 1)
	    	input_recom5 = sc.textFile(sys.argv[5], 1)
	    	input_recom6 = sc.textFile(sys.argv[6], 1)
	    	input_recom7 = sc.textFile(sys.argv[7], 1)
	    	input_recom8 = sc.textFile(sys.argv[8], 1)
	    	
        except:
            print ("fail")
            sys.exit(0)
        input_r1= input_recom1.map(lambda x: x.split("\t")).map(map1)
        input_r2= input_recom2.map(lambda x: x.split("\t")).map(map1)
        input_r3= input_recom3.map(lambda x: x.split("\t")).map(map1)
        input_r4= input_recom4.map(lambda x: x.split("\t")).map(map1)
        input_r5= input_recom5.map(lambda x: x.split("\t")).map(map1)
        input_r6= input_recom6.map(lambda x: x.split("\t")).map(map1)
        input_r7= input_recom7.map(lambda x: x.split("\t")).map(map1)
        input_r8= input_recom8.map(lambda x: x.split("\t")).map(map1)
        
	        
        input_M=input_r1.union(input_r2).union(input_r3).union(input_r4).union(input_r5).union(input_r6).union(input_r7).union(input_r8).groupByKey().map(map2).takeOrdered(20, key=lambda x: -x[1])
        ################# PRINT OUT THE RESULT
        for i in input_M:
        	print (str(i[0]+"\t"+str(i[1])))
        #input_v2.coalesce(1).saveAsTextFile(sys.argv[4])


	sc.stop()
