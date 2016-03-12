import os
import sys
import os.path
if __name__ == "__main__":
	import time
	start_time = time.time()
	
	if (os.path.exists('./recom1')):
		os.system("rm recom1")
	if (os.path.exists('./recom2')):
		os.system("rm recom2")
	if (os.path.exists('./recom3')):
		os.system("rm recom3")
	if (os.path.exists('./recom4')):
		os.system("rm recom4")
	if (os.path.exists('./recom5')):
		os.system("rm recom5")
	if (os.path.exists('./recom6')):
		os.system("rm recom6")
	if (os.path.exists('./recom7')):
		os.system("rm recom7")
	if (os.path.exists('./recom8')):
		os.system("rm recom8")
	if (os.path.exists('./Final_Recomm')):
		os.system("rm Final_Recomm")

	if (os.path.exists('./matrix1') == False):
		print "Need sub matrix1, please check whether it is exits or it uses another name"
		exit()
	if (os.path.exists('./matrix2') == False):
		print "Need sub matrix2, please check whether it is exits or it uses another name"
		exit()
	if (os.path.exists('./matrix3') == False):
		print "Need sub matrix3, please check whether it is exits or it uses another name"
		exit()
	if (os.path.exists('./matrix4') == False):
		print "Need sub matrix4, please check whether it is exits or it uses another name"
		exit()
	if (os.path.exists('./matrix5') == False):
		print "Need sub matrix5, please check whether it is exits or it uses another name"
		exit()
	if (os.path.exists('./matrix6') == False):
		print "Need sub matrix6, please check whether it is exits or it uses another name"
		exit()
	if (os.path.exists('./matrix7') == False):
		print "Need sub matrix7, please check whether it is exits or it uses another name"
		exit()
	if (os.path.exists('./matrix8') == False):
		print "Need sub matrix8, please check whether it is exits or it uses another name"
		exit()

	user = sys.argv[1]
	if (os.path.exists('./%s'%user) == False):
		print "user history vector does not exit in the current directory, please check again"
		exit()
	if (os.path.exists('./run_recom.py') == False):
		print "need run_recom.py file"
		exit()
	if (os.path.exists('./mergeRecom.py') == False):
		print "need mergeRecom.py file"
		exit()
		
	os.system("../bin/spark-submit run_recom.py matrix1 %s > recom1"%user)
	os.system("../bin/spark-submit run_recom.py matrix2 %s > recom2"%user)
	os.system("../bin/spark-submit run_recom.py matrix3 %s > recom3"%user)
	os.system("../bin/spark-submit run_recom.py matrix4 %s > recom4"%user)
	os.system("../bin/spark-submit run_recom.py matrix5 %s > recom5"%user)
	os.system("../bin/spark-submit run_recom.py matrix6 %s > recom6"%user)
	os.system("../bin/spark-submit run_recom.py matrix7 %s > recom7"%user)
	os.system("../bin/spark-submit run_recom.py matrix8 %s > recom8"%user)
	os.system("../bin/spark-submit mergeRecom.py recom1 recom2 recom3 recom4 recom5 recom6 recom7 recom8 > Final_Recomm")
	print("--- %s seconds ---" % (time.time() - start_time))
