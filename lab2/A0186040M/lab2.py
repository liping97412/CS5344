
# coding: utf-8

# In[1]:

from pyspark import SparkConf, SparkContext
import re
conf = SparkConf()
sc = SparkContext(conf=conf)


# In[2]:

texts = sc.wholeTextFiles("/home/spark/Desktop/lab2/datafiles")
#split the textfile by space and special characters
#words = texts.map(lambda x : x[1].split(' ')).collect()
words = texts.map(lambda x: re.split('[, .\n]',x[1])).collect()


# In[3]:

stopwords = sc.textFile("/home/spark/Desktop/lab2/stopwords.txt")
#RDD cannot be used in another rdd transformation, we must use the following code to collect the value in an RDD
sw = sc.broadcast(stopwords.collect()).value


# In[4]:

ww = [[],[],[],[],[],[],[],[],[],[]]
#transform the words to lowercase,remove the stopwords
for i in range(0,10):
    ww[i] = sc.parallelize(words[i]).filter(lambda w: len(w)>0).map(lambda w: w.lower()).filter(lambda w: w not in sw).collect()


# In[5]:

count = [[],[],[],[],[],[],[],[],[],[]]
for i in range(0,10):
    count[i] = sc.parallelize(ww[i]).map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y).collect()


# In[6]:

join = [[],[],[],[],[],[],[],[]]
join[0] = sc.parallelize(count[0]).join(sc.parallelize(count[1])).map(lambda x:(x[0],min(x[1]))).collect()
for i in range(1,8):
    join[i] = sc.parallelize(join[i-1]).join(sc.parallelize(count[i+1])).map(lambda x:(x[0],min(x[1]))).collect()


# In[7]:

join = sc.parallelize(join[7])


# In[8]:

# Swap the keys and values
final = join.map(lambda x: (x[1], x[0]))

# Sort the keys in descending order
final_sort = final.sortByKey(ascending=False).map(lambda x: (x[1], x[0]))


# In[9]:

final_sort.repartition(1).saveAsTextFile("output")

