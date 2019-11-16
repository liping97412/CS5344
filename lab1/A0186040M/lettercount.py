import re
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
#split the input file to words with space
words1 = lines.flatMap(lambda l: re.split(" ",l))
#filter the words that have length
words = words1.filter(lambda w: len(w)>0)
#map to the first letter
pairs = words.map(lambda w : (w[0], 1)) 
#reduce - add up together
result = pairs.reduceByKey(lambda x, y : x + y)
#save the result
result.saveAsTextFile(sys.argv[2])
sc.stop()
