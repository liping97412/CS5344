import re
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
words = lines.flatMap(lambda l: re.split(" ",l))
wCount = words.filter(word => Character.isLetter(word.head)).map(word => (word.head, 1))
result = wCount.reduceByKey((x, y) => x + y)

result.saveAsTextFile(sys.argv[2])
sc.stop()
