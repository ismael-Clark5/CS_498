#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
    stopwords = f.read()

with open(delimitersPath) as f:
    delimiters = str(f.read())

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

print("Ismael" + delimiters)
print("Ismael" + stopWordsPath)
lines = sc.textFile(sys.argv[3], 1)
words = lines.flatMap(lambda line: line.strip().split(delimiters))

wordCounts = words.map(lambda word: (word, 1) if (word not in stopwords) else None).reduceByKey(lambda a,b:a +b)
top10Lists = wordCounts.sortBy(lambda x :(-x[1], x[0])).cache().take(10)
outputFile = open(sys.argv[4],"w")
for finalWord in top10Lists:
    word = finalWord[0]
    count = finalWord[1]
    outputFile.write(word + " " + str(count) + "\n")

sc.stop()
