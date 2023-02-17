#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
	stopwords = f.read()

with open(delimitersPath) as f:
    delimiters = f.read()

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)
actualWords = []
for line in lines:
    words = line.strip().split(delimiters)
    for word in words:
        if word not in stopwords:
            actualWords.append(word)

wordCounts = actualWords.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
print(wordCounts)
outputFile = open(sys.argv[4],"w")

#TODO
#write results to output file. Foramt for each line: (line +"\n")

sc.stop()
