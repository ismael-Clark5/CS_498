#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 100)
linksTo =  lines.map(lambda line: line.strip().split(":")[1])
linksTo = linksTo.flatMap(lambda line: line.strip().split(" "))
linkCounts = sorted(linksTo.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x : (-x[1], x[0])).take(10), lambda x: x[0])
output = open(sys.argv[2], "w")
for finalWord in linkCounts:
    word = finalWord[0]
    count = finalWord[1]
    linkCounts.write(word + " " + str(count) + "\n")

sc.stop()

