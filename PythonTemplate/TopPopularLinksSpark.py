#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 100)
linksTo =  lines.map(lambda line: line.strip().split(":")[1])
linksTo = linksTo.flatMap(lambda line: line.strip().split(" "))
linkCounts = linksTo.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x : (-x[1], x[0])).take(10)
sortedLinkCount = sorted(linkCounts, key=lambda x: x[0])
output = open(sys.argv[2], "w")
for pageLink in sortedLinkCount:
    linkId = pageLink[0]
    count = pageLink[1]
    output.write(str(linkId) + " " + str(count) + "\n")

sc.stop()

