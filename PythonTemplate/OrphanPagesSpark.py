#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 100)
pageIds = lines.map(lambda line: line.strip().split(":")[0])
linksTo =  lines.map(lambda line: line.strip().split(":")[1])
linksTo = linksTo.flatMap(lambda line: line.strip().split(" "))

pageIdsAsSet = set(pageIds.collect())
linksTo = set(linksTo.collect())
orphanLinks = sorted(pageIdsAsSet.difference(linksTo))

output = open(sys.argv[2], "w")
for element in orphanLinks:
    output.write(str(element) + "\n")


#write results to output file. Foramt for each line: (line + "\n")

sc.stop()

