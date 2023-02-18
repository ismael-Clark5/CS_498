#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 
idToLinksMap = lines.flatMap(lambda line: line.strip().split(":")).collect()

output = open(sys.argv[2], "w")
for element in idToLinksMap:
    output.write(str(element) + "\n")

#write results to output file. Foramt for each line: (line + "\n")

sc.stop()

