#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 100)

linksTo =  lines.map(lambda line: line.strip().split(":")[1])
linksTo = linksTo.flatMap(lambda line: line.strip().split(" "))
linkCounts = linksTo.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x : (-x[1], x[0]))

leagueIds = sc.textFile(sys.argv[2], 1).collect()

linksInLeague = linkCounts.filter(lambda link: link[0] in leagueIds).collect()
output = open(sys.argv[2], "w")
for pageLink in linksInLeague:
    linkId = pageLink[0]
    count = pageLink[1]
    output.write(str(linkId) + " " + str(count) + "\n")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

