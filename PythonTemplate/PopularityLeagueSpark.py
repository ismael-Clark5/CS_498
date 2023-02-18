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
linkCounts = linksTo.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

leagueIds = sc.textFile(sys.argv[2], 1)
leagueIdsAsList = leagueIds.collect()
linksInLeague = sorted(linkCounts.filter(lambda link: link[0] in leagueIdsAsList).collect(), key=lambda x: x[0], reverse=True)
output = open(sys.argv[3], "w")
for linkData in linksInLeague:
    linkCount = linkData[1]
    count = 0
    for secondaryLink in linksInLeague:
        if linkCount > secondaryLink[1]:
            count = count + 1
    output.write(str(linkData[0]) + " " + str(count) + "\n")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

