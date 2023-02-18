#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
import math

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

words = lines.flatMap(lambda line: line.split(" ")).filter(lambda x : x.isnumeric())

minimum = words.reduce(lambda a, b : min(int(a), int(b)))
maximum = words.reduce(lambda a,b : max(int(a), int(b)))
sum = words.reduce(lambda a, b :(int(a) + int(b)))
mean = sum / words.count()
var = words.map(lambda a: math.floor(pow((int(a) - mean), 2)))
var = var.reduce(lambda a, b :int((int(a) + int(b))) / words.count())

outputFile = open(sys.argv[2], "w")
outputFile.write('Sum\t%s\n' % sum)
outputFile.write('Min\t%s\n' % minimum)
outputFile.write('Max\t%s\n' % maximum)
outputFile.write('Mean\t%s\n' % math.floor(mean))
outputFile.write('Var\t%s\n' % math.floor(var))


sc.stop()

