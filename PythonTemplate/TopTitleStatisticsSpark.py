#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

words = lines.flatMap(lambda line: line.split(" ")).filter(lambda x : x.isnumeric())
# mean = words
sum = words.reduce(lambda a,b :( a + b))
minimum = words.reduce(lambda a,b : min(a, b))
maximum = words.reduce(lambda a,b : max(a, b))
# var =
words.saveAsTextFile("./output")
outputFile = open(sys.argv[2], "w")
outputFile.write('Sum\t%s\n' % sum)
outputFile.write('Min\t%s\n' % minimum)
outputFile.write('Max\t%s\n' % maximum)

'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)

outputFile.write('Var\t%s\n' % ans5)
'''

sc.stop()

