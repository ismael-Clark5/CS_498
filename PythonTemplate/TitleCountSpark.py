#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
import re
from pyspark import SparkConf, SparkContext


def process_delimiters(delmitersAsString):
    special_characters = ['.', '\\', '+', '*', '?', '[', ']', '(', ')', '{', '}', '!', ':', '-']
    regex = ""
    delimiters_as_list = [*delmitersAsString]
    for delimiter in delimiters_as_list:
        if delimiter in special_characters:
            regex += "\\" + delimiter + "|"
        else:
            regex += delimiter + "|"
    return regex[:-1]

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
    stopwords = [line.rstrip('\n') for line in f]

with open(delimitersPath) as f:
    for line in f:
        delimiters = line

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

delimiters = process_delimiters(delimiters)
stopwords.append("")
lines = sc.textFile(sys.argv[3], 1)
delimiterAsRe = re.compile('[' + re.escape(delimiters) + ']')
words = lines.flatMap(lambda line: delimiterAsRe.split(line.lower().strip()))
wordCounts = words.filter(lambda x: x not in stopwords).map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x : (-x[1], x[0])).take(10)
wordCounts = sorted(wordCounts, key=lambda x: x[0])
outputFile = open(sys.argv[4],"w")
for finalWord in wordCounts:
    word = finalWord[0]
    count = finalWord[1]
    outputFile.write(word + "\t" + str(count) + "\n")

sc.stop()
