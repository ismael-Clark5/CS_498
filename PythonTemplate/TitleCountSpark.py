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
    stopwords = f.read()

with open(delimitersPath) as f:
    delimiters = str(f.read())

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

delimiters = process_delimiters(delimiters)
lines = sc.textFile(sys.argv[3], 1)
words = lines.flatMap(lambda line: re.split(delimiters, line.lower().strip()))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda x : (-x[1], x[0])).take(10)

outputFile = open(sys.argv[4],"w")
for finalWord in wordCounts:
    word = finalWord[0]
    count = finalWord[1]
    outputFile.write(word + " " + str(count) + "\n")

sc.stop()
