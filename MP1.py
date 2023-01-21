import random 
import os
import re
import fileinput
import sys

stopWordsList = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

delimiters = " \t,;.?!-:@[](){}_*/"

def getIndexes(seed):
    random.seed(seed)
    n = 10000
    number_of_lines = 50000
    ret = []
    for i in range(0,n):
        ret.append(random.randint(0, 50000-1))
    return ret


def process_delimiters(delimiters):
    special_characters = ['.', '\\', '+', '*', '?', '[', ']', '(', ')', '{', '}', '!', ':', '-']
    regex = ""
    delimiters_as_list = [*delimiters]
    for delimiter in delimiters_as_list:
        if delimiter in special_characters:
            regex += "\\" + delimiter + "|"
        else:
            regex += delimiter + "|"
    return regex[:-1]

def process(userID):
    indexes = getIndexes(userID)
    ret = []

    lines_subset = []
    file_lines = []

    for line in sys.stdin:
        file_lines.append(line.strip())

    regex_delimeters = process_delimiters(delimiters)
    word_dict = {}
    for index in indexes:
        lines_subset.append(file_lines[index].lower())

    for line in lines_subset:
        words = re.split(regex_delimeters, line)
        for word in words:
            if word not in stopWordsList and word != "":
                if word in word_dict:
                    word_dict[word] += 1
                else:
                    word_dict[word] = 1

    ret = sorted(word_dict.items(), key=lambda x : (-x[1], x[0]))[0:20]
    for word in ret:
        print(word[0])

process(sys.argv[1])
