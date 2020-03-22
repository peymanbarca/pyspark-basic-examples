import sys
import os

from pyspark import SparkContext, SparkConf
from SimpleMapReduce.SparkContextCreator import sparkContextCreator

if __name__ == "__main__":

    sc = sparkContextCreator()

    # read input text file to RDD
    lines = sc.textFile('/home/zevik/PycharmProjects/PySparkTutorial/SimpleMapReduce/input.txt')

    # flatMap each line to words
    words = lines.flatMap(lambda line: line.split(" "))

    # collect the RDD to a list
    llist = words.collect()

    # print the list
    for line in llist:
        print(line)
