import sys
import os

from pyspark import SparkContext, SparkConf
from MapReduce.SparkContextCreator import sparkContextCreator
import time

if __name__ == "__main__":

    sc = sparkContextCreator()

    t1 = time.time()

    # read input text file to RDD
    lines = sc.textFile('/home/zevik/PycharmProjects/pyspark-basic-examples/MapReduce/input.txt')

    # flatMap each line to words
    words = lines.flatMap(lambda line: line.split(" "))

    # collect the RDD to a list
    word_list = words.collect()

    # print the list
    for word in word_list:
        print(word)

    t2=time.time()

    print('Took : ',t2-t1,' seconds!')