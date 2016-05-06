#!/usr/bin/env python
from __future__ import print_function
import sys
import json
import numpy
from pyspark import SparkContext

def getYear(line):
        try:
                word=line.split('\t')[0]
                year=line.split('\t')[1]
                return [(str(year),(len(word),1))]
        except Exception as a:
                return []

if __name__ == "__main__":
        if len(sys.argv)<2:
                print("enter a filename")
                sys.exit(1)

        sc= SparkContext(appName="word_len_years")

        gram_data=sc.textFile(sys.argv[1])

        word=gram_data.flatMap(getYear)
        x=word.reduceByKey(lambda a,b: list(numpy.add(a,b)))
        d=x.map(lambda pair : (pair[0],pair[1][0]/float(pair[1][1])))


        d.coalesce(1).saveAsTextFile("outp.txt")
        sc.stop()


