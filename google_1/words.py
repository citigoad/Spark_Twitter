#!/usr/bin/env python
from __future__ import print_function
import sys
import json
import numpy
from pyspark import SparkContext

def getYear(line):
        try:
                year=line.split('\t')[1]
                return [(str(year),1)]
        except Exception as a:
                return []

if __name__ == "__main__":
        if len(sys.argv)<2:
                print("enter a filename")
                sys.exit(1)

        sc= SparkContext(appName="words_years")

        gram_data=sc.textFile(sys.argv[1])

        year=gram_data.flatMap(getYear)
        x=year.reduceByKey(lambda a,b: a+b)


        x.coalesce(1).saveAsTextFile("op1.txt")
        sc.stop()


