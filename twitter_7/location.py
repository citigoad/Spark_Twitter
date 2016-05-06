#!/usr/bin/env python
from __future__ import print_function
import sys
import json
import numpy
from pyspark import SparkContext

def getCount(line):
        try:
                js=json.loads(line)
                id=js['id']
                coordinates=js['coordinates']
                #l=len(text)
                if coordinates:
                        return [("loc",coordinates['coordinates'])]
                else:
                        return[("noloc",coordinates)]
        except Exception as a:
                return []

if __name__ == "__main__":
        if len(sys.argv)<2:
                print("enter a filename")
                sys.exit(1)

        sc= SparkContext(appName="location")
        tweets=sc.textFile(sys.argv[1])
        coordinates=tweets.flatMap(getCount)
        l=coordinates.filter(lambda pair:pair[0]=="loc")
        nl=coordinates.filter(lambda pair:pair[0]=="noloc")
        nl_count=nl.count()
        l_count=l.count()
        x=l.reduceByKey(lambda a,b: list(numpy.add(a,b)))
        centroid=x.take(1)
        centroid=numpy.array(centroid[0][1])
        centroid=centroid/float(l_count)
        print ("The proportion of tweets with location to those without location is :")
        print ((l_count/float(nl_count)))
        print ("The centroid of the locations is ")
        print (centroid)

        sc.stop()

