#!/usr/bin/env python
from __future__ import print_function
import sys
import json
import numpy
from pyspark import SparkContext

def getCount(line):
        try:
                js=json.loads(line)
                user=js['user']['screen_name']
                text=js['text']
                l=len(text)
                return [(user,(l,1))]
        except Exception as a:
                return []

if __name__ == "__main__":
        if len(sys.argv)<2:
                print("enter a filename")
                sys.exit(1)

        sc= SparkContext(appName="avdTweetlength")

        tweets=sc.textFile(sys.argv[1])

        texts=tweets.flatMap(getCount)
        x=texts.reduceByKey( lambda a,b:list(numpy.add(a,b)))
        d=x.map(lambda pair: (pair[0],(pair[1][0]/float(pair[1][1]),pair[1][1])))
        y=d.top(5, key= lambda z: z[1][0])
        j=d.top(5, key= lambda z: -z[1][0])
        z=d.top(1, key=lambda a:a[1][1])

        print ("The top 5 users with most avg tweet length are")
        print (y)
        print ("\nThe bottom 5 users of most avg tweet length are")
        print (j)
        print ("\n\n The User who tweeted the most is %s making %s Tweets" %(z[0][0],z[0][1][1]))


        sc.stop()

