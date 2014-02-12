#!/usr/bin/python

import os
import sys

filename = sys.argv[1]

fd = open( filename, "r" )

# skip first two lines
fd.readline()
fd.readline()

while True:

   for m in xrange(0, 9):
      line = fd.readline()
      if len(line) == 0:
         sys.exit(0)

      this_file = line.split(" ")[-1].strip("\n")
      methods = {}
      for i in xrange(0, 90):
         line = fd.readline()
         line = line[ line.find(']') + 2 :].strip("\n")     # strip [iftd ...]
         method = line.split(" ")[0]
         time = line.split(" ")[2]
         if not method in methods.keys():
            methods[method] = [float(time)]
         else:
            methods[method].append( float(time) )

      print "file " + this_file + ":"
      for method in methods.keys():
         avg = 0
         min = 100000000
         max = 0
         n = 0
         for t in methods[method]:
            avg += t
            n += 1
            if t > max:
               max = t
            if t < min:
               min = t

         avg /= len(methods[method])

         print "   " + method
         print "      avg: " + str(avg)
         print "      min: " + str(min)
         print "      max: " + str(max)

      fd.readline() 
