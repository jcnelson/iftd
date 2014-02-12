#!/usr/bin/python

import sys

filename = sys.argv[1]

fd = open(filename, 'r')

curr_t = 0
total = 0
for line in fd:
   t = float(line.split(" ")[1])
   if curr_t == 0:
      curr_t = t

   if " saved " in line:
      total += 50000000


   #if "http end" in line:
   #   curr_t = t + 0.2

   #if "http start" in line:
   #   curr_t = t

   if curr_t + 0.2 <= t:
      while curr_t + 0.2 <= t:
         print str(total - 50000000)
         print "====================="
         curr_t += 0.2

   print line.strip("\n")
