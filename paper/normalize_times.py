#!/usr/bin/python

import sys
log = sys.argv[1]
d = sys.argv[2]

fd = open(log, "r")
lines = fd.readlines()
fd.close()

for l in lines:
   if '[' not in l and ']' not in l:
      continue

   lb = l.find('[')
   rb = l.find(']')
   
   try:
      num = float(l[lb + 1 : rb]) - float(d)
   except:
      continue

   print '[' + str(num) + '] ' + l[rb+1:-1]


