#!/usr/bin/python

import os
import sys
import httplib
import time

squid_port = 3128
filename = sys.argv[1]
print filename

t1 = time.ctime()
t2 = time.ctime( time.time() + 10 )

cache_headers = {
      "If-Modified-Since": t1
}

print "GET localhost:" + str(squid_port) + filename

connection = httplib.HTTPConnection( "localhost:" + str(squid_port) )
connection.request("GET", filename, headers=cache_headers )
response = connection.getresponse()

print "Status: " + str(response.status)

time.sleep(11)

connection.request("GET", filename,  headers=cache_headers )
response = connection.getresponse()

print "Status for unmodified GET: " + str(response.status)

os.popen("touch " + filename )
time.sleep(11)

connection.request("GET", filename, headers=cache_headers )
response = connection.getresponse()

print "Status for modified-since GET: " + str(response.status)

connection.close()
