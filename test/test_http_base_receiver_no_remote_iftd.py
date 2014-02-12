#!/usr/bin/env python

# get a file, but don't use iftd at all
# this is used for timing comparisons

import httplib
import time
import os
import cProfile

import sys
sys.path.append( "/usr/lib/python2.5" )

import pstats

# get a file that we know about
def get_file():
	filename = "/X1Hi1.gif"
	host = "imgur.com"
	chunk_size = 4096
	num_chunks_in_transit = 4
	file_size = 53966 #os.stat( "X1Hi1.gif.original" ).st_size

	next_chunk_offset = 0

	start_time = time.clock() 
	while next_chunk_offset < file_size:
		headers = { 'Range' : "bytes=" + str(next_chunk_offset) + "-" + str(next_chunk_offset * num_chunks_in_transit) }
		connection = httplib.HTTPConnection( host + ":80" )
		connection.request( "GET", filename, None, headers )
		response = connection.getresponse()
		if response.status != 206:
			print "Error receiving " + filename + " from host " + host
			exit(1)
		response.read()	# actually receive it
		next_chunk_offset = next_chunk_offset + num_chunks_in_transit * chunk_size
		connection.close()

	end_time = time.clock()

	print "ticks: " + str(end_time - start_time)


# get the file without chunking
def get_whole_file():
	connection = httplib.HTTPConnection( "imgur.com:80" )
	connection.request( "GET", "/X1Hi1.gif" )
	response = connection.getresponse()
	if response.status != 200:
		print "Error receiving"
		exit(1)
	response.read()
	connection.close()


cProfile.run( "get_file()" )

cProfile.run( "get_whole_file()" )

cProfile.run( "get_file()" )

cProfile.run( "get_whole_file()" )

exit(0)
