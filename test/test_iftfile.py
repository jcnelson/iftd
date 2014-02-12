#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )

import iftfile

import test_setup
import test_cleanup

test_setup.setup()

filename = "/tmp/test_iftfile"

file_d = open( filename, "w" )

for i in range(0, 500):
	file_d.write("abcdefg\n")

file_d.close()

file_attrs = {
		iftfile.JOB_ATTR_SRC_NAME:filename,
		iftfile.JOB_ATTR_CHUNKSIZE:len("abcdefg\n")
		}

ift_job = iftfile.iftjob( file_attrs )

ift_file = iftfile.iftfile( filename )
rc = ift_file.fopen( file_attrs, iftfile.MODE_READ )

assert rc != None, "Could not open file!"
assert ift_file.last_error() == 0, "Could not open file!"

for i in range(0, 500):
	chunk, chunk_id = ift_file.read_chunk()
	assert chunk_id == i, "Chunk ID incorrect (" + str(chunk_id) + ")"
	assert chunk == "abcdefg\n", "Could not read chunk " + str(i) + ", got " + chunk

ift_file.fclose()

assert ift_file.last_error() == 0, "Could not close file!"


os.system( "rm " + filename )


ift_job.set_attr( iftfile.JOB_ATTR_CHUNKSIZE, 14 )
ift_job.set_attr( iftfile.JOB_ATTR_FILE_SIZE, 140 )

rc = ift_file.fopen( file_attrs, iftfile.MODE_WRITE )

assert rc != None, "Could not open file for writing! (error " + str(ift_file.last_error()) + ")"
assert ift_file.last_error() == 0, "Could not open file for writing!"

chars = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

for i in range(0, 10):
	assert ift_file.lock_chunk(i) == 0, "Could not lock chunk!"
	ift_file.set_chunk( "hello world " + chars[i] + "\n", i )
	assert ift_file.unlock_chunk(i) == 0, "Could not unlock chunk!"
	assert ift_file.last_error() == 0, "Could not write chunk " + str(i) + ", error=" + str(ift_file.last_error())

ift_file.fclose()

assert ift_file.last_error() == 0, "Could not close file!"

file_d = open( filename, "r" )
lines = file_d.readlines()

i = 0
for line in lines:
	assert line == "hello world " + chars[i] + "\n", "Could not read line " + str(i) + ", got " + line
	i = i + 1
		
ift_file.fclose()

assert ift_file.last_error() == 0, "Could not close file!"

os.system( "cat " + filename )
os.system( "rm " + filename )

test_cleanup.cleanup()
