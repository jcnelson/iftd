#!/usr/bin/env python

import sys

sys.path.append( "../" )

import iftfile

def cleanup():
	print "testcase shutting down"
	iftfile.shutdown()

	print "testcase shutdown completed"

