#!/usr/bin/env python

import sys
import os
import hashlib

sys.path.append("../")
sys.path.append("../arizonatransfer")
sys.path.append("../../../2.0/python")
sys.path.append("/usr/lib/python2.5/site-packages/transfer")

import arizonatransfer_iftd as arizonatransfer

rc = arizonatransfer.init_transfer_program( None, None, None, None )

assert rc == True, "init_transfer_program failed!"


filedat = {
   "filename" : "X1Hi1.gif",
   "hashfuncs" : "default",
   "size" : 53966,
   "hash" : 'da2056c2062dfb52102756568e6c5a1982c5aa36'
}

rc, file_list = arizonatransfer.retrieve_files( "i.imgur.com", [filedat], "/tmp/iftd/files", None, nestmode=False )

assert rc == True, "retrieve_files failed!"

print "files retrieved:"
for file in file_list:
   print "   " + str(file)

rc = arizonatransfer.close_transfer_program()

assert rc == True, "close_transfer_program failed!"


