#!/usr/bin/python2.5

filename = 'output'
filesize = 53966
filehash = 'da2056c2062dfb52102756568e6c5a1982c5aa36'

filedict = {
   "filename":filename,
   "size":filesize,
   "hash":filehash,
   "hashfuncs":None
}

import os
import sys
sys.path.append("/home/jude/raven/2.0/python")
#sys.path.append("/home/jude/raven/2.0/python/transfer")

print sys.path

import transfer
import transfer.arizonatransfer_http as arizonafetch

arizonafetch.init_transfer_program(None, None, None, None)


arizonafetch.retrieve_files( "http://localhost:18090/tmp/", [filedict], "/tmp/arizonafetch", None )

