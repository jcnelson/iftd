#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python/" )
sys.path.append( "/usr/local/stork/bin/arizonalib" )

import iftproto
import iftfile
import protocols
import threading
import time
import iftapi
import iftstats
import test_setup
import test_cleanup
import iftutil

import protocols.raven

iftapi.iftd_setup( iftutil.get_available_protocols(), "../iftd.xml" )
test_setup.setup()
iftstats.startup( iftapi.list_protocols(), 1, "NaiveBayes" )


http_connect_attrs = {
	iftproto.PROTO_PORTNUM:8080
}

iftsocket_connect_attrs = {
   iftproto.PROTO_PORTNUM:4000
}

src_file = "/home/jude/raven/tools/iftd/testfile"

job_attrs = {
	iftfile.JOB_ATTR_SRC_HOST:"localhost",
	iftfile.JOB_ATTR_SRC_NAME:"/home/jude/raven/tools/iftd/testfile",
   iftfile.JOB_ATTR_FILE_HASH: iftfile.get_hash( src_file ),
   iftfile.JOB_ATTR_FILE_SIZE: iftfile.get_filesize( src_file ),
	protocols.raven.TRANSFER_PROGRAM_PACKAGE:"transfer.arizonatransfer_http",
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG1:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG2:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG3:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG4:None,
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile",
   iftfile.JOB_ATTR_DEST_HOST:"192.168.1.109"
}

connects = {
      "http":http_connect_attrs,
      "iftsocket":iftsocket_connect_attrs,
      "raven":None
}

job = iftfile.iftjob( job_attrs )

print "prepare_sender"
rc, file_hash, chunk_hashes, chunk_data = iftapi.prepare_sender( job, ["http_sender"])
#rc, file_hash, chunk_hashes, chunk_data = iftapi.prepare_sender( job, ["http_sender", "raven_http_sender"])

print rc
print file_hash
print chunk_hashes
print chunk_data

print "recv_iftd_sender_data"
my_id, chunk_dir, best_proto, connected = iftapi.recv_iftd_sender_data( "12345", job_attrs, ["http_sender", "raven_http_sender"], connects, ["12345", "23456"])

print my_id
print chunk_dir
print best_proto
print connected


time.sleep(60)

test_cleanup.cleanup()
