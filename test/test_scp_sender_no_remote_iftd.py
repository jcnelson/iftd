#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )

import iftproto
import iftfile
import protocols
import protocols.iftscp
import hashlib
import threading
import time

import test_setup
import test_cleanup

test_setup.setup()

filename = "/home/jude/raven/tools/iftd/test/testfile.original"


sender = protocols.iftscp.iftscp_sender()

fsize = os.stat( filename ).st_size

file_hash = os.popen("sha1sum " + filename).readlines()[0].split(" ")[0]
print "hash of " + filename + " is " + file_hash

job_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:filename,
	iftfile.JOB_ATTR_PROTOS:"http",
	iftfile.JOB_ATTR_SRC_HOST:"localhost",
	iftfile.JOB_ATTR_CHUNKSIZE:4096,
	iftfile.JOB_ATTR_FILE_SIZE:fsize,
	iftfile.JOB_ATTR_FILE_HASH:file_hash,
	protocols.iftscp.IFTSCP_REMOTE_LOGIN:"jnelson",
	iftfile.JOB_ATTR_DEST_HOST:"lectura.cs.arizona.edu",
	iftfile.JOB_ATTR_DEST_NAME:"/home/jnelson/testfile",
   iftfile.JOB_ATTR_REMOTE_IFTD:False
}

file_job = iftfile.iftjob( job_attrs )

connect_attrs = {
	iftproto.PROTO_PORTNUM:22,
	iftfile.JOB_ATTR_SRC_NAME:filename
}



rc = sender.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

rc = sender.on_start( connect_attrs, file_job )
assert rc == 0, "could not start up! (rc=" + str(rc) + ")"

sender.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )

sender.run( -1 )

print "sleeping"
os.system("sleep 55")
print "done"

test_cleanup.cleanup()
