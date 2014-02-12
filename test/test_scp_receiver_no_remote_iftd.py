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

filename = "/home/jnelson/remote-testfile.txt"

os.popen("mkdir -p /tmp/iftd/files")

receiver = protocols.iftscp.iftscp_receiver()

connect_attrs = {
	iftproto.PROTO_PORTNUM:22,
	iftfile.JOB_ATTR_SRC_NAME:filename,
	iftfile.JOB_ATTR_SRC_HOST:"lectura.cs.arizona.edu",
	protocols.iftscp.IFTSCP_REMOTE_LOGIN:"jnelson",
	iftfile.JOB_ATTR_DEST_NAME:"/home/jude/raven/tools/iftd/test/testfile-remote.txt",

   iftfile.JOB_ATTR_REMOTE_IFTD:False,
   iftfile.JOB_ATTR_SRC_CHUNK_DIR:"/home/jnelson",
   iftfile.JOB_ATTR_NUM_CHUNKS: 1
}



rc = receiver.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

rc = receiver.on_start( iftfile.iftjob( connect_attrs ), connect_attrs )
assert rc == 0, "could not start up! (rc=" + str(rc) + ")"

receiver.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )

receiver.run( -1 )


print "sleeping"
os.system("sleep 55")
print "done"

test_cleanup.cleanup()
