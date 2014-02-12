#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )

import iftproto
import iftfile
import protocols
import protocols.iftsocket
import hashlib
import time
import threading

import test_setup
import test_cleanup

test_setup.setup()

class starter(threading.Thread):
	def __init__(self, transmitter):
		threading.Thread.__init__(self)
		self.transmitter = transmitter
	
	def run(self):
		time.sleep(1)
		print "starting"
		self.transmitter.state = iftproto.PROTO_STATE_RUNNING

sender = protocols.iftsocket.iftsocket_sender()

fsize = os.stat( "/home/jude/raven/tools/iftd/testfile" ).st_size

m = hashlib.sha1()
file_handle = open( "/home/jude/raven/tools/iftd/testfile" )
for line in file_handle.readlines():
	m.update( line )
file_hash = m.hexdigest()


job_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:"/home/jude/raven/tools/iftd/testfile",
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile",
	iftfile.JOB_ATTR_PROTOS:"iftsocket",
	iftfile.JOB_ATTR_SRC_HOST:"localhost",
	iftfile.JOB_ATTR_DEST_HOST:"localhost",
	iftfile.JOB_ATTR_CHUNKSIZE:3,
	iftfile.JOB_ATTR_FILE_SIZE:fsize,
	iftfile.JOB_ATTR_FILE_HASH:file_hash
}

file_job = iftfile.iftjob( job_attrs )

connect_attrs = {
	iftproto.PROTO_PORTNUM:8080
}

rc = sender.setup( connect_attrs )
assert rc == 0, "could not setup (rc=" + str(rc) + ")"

sender.assign_job( file_job )
rc = sender.on_start( connect_attrs )
assert rc == 0, "could not on_start (rc=" + str(rc) + ")"

#rc = sender.open_connection( file_job )
#assert rc == 0, "could not open connection (rc=" + str(rc) + ")"

sender.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )
sender.run(-1)

print "sleeping"
os.system("sleep 5")
print "done"

test_cleanup.cleanup()
