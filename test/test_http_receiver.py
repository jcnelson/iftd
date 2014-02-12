#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )

import iftproto
import iftfile
import protocols
import protocols.http
import hashlib
import threading
import time

import test_setup
import test_cleanup

test_setup.setup()

filename = "/home/jude/raven/tools/iftd/test/testfile.original"

class starter(threading.Thread):
	def __init__(self, transmitter):
		threading.Thread.__init__(self)
		self.transmitter = transmitter
	
	def run(self):
		time.sleep(0.01)
		print "starting"
		self.transmitter.state = iftproto.PROTO_STATE_RUNNING

receiver = protocols.http.http_receiver()

fsize = os.stat( filename ).st_size

connect_attrs = {
	iftproto.PROTO_PORTNUM:8080,
	iftfile.JOB_ATTR_CHUNKSIZE:4096,
	iftfile.JOB_ATTR_SRC_HOST:"localhost",
	iftfile.JOB_ATTR_SRC_NAME:filename,
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile"
}


rc = receiver.setup( connect_attrs )
assert rc == 0, "could not setup (rc=" + str(rc) + ")"

rc = receiver.on_start( connect_attrs )
assert rc == 0, "could not on_start (rc=" + str(rc) + ")"

#rc = receiver.open_connection()
#assert rc == 0, "could not open connection (rc=" + str(rc) + ")"

transmit_starter = starter( receiver )
transmit_starter.start()

receiver.run( 0.01 )

print "sleeping"
os.system("sleep 5")

print "done"

test_cleanup.cleanup()
