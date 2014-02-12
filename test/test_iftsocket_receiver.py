#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )

import iftproto
import iftfile
import protocols
import protocols.iftsocket
import hashlib
import threading
import time

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

receiver = protocols.iftsocket.iftsocket_receiver()

fsize = os.stat( "/home/jude/raven/tools/iftd/testfile" ).st_size

m = hashlib.sha1()
file_handle = open( "/home/jude/raven/tools/iftd/testfile" )
for line in file_handle.readlines():
        m.update( line )
file_hash = m.hexdigest()


connect_attrs = {
	iftproto.PROTO_PORTNUM:8080
}


rc = receiver.setup( connect_attrs )
assert rc == 0, "could not setup (rc=" + str(rc) + ")"

rc = receiver.on_start( connect_attrs )
assert rc == 0, "could not on_start (rc=" + str(rc) + ")"

#rc = receiver.open_connection()
#assert rc == 0, "could not open connection (rc=" + str(rc) + ")"

receiver.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )
receiver.run(-1)

print "sleeping"
os.system("sleep 5")

print "done"

test_cleanup.cleanup()
