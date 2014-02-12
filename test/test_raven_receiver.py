#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python/" )
sys.path.append( "/usr/local/stork/bin/arizonalib" )

import iftproto
import iftfile
import protocols
import protocols.raven
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

receiver = protocols.raven.raven_receiver()

connect_attrs = {
	iftproto.PROTO_PORTNUM:8080,
	iftfile.JOB_ATTR_SRC_HOST:"http://localhost:8080/",
	iftfile.JOB_ATTR_SRC_NAME:"/home/jude/raven/tools/iftd/testfile",
	protocols.raven.TRANSFER_PROGRAM_PACKAGE:"transfer.arizonatransfer_http",
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG1:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG2:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG3:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG4:None,
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile"
}

os.system("sleep 1")

rc = receiver.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

rc = receiver.on_start( connect_attrs )
assert rc == 0, "could not on_start! (rc=" + str(rc) + ")"

#rc = receiver.open_connection()
#assert rc == 0, "could not open connection! (rc=" + str(rc) + ")"

receiver.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )
receiver.run(-1)

os.system("sleep 5")
print "done"

test_cleanup.cleanup()
