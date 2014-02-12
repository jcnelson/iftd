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


sender = protocols.http.http_sender()

fsize = os.stat( filename ).st_size

file_hash = os.popen("sha1sum " + filename).readlines()[0].split(" ")[0]
print "hash of " + filename + " is " + file_hash

job_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:filename,
	iftfile.JOB_ATTR_PROTOS:"http",
	iftfile.JOB_ATTR_SRC_HOST:"localhost",
	iftfile.JOB_ATTR_CHUNKSIZE:4096,
	iftfile.JOB_ATTR_FILE_SIZE:fsize,
	iftfile.JOB_ATTR_FILE_HASH:file_hash
}

file_job = iftfile.iftjob( job_attrs )

connect_attrs = {
	iftproto.PROTO_PORTNUM:8080,
	iftfile.JOB_ATTR_SRC_NAME:filename
}



rc = sender.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

sender.assign_job( file_job )
rc = sender.on_start( connect_attrs )
assert rc == 0, "could not start up! (rc=" + str(rc) + ")"

sender_starter = starter( sender )
sender_starter.start()

sender.run( 0.01 )

print "sleeping"
os.system("sleep 55")
print "done"

test_cleanup.cleanup()
