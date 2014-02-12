#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python" )

import iftproto
import iftfile
import protocols
import protocols.iftraven
import hashlib
import threading
import time
import SimpleHTTPServer
import BaseHTTPServer

import test_setup
import test_cleanup

test_setup.setup()

if not os.path.exists( "/tmp/src" ):
	os.makedirs("/tmp/src")
else:
	os.system("rm -rf /tmp/src/*")

class starter(threading.Thread):
        def __init__(self, transmitter):
                threading.Thread.__init__(self)
                self.transmitter = transmitter

        def run(self):
                time.sleep(1)
                print "starting"
                self.transmitter.state = iftproto.PROTO_STATE_RUNNING
		
		# normally, the sender is already running a server, so we'll make an HTTP server
		os.chdir("/")
		httphandler = SimpleHTTPServer.SimpleHTTPRequestHandler
		httpserver = BaseHTTPServer.HTTPServer( ("", 8080), httphandler )
		httpserver.serve_forever()


sender = protocols.iftraven.iftraven_sender()

proto = iftproto.iftproto( "iftraven_sender", sender, None )

#rc = proto.init( sender )
#assert rc == 0, "Could not start sender!"

#rc = proto.init( receiver )
#assert rc == 0, "Could not start reciever!"

fsize = os.stat( "/home/jude/raven/tools/iftd/testfile" ).st_size

m = hashlib.sha1()
file_handle = open( "/home/jude/raven/tools/iftd/testfile" )
for line in file_handle.readlines():
        m.update( line )
file_hash = m.hexdigest()

job_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:"/home/jude/raven/tools/iftd/testfile",
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile",
	iftfile.JOB_ATTR_PROTOS:"iftraven",
	iftfile.JOB_ATTR_SRC_HOST:"localhost",
	iftfile.JOB_ATTR_DEST_HOST:"localhost",
	iftfile.JOB_ATTR_CHUNKSIZE:3,
	iftfile.JOB_ATTR_FILE_SIZE:fsize,
	iftfile.JOB_ATTR_FILE_HASH:file_hash,
	protocols.iftraven.SRC_CHUNK_DIR:"/tmp/src"
}


file_job = iftfile.iftjob( job_attrs )

connect_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:"/home/jude/raven/tools/iftd/testfile",
	protocols.iftraven.SRC_CHUNK_DIR:"/tmp/src"
}


rc = sender.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

sender.assign_job( file_job )
rc = sender.on_start( connect_attrs )
assert rc == 0, "could not start up! (rc=" + str(rc) + ")"

startup = starter( sender )
startup.start()
#rc = sender.open_connection( file_job )
#print "open_connection"
#assert rc == 0, "could not open connection! (rc=" + str(rc) + ")"

sender.run( 0.01, False )

print "sleeping"
os.system("sleep 5")
print "done"

test_cleanup.cleanup()
