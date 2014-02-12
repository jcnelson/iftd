#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python" )

import iftproto
import iftfile
import protocols
import protocols.iftcache
import hashlib
import threading
import time
import SimpleHTTPServer
import BaseHTTPServer

import test_setup
import test_cleanup

test_setup.setup()

sender = protocols.iftcache.iftcache_sender()

filename = "/tmp/iftcache_test_" + str(os.getpid())

os.popen( "echo 'testing at " + time.ctime() + "' > " + filename )
os.popen( "echo '" + filename + "' > /tmp/iftcache_filename")
fsize = os.stat( filename ).st_size

m = hashlib.sha1()
file_handle = open( filename )
for line in file_handle.readlines():
        m.update( line )
file_hash = m.hexdigest()

job_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:filename,
}


file_job = iftfile.iftjob( job_attrs )

connect_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:filename,
}


rc = sender.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

sender.assign_job( file_job )
rc = sender.on_start( connect_attrs )
assert rc == 0, "could not start up! (rc=" + str(rc) + ")"

sender.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )

#rc = sender.open_connection( file_job )
#print "open_connection"
#assert rc == 0, "could not open connection! (rc=" + str(rc) + ")"

sender.run( 0.01, False )

print "sleeping"
os.system("sleep 5")
print "done"

test_cleanup.cleanup()
