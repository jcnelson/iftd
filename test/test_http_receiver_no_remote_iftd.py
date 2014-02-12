#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/usr/lib/python2.5" )

import iftproto
import iftfile
import protocols
import protocols.http
import hashlib
import threading
import time
import cProfile
try:
   import pstats
except:
   pass

import test_setup
import test_cleanup

test_setup.setup()

try:
	import psyco
	psyco.cannotcompile( re.compile )
	psyco.log()
	psyco.profile()
	psyco.full()
except Exception, inst:
	print "no psycho"
	print str(inst)
	pass


receiver = protocols.http.http_receiver()


fsize = os.stat( "/home/jude/raven/tools/iftd/test/X1Hi1.gif.original" ).st_size

m = hashlib.sha1()
file_handle = open( "/home/jude/raven/tools/iftd/test/X1Hi1.gif.original" )
for line in file_handle.readlines():
        m.update( line )
file_hash = m.hexdigest()


connect_attrs = {
	iftproto.PROTO_PORTNUM:80,
	iftfile.JOB_ATTR_CHUNKSIZE:4096,
	iftfile.JOB_ATTR_SRC_HOST:"i.imgur.com",
   iftfile.JOB_ATTR_SRC_CHUNK_DIR:"/",
	iftfile.JOB_ATTR_SRC_NAME:"/X1Hi1.gif",
   iftfile.JOB_ATTR_REMOTE_IFTD:False,
   iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile.gif"
   #iftfile.JOB_ATTR_FILE_SIZE:53966
}


rc = receiver.setup( connect_attrs )
assert rc == 0, "could not setup (rc=" + str(rc) + ")"

rc = receiver.on_start( iftfile.iftjob(connect_attrs), connect_attrs )
assert rc == 0, "could not on_start (rc=" + str(rc) + ")"

receiver.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )
receiver.run(-1)

#rc = receiver.open_connection()
#assert rc == 0, "could not open connection (rc=" + str(rc) + ")"

#transmit_starter = starter()
#transmit_starter.start()

#cProfile.run( "receiver.run(-1)" )

print "sleeping"
os.system("sleep 5")

print "done"

test_cleanup.cleanup()
