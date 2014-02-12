#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python/" )

import iftproto
import iftfile
import protocols
import protocols.iftcache
import threading
import time
import iftapi
import test_setup
import test_cleanup
import iftutil

test_setup.setup()

iftapi.iftd_setup( iftutil.get_available_protocols(), "../iftd.xml" )

receiver = protocols.iftcache.iftcache_receiver()

filename = os.popen("cat /tmp/iftcache_filename").readlines()[0].strip()

connect_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:filename,
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile",
   iftfile.JOB_ATTR_SRC_HOST:"localhost",
   iftfile.JOB_ATTR_DEST_HOST:"localhost",
   protocols.iftcache.IFTCACHE_MAX_AGE:30
}


os.system("sleep 1")

rc = receiver.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

rc = receiver.on_start( iftfile.iftjob(connect_attrs), connect_attrs )
assert rc == 0, "could not on_start! (rc=" + str(rc) + ")"

receiver.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )

#rc = receiver.open_connection()
#assert rc == 0, "could not open connection! (rc=" + str(rc) + ")"


receiver.run( -1 )

os.system("sleep 5")
print "done"

test_cleanup.cleanup()
