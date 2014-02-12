#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python/" )

import iftproto
import iftfile
import iftutil
import protocols
import protocols.iftcache
import protocols.http
import threading
import time

import test_setup
import test_cleanup

import iftapi

iftapi.iftd_setup( iftutil.get_available_protocols(), "../iftd.xml")

test_setup.setup()

receiver = protocols.iftcache.iftcache_receiver()

connect_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:"/X1Hi1.gif",
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile",
   protocols.iftcache.IFTCACHE_MAX_AGE:30,
   protocols.iftcache.IFTCACHE_BASEDIR:"/tmp/test_iftcache",
   protocols.http.iftfile.JOB_ATTR_SRC_HOST:"i.imgur.com"
}


os.system("sleep 1")

rc = receiver.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

rc = receiver.on_start( connect_attrs )
assert rc == 0, "could not on_start! (rc=" + str(rc) + ")"

receiver.post_msg( iftproto.PROTO_MSG_USER, iftproto.PROTO_STATE_RUNNING )

#rc = receiver.open_connection()
#assert rc == 0, "could not open connection! (rc=" + str(rc) + ")"


receiver.run( 0.01 )

os.system("sleep 5")
print "done"

test_cleanup.cleanup()
