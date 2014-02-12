#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python" )

import iftproto
import iftfile
import protocols
import protocols.bittorrent
import hashlib
import threading
import time
import SimpleHTTPServer
import BaseHTTPServer

import test_setup
import test_cleanup

test_setup.setup()

sender = protocols.bittorrent.bittorrent_sender()

job_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:"/home/jude/raven/tools/iftd/test/bittorrent/update-cm-4.1.11.1-signed.zip",
   protocols.bittorrent.IFTBITTORRENT_TORRENT_PATH:"/home/jude/raven/tools/iftd/test/bittorrent/cyanogenmod_4.1.11.1.TPB.torrent"
}


file_job = iftfile.iftjob( job_attrs )

connect_attrs = {
      protocols.bittorrent.IFTBITTORRENT_PORTRANGE_LOW:1025,
      protocols.bittorrent.IFTBITTORRENT_PORTRANGE_HIGH:65535
}


rc = sender.setup( connect_attrs )
assert rc == 0, "could not set up! (rc=" + str(rc) + ")"

rc = sender.on_start( connect_attrs, file_job )
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
