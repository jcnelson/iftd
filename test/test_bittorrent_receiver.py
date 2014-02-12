#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python/" )

import iftproto
import iftfile
import protocols
import protocols.bittorrent
import threading
import time

import test_setup
import test_cleanup

test_setup.setup()

receiver = protocols.bittorrent.bittorrent_receiver()


connect_attrs = {
   iftfile.JOB_ATTR_DEST_NAME:"/tmp/cyanogenmod/cm.zip",
   protocols.bittorrent.IFTBITTORRENT_PORTRANGE_LOW:1025,
   protocols.bittorrent.IFTBITTORRENT_PORTRANGE_HIGH:65534,
   protocols.bittorrent.IFTBITTORRENT_TORRENT_PATH:"/home/jude/raven/tools/iftd/test/bittorrent/cyanogenmod_4.1.11.1.TPB.torrent"
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

os.system("sleep 555")
print "done"

test_cleanup.cleanup()
