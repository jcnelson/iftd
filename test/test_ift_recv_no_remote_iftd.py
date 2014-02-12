#!/usr/bin/env python

import sys
import os

sys.path.append( "../" )
sys.path.append( "/home/jude/raven/2.0/python/" )
sys.path.append( "/usr/local/stork/bin/arizonalib" )

import iftproto
import iftfile
import protocols
import threading
import time
import iftapi
import test_setup
import test_cleanup
import iftutil

import protocols.raven

test_setup.setup()

http_connect_attrs = {
	iftproto.PROTO_PORTNUM:80,
	iftfile.JOB_ATTR_SRC_NAME:"/X1Hi1.gif",
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/img.gif",
	iftfile.JOB_ATTR_SRC_HOST:"i.imgur.com"
}

iftsocket_connect_attrs = {
   iftproto.PROTO_PORTNUM:8081
}

src_file = "/X1Hi1.gif"

job_attrs = {
	iftfile.JOB_ATTR_SRC_HOST:"i.imgur.com",
	iftfile.JOB_ATTR_SRC_NAME:src_file,
	protocols.raven.TRANSFER_PROGRAM_PACKAGE:"transfer.arizonatransfer_http",
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG1:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG2:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG3:None,
	protocols.raven.INIT_TRANSFER_PROGRAM_ARG4:None,
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/img.gif",
	iftfile.JOB_ATTR_DEST_HOST:"localhost"
}

client = iftapi.make_XMLRPC_client()

connects = {
      "http_receiver":http_connect_attrs,
      "http_sender":http_connect_attrs,
      "iftsocket_receiver":iftsocket_connect_attrs,
      "iftsocket_sender":iftsocket_connect_attrs,
      "raven":None
}

rc = client.begin_ift( job_attrs, connects, False, True, 4001, "/RPC2", True, False, 60 )
print "called"

test_cleanup.cleanup()
