#!/usr/bin/env python

import sys
sys.path.append("../")

import iftproto

from iftutil import *
from iftdata import *
from iftloader import *

import xmlrpclib

server = xmlrpclib.Server("http://localhost:" + str(USER_PORT) + "/RPC2")
try:
	server.hello_world()
except:
	print "Couldn't connect to xmlrpc server"
	sys.exit(1)

supported_protos = server.get_available_protocols()

print "available: " + str(supported_protos)

connect_args = { iftproto.PROTO_PORTNUM:8080 }

proto_handle = 0
try:

	proto_handle = server.make_protocol( 'iftsocket', connect_args, connect_args, 0.01 )
	assert proto_handle > 0, "make_protocol(\'iftsocket\') rc=" + str(proto_handle)
except:
	print "couldn't use protocol iftsocket"
	sys.exit(1)

rc = server.activate_protocol( proto_handle, {iftproto.PROTO_PORTNUM:8081}, False )
assert rc == [0,0], "activate_protocol rc=" + str(rc)
