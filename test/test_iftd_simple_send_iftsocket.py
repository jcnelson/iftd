#!/usr/bin/env python

# OBSOLETE! 

import sys
sys.path.append("../")

import iftproto

import iftfile
from iftutil import *
from iftdata import *
from iftloader import *

import xmlrpclib

server = xmlrpclib.Server("http://localhost:" + str(USER_PORT) + "/RPC2", allow_none=True)
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

	proto_handle = server.make_protocol( 'iftsocket', connect_args, connect_args, 0.01, 0.01 )
	assert proto_handle > 0, "make_protocol(\'iftsocket\') rc=" + str(proto_handle)
except Exception, inst:
	print "couldn't use protocol iftsocket"
	print str(inst)




rc = server.activate_protocol( proto_handle, {iftproto.PROTO_PORTNUM:8080}, True )
assert rc == [0,0], "activate_protocol rc=" + str(rc)

fsize = os.stat( "/home/jude/raven/tools/iftd/testfile" ).st_size
fhash = iftfile.get_hash( "/home/jude/raven/tools/iftd/testfile" )

job_attrs = {
        iftfile.JOB_ATTR_SRC_NAME:"/home/jude/raven/tools/iftd/testfile",
        iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile",
        iftfile.JOB_ATTR_PROTOS:"iftsocket",
        iftfile.JOB_ATTR_SRC_HOST:"localhost",
        iftfile.JOB_ATTR_DEST_HOST:"localhost",
        iftfile.JOB_ATTR_CHUNKSIZE:3,
        iftfile.JOB_ATTR_FILE_SIZE:fsize,
	iftfile.JOB_ATTR_FILE_HASH:fhash
}

rc = server.simple_send( proto_handle, job_attrs )
assert rc == 0, "simple_send: rc=" + str(rc)

rc = server.deactivate_protocol( proto_handle )
assert rc == 0, "deactivate_protocol: rc=" + str(rc)

rc = server.terminate_protocol( proto_handle )
assert rc == 0, "terminate_protocol: rc=" + str(rc)

#rc = server.delete_protocol( proto_handle )
#assert rc == 0, "delete_protocol: rc=" + str(rc)







