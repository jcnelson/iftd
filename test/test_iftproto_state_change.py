#!/usr/bin/env python

import sys
import os

sys.path.append("../")

import iftproto
import iftfile
import hashlib
import protocols
import time
import test_setup
import test_cleanup

test_setup.setup()

print os.getcwd()

connect_attrs = {
	iftproto.PROTO_PORTNUM:8080
}

proto_handle = iftproto.make_protocol( "iftsocket", connect_attrs, connect_attrs, 1.0, 1.0 )
assert proto_handle > 0, "failed to make protocol, rc=" + str(proto_handle)

sender_rc, receiver_rc = iftproto.activate_protocol( proto_handle, False, False, 1.0, 1.0 )
assert sender_rc == 0 and receiver_rc == 0, "failed to activate protocol (rc=" + str(sender_rc) + "," + str(receiver_rc) + ")"

fsize = os.stat( "/home/jude/raven/tools/iftd/testfile" ).st_size

m = hashlib.sha1()
file_handle = open( "/home/jude/raven/tools/iftd/testfile" )
for line in file_handle.readlines():
	m.update( line )
file_hash = m.hexdigest()


job_attrs = {
	iftfile.JOB_ATTR_SRC_NAME:"/home/jude/raven/tools/iftd/testfile",
	iftfile.JOB_ATTR_DEST_NAME:"/tmp/testfile",
	iftfile.JOB_ATTR_PROTOS:"iftsocket",
	iftfile.JOB_ATTR_SRC_HOST:"localhost",
	iftfile.JOB_ATTR_DEST_HOST:"localhost",
	iftfile.JOB_ATTR_CHUNKSIZE:3,
	iftfile.JOB_ATTR_FILE_SIZE:fsize,
	iftfile.JOB_ATTR_FILE_HASH:file_hash
}

file_job = iftfile.iftjob( job_attrs )

assert iftproto.start_receiver( proto_handle ) == 0, "failed to start receiver"
assert iftproto.start_sender( proto_handle, file_job ) == 0, "failed to start sender"

time.sleep( 5.0 )

assert iftproto.suspend_sender( proto_handle ) == 0, "failed to suspend sender"
assert iftproto.suspend_receiver( proto_handle ) == 0, "failed to suspend receiver"

time.sleep( 2.0 )

assert iftproto.resume_receiver( proto_handle ) == 0, "failed to resume receiver"
assert iftproto.resume_sender( proto_handle ) == 0, "failed to resume sender"

time.sleep( 2.0 )

iftproto.deactivate_protocol( proto_handle )
#iftproto.end_sender( proto_handle )
#iftproto.end_receiver( proto_handle )

time.sleep( 2.0 )

iftproto.terminate_protocol( proto_handle )
#iftproto.term_sender( proto_handle )
#iftproto.term_receiver( proto_handle )


print "done"

test_cleanup.cleanup()
