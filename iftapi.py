#!/usr/bin/python2.5

"""
iftapi.py
Copyright (c) 2009 Jude Nelson

This package provides the top-level iftd API,
accessable through a package and through an
XMLRPC server if iftd is running.
"""


import os
import sys
import getopt
import time
import copy

import iftfile
import iftlog
import iftstats
import iftutil
from iftdata import *
import iftloader
import iftcore
from iftcore.consts import *
from ifttransfer import *

import threading
import thread

import xmlrpclib
import hashlib
import cPickle
import Queue
import random
import httplib
import stat
import socket

import protocols

import traceback

import sets


# connection attributes...
CONNECT_ATTR_REMOTE_PORT = "CONNECT_ATTR_REMOTE_PORT"
CONNECT_ATTR_REMOTE_RPC = "CONNECT_ATTR_REMOTE_RPC"
CONNECT_ATTR_USER_TIMEOUT = "CONNECT_ATTR_USER_TIMEOUT"

# instances of every available protocol.
# protocol class name ==> protocol instance
PROTOCOLS = {}

# specify True/False (or None) if there is a known remote IFTD port
IFTD_REMOTE_PORT = "IFTD_REMOTE_PORT"



iftd_alive = False

def is_alive():
   """
   Is IFTD actually running?  Have threads check this value periodically so they die as instructed.
   """
   global iftd_alive
   return iftd_alive


def set_alive( val ):
   global iftd_alive
   iftd_alive = val


def iftd_setup(given_protocols, config_filename):
   """
   Read the configuration data for iftd and set up each protocol with it.
   Also set up global defaults
   
   @arg given_protocols
      List of names of protocols to use
      
   @arg config_filename
      Path to iftd xml configuration file
   
   @return
      0; check log for results
   """
   
   # read config
   rc, config, config_data = load_config( config_filename )
   
   if rc != 0:
      return (rc, None)
   
   # initialize all available protocols
   for proto_name in given_protocols:
      for proto_type in ("_sender", "_receiver"):
         
         proto_class = proto_name + proto_type
         
         if config_data.has_key(proto_class) == False:
            continue
         
         for proto_setup_config in config_data[proto_class]:
            rc, proto = config_protocol( "protocols." + proto_name, proto_class, proto_setup_config )
            if rc != 0:
               iftlog.log(5, "protocol_setup: could not set up protocol " + proto_class + " (rc = " + str(rc) + ")")
               continue
         
            PROTOCOLS[proto.name] = proto
            
            iftlog.log(3, "protocol_setup: set up " + proto_name + " as " + proto.name)
   
   
   iftlog.log(1, "==============" )
   iftlog.log(1, "Setup complete" )
   iftlog.log(1, "==============" )
   iftlog.log(1, "Available protocols:")
   protos = list_protocols()
   protos.sort()
   iftlog.log(1, str(protos))
   
   return (0, config)


def config_protocol( package_name, proto_name, setup_args ):
   """
   Given the name of the protocol (package name and class name), use the given
   setup and connect arguments to load, instantiate, and setup the protocol.
   
   @arg package_name:
      Name of the package containing the protocol
      
   @arg proto_name:
      Name of the protocol class
   
   @arg setup_args:
      Dictionary of one-time setup (passed to setup())
      If this is none, setup() will not be called.
   
   @return
      (0, proto_instance) on success, (nonzero, None) on failure.
   """
   
   rc = iftloader.import_package( package_name )
   if rc < 0:
      iftlog.log(4, "config_protocol: failed to import " + package_name)
      return (rc, None)
   
   proto = iftloader.instantiate_class( package_name + "." + proto_name, None )
   if proto == None:
      iftlog.log(4, "config_protocol: failed to load " + proto_name)
      return (E_FILE_NOT_FOUND, None)
   
   # setup the protocol
   if setup_args == None:
      setup_args = {}

   rc = proto.validate_attrs( setup_args, proto.get_setup_attrs() )
   if rc != 0:
      iftlog.log(5, "config_protocol: setup attributes are insufficient")
      return (rc, None)
   
   rc = proto.do_setup( setup_args )
   if rc != 0:
      iftlog.log(4, "config_protocol: failed to setup " + proto_name)
      iftlog.log(1, "                 with args " + str(setup_args))
      return (rc, None)
   
   # save setup information
   # TODO: what happens in the case of raven?
   # save_reusable_data( package_name + "." + proto_name, setup_args )
   
   return (0, proto)    # success!
   


def start_proto( proto, connect_arguments, user_job, t ):
   """
   Start a single protocol, and handle errors gracefully.
   Return True if it started up; False if not
   """
   try:
      iftlog.log(1, "iftapi.start_proto: trying " + str(proto.name))
      rc = proto.on_start( user_job, connect_arguments, t )
      if rc != 0:
         iftlog.log(1, "iftapi.start_proto: FAIL on " + str(proto.name) + ", rc = " + str(rc))
         iftlog.log(5, "iftapi.start_proto: " + proto.name + ".on_start failed with rc=" + str(rc))
         return False
   
      
      iftlog.log(1, "iftapi.start_proto: set up " + str(proto.name))
      iftlog.log(1, "iftapi.start_proto: started up protocol " + proto.name)   
      return True
   except Exception, inst:
      iftlog.log(5, "iftapi.start_proto: could not start " + proto.name, inst )
      traceback.print_exc()
      return False
      
      

def start_protos( connect_dict=None, user_job=None, protos=None, timeout=0.0 ):
   """
   Run on_start() for all protocols
   return the list of protocols that successfully started.
   """
   return start_active_protos( connect_dict, user_job, protos, timeout ) + start_passive_protos( connect_dict, user_job, protos, timeout )




def start_active_protos( connect_dict=None, user_job=None, protos=None, timeout=0.0 ):
   """
   Run on_start() for all protocols
   return the list of protocols that successfully started.
   """
   connected = []
   
   # start active protocols first, since startup is non-blocking.
   for proto in protos:
      if proto and proto.isactive():
         connect_arguments = {}
         if connect_dict:
            connect_arguments = connect_dict.get( proto.name )
         elif user_job:
            connect_arguments = user_job.attrs
         if start_proto( proto, connect_arguments, user_job, timeout ):
            connected.append( proto )
               
   # give back all that connected
   return connected




def start_passive_protos( connect_dict=None, user_job=None, protos=None, timeout=0.0 ):
   """
   Run on_start() for all protocols
   return the list of protocols that successfully started.
   """
   connected = []
   
   # start passive protocols, since startup may be blocking
   for proto in protos:
      if proto and not proto.isactive():
         connect_arguments = {}
         if connect_dict:
            connect_arguments = connect_dict.get( proto.name )
         elif user_job:
            connect_arguments = user_job.attrs
         if start_proto( proto, connect_arguments, user_job, timeout ):
            connected.append( proto )
         
      
   # give back all that connected
   return connected



def protocol_shutdown( shutdown_arg_dict=None ):
   """
   Shut down each protocol
   
   @arg shutdown_arg_dict
      Dictionary of shutdown arguments to pass, where each key is a protocol name
      and each value is a dictionary of shutdown arguments for that protocol
      
   @return
      0
   """
   
   global PROTOCOLS
   
   for (proto_name, proto) in PROTOCOLS.items():
      if shutdown_arg_dict != None and shutdown_arg_dict.has_key(proto_name):
         proto.shutdown( shutdown_arg_dict[proto_name] )
         proto.kill( shutdown_arg_dict[proto_name] )
         
      else:
         proto.shutdown( None )
         proto.kill( None )
         
   
   return 0




def begin_ift( job_attrs, connect_dict=None, sender=False, receiver=False, iftd_remote_port=USER_PORT+1, iftd_xmlrpc_path="/RPC2", user_timeout=60 ):
   """
   Initiate an intelligent file transmission, using the local and remote iftd instances' previous knowledge
   of which protocols work best for which files.
   
   @arg job_attrs
      This is a dictionary describing everything known about the file, the host with the file (the source host),
      and the host to receive the file (the dest host)
      
   @arg connect_dict
      This is a dictionary that maps protocol names to dictionaries containing their connection arguments.
      Protocol names must end in _sender or _receiver! 
   
   @arg sender
      True if the file to transmit is on localhost (in which JOB_ATTR_SRC_NAME is a path on localhost's disk to the file)
   
   @arg receiver
      True if the file to transmit is on the remote host (in which JOB_ATTR_SRC_NAME is a path on the remote host's disk to the file)
   
   @arg iftd_remote_port
      Port number that the remote IFTD listens on for inter-IFTD communication.
      
   @arg iftd_xmlrpc_path
      Directory under which the remote IFTD listens for XMLRPC calls.
   
   @arg user_timeout
      Timeout in seconds before the transfer must complete to be a success.
      
   @return
      0 on success
      -1 on bad data
      < -1 on internal error (see iftdata.py for error constants)
      11 (TRANSMIT_STATE_FAIULRE) on transmission failure
   """
  
   iftlog.log(5, ">>> begin_ift entered <<<")
   
   global PROTOCOLS
   
   job = iftfile.iftjob( job_attrs )
   
   # sanity check
   if sender and receiver:
      return -1      # cannot do
   
   if not (sender or receiver):
      return -1      # cannot do
   
   # get the available protocols
   available_protocols = job.get_attr( iftfile.JOB_ATTR_PROTOS )
   if available_protocols == None or len(available_protocols) == 0:
      available_protocols = list_protocols()
   else:
      absent = []
      
      # make sure that they are all defined
      for proto in available_protocols:
         if proto not in PROTOCOLS.keys():
            absent.append( proto )
         
      
      for proto in absent:
         iftlog.log(3, "begin_ift: unrecognized protocol " + proto + ", ignoring..." )
         available_protocols.remove( proto )
   
   # stuff the relevant data into connect_dict if needed
   if connect_dict == None:
      connect_dict = {}
   
   
   for proto in list_protocols():
      cd = connect_dict.get(proto)
      if cd == None:
         connect_dict[proto] = {}
         cd = connect_dict[proto]
      
   
   # am I the sender or receiver?
   # Have the sender contact the remote iftd instance for protocol information.
   if sender:
      rc = iftsend( job, available_protocols, connect_dict, iftd_remote_port, iftd_xmlrpc_path, user_timeout )
      iftlog.log(5, ">>> begin_ift rc=" + str(rc) + " <<<")
      return rc
   else:
      rc = iftreceive( job, available_protocols, connect_dict, iftd_remote_port, iftd_xmlrpc_path, user_timeout )
      iftlog.log(5, ">>> begin_ift rc=" + str(rc) + " <<<")
      return rc





def iftsend( job, available_protocols, connect_dict=None, iftd_remote_port = USER_PORT + 1, iftd_xmlrpc_path = "/RPC2", user_timeout = 60 ):
   """
   Intelligently send a file to a remote host.
   
   @arg job
      This is an iftjob instance containing the job data
   
   @arg available_protocols
      This is a list of strings of each available (usable) protocol to use
   
   @arg connect_dict
      This is a dictionary of dictionaries mapping protocol names in available_protocols to connection attribute dictionaries.
   
   @arg iftd_remote_port
      Remote IFTD XML-RPC port (default is USER_PORT+1)
   
   @arg iftd_xmlrpc_path
      Remote IFTD XML-RPC domain (default is /RPC2)
   
   @arg user_timeout
      Timeout for XML-RPC calls
   """
   
   from iftdata import SEND_FILES_DIR
   job_attrs = job.attrs
   
   # get remote host
   recv_host = job.get_attr( iftfile.JOB_ATTR_DEST_HOST )
   if recv_host == None:
      iftlog.log(5, "iftsend: remote host not defined")
      return E_INVAL
   
   # can we even proceed to read this?
   filename = job.get_attr( iftfile.JOB_ATTR_SRC_NAME )
   
   # sanity check
   if filename == None:
      iftlog.log(5, "iftsend: filename is not specified!")
      return E_INVAL
   
   if not os.path.exists( filename ):
      iftlog.log(5, "iftsend: file " + filename + " does not exist!")
      return E_FILE_NOT_FOUND
   
   if not (stat.S_IWUSR & os.stat( filename ).st_mode):
      iftlog.log(5, "iftsend: cannot read file " + filename)
      return E_IOERROR
   
   if SEND_FILES_DIR[-1] != "/":
      SEND_FILES_DIR = SEND_FILES_DIR + "/"
   
   if not filename.startswith(SEND_FILES_DIR, 0, len(SEND_FILES_DIR)):
      iftlog.log(5, "iftsend: cannot send file " + filename + ", it is not in " + SEND_FILES_DIR )
      return E_FILE_NOT_FOUND
   
   # prepare to send
   rc, file_hash, chunk_hashes, chunk_data = prepare_sender( job.get_attr( iftfile.JOB_ATTR_SRC_NAME ), job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE) )
   if rc != 0:
      iftlog.log(5, "iftsend: could not prepare to send")
      return rc
   
   job.set_attr( iftfile.JOB_ATTR_GIVEN_CHUNKS, True )      # we will be given chunks out of band
   job.set_meta( iftfile.JOB_ATTR_GIVEN_CHUNKS, Queue.Queue(0) )  # use a blocking queue to get chunks within
   
   # make sure to pass along the file hash
   if job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) == None:
      job.set_attr( iftfile.JOB_ATTR_FILE_HASH, file_hash )
   
   # make sure to pass along the file size
   if job.get_attr( iftfile.JOB_ATTR_FILE_SIZE ) == None:
      job.set_attr( iftfile.JOB_ATTR_FILE_SIZE, os.stat( filename ).st_size )
   
   # make sure to pass along the file type
   if job.get_attr( iftfile.JOB_ATTR_FILE_TYPE ) == None:
      job.set_attr( iftfile.JOB_ATTR_FILE_TYPE, iftstats.fset_filetype( job.attrs ) )
   
   # pass receiver the chunk hashes
   if job.get_attr( iftfile.JOB_ATTR_CHUNK_HASHES ) == None:
      job.set_attr( iftfile.JOB_ATTR_CHUNK_HASHES, chunk_hashes )
      
   job.set_attr( iftfile.JOB_ATTR_SRC_CHUNK_DIR, iftfile.get_chunks_dir( filename, file_hash, True ) )
   
   m = hashlib.sha1()
   m.update( cPickle.dumps( job.attrs ) )
   
   # transmission id is the sha-1 of the job attrs
   xmit_id = m.hexdigest()
   
   iftlog.log(3, "iftsend(id: " + xmit_id + "): will send " + str(job.get_attr( iftfile.JOB_ATTR_SRC_NAME )) + " to " + str(job.get_attr( iftfile.JOB_ATTR_DEST_HOST )))
   
   
   # make clones of each of the protocols
   proto_list = []
   for proto_name in PROTOCOLS.keys():
      if proto_name.find("_sender") < 0:
         continue
         
      proto = None
      try:
         # clone the vanilla protocol so we can run more than one concurrently
         proto = copy.deepcopy( PROTOCOLS[proto_name] )
         proto.assign_job( job )
      except Exception, inst:
         iftlog.exception("iftsend: cannot clone " + proto_name + ", skipping...", inst)
         continue
      proto_list.append( proto )
   
   # start up the passive sending protocols
   passive_protos = start_passive_protos( user_job=job, connect_dict=connect_dict, protos=proto_list, timeout=1.0 )
   TransferCore.begin_ift_send( xmit_id, job, chunk_data, job.get_attr( iftfile.JOB_ATTR_CHUNK_TIMEOUT ), connect_dict )
   TransferCore.run_ift_send_passive( xmit_id, job, passive_protos, user_timeout )
   
   iftlog.log(1, "iftsend: passive senders started: " + str([p.name for p in passive_protos]))
   
   # expect acknowledgement from the receiver...
   TransferCore.add_receiver_ack( xmit_id )
   
   # send our data to the receiver so it can begin listening
   recv_xmlrpc = make_XMLRPC_client( host=recv_host, port=iftd_remote_port, xmlrpc_dir=iftd_xmlrpc_path, timeout = user_timeout )
   
   xmlrpc_response_time = 0
   try:
      from iftdata import USER_PORT
      from iftdata import RPC_DIR
      
      t1 = time.time()
      dat = recv_xmlrpc.recv_iftd_sender_data( xmit_id, job.attrs, available_protocols, connect_dict, chunk_hashes, "http://" + job.get_attr( iftfile.JOB_ATTR_SRC_HOST ) + ":" + str(USER_PORT+1) + "/" + RPC_DIR )
      t2 = time.time()
      xmlrpc_response_time = t2 - t1
      
      if dat[1] == None and dat[2] == None and dat[3] == None:
         iftlog.log(5, "iftsend: could not connect to the receiver!")
         return E_NO_CONNECT
      
      # unpack data
      ack_id = dat[0]
      remote_chunk_dir = dat[1]
      best_proto_name = dat[2]
      available_proto_names = proto_names( dat[3] )
      
      if ack_id != xmit_id:
         iftlog.log(5, "iftsend: Out-of-sequence ACK will not be handled!")
         return E_NO_CONNECT
      
      # store remote chunk dir
      job.set_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR, remote_chunk_dir )
      
      
   except Exception, inst:
      iftlog.exception( "iftsend: could not contact receiver!", inst )
      iftfile.cleanup_chunks_dir( job.get_attr( iftfile.JOB_ATTR_SRC_NAME ), file_hash )
      return E_NO_CONNECT
   
   # supply the chunks data with the remote chunk dir
   for i in xrange(0, len(chunk_data) ):
      chunk_data[i][3] = remote_chunk_dir
   
   # if we don't have a best protocol, then pick one at random
   if best_proto_name == None:
      iftlog.log(3, "iftsend: WARNING: receiver did not supply an ideal protocol, so choosing one at random")
      best_proto_name = random.choice( available_proto_names )
      available_proto_names.remove( best_proto_name )   
   
   
   # start active senders
   active_senders = start_active_protos( user_job=job, connect_dict=connect_dict, protos=proto_list, timeout=xmlrpc_response_time*2 )
   
   rc = TRANSMIT_STATE_SUCCESS
   receiver_rc = None
   if len(active_senders) > 0:
      rc, receiver_rc = TransferCore.run_ift_send_active( xmit_id, job, active_senders, user_timeout, best_proto_name == None )
   
   else:
      receiver_rc = TransferCore.await_receiver_ack( xmit_id, user_timeout )
      
   iftlog.log(1, "iftsend is done!")
   iftfile.cleanup_chunks_dir( job.get_attr( iftfile.JOB_ATTR_SRC_NAME ), file_hash )
   if rc != TRANSMIT_STATE_SUCCESS:
      iftlog.log(5, "iftsend: transmission failed (sender rc = " + str(rc) + ", receiver rc = " + str(receiver_rc) + ")")
      return rc
   else:
      return 0
         
   



def iftreceive(  job, available_protocols, connect_dict=None, iftd_remote_port = USER_PORT + 1, iftd_xmlrpc_path = "/RPC2", user_timeout = 60 ):
   """
   Intelligently receive a file to the local host.
   
   @arg job
      This is an iftjob instance containing the job data
   
   @arg available_protocols
      This is a list of strings of each available (usable) protocol to use
   
   @arg connect_dict
      This is a dictionary of dictionaries mapping protocol names in available_protocols to connection attribute dictionaries.
   
   @arg iftd_remote_port
      Remote IFTD XML-RPC port (default is USER_PORT+1)
   
   @arg iftd_xmlrpc_path
      Remote IFTD XML-RPC domain (default is /RPC2)
   
   @arg user_timeout
      Timeout for XML-RPC calls
   """
   
   
   from iftdata import RECV_FILES_DIR
   job_attrs = job.attrs
   
   if RECV_FILES_DIR[-1] != "/":
      RECV_FILES_DIR = RECV_FILES_DIR + "/"
      
   # don't even bother if this isn't to the right path
   if not os.path.abspath( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ) ).startswith( RECV_FILES_DIR, 0, len(RECV_FILES_DIR)):
      iftlog.log(5, "iftreceive: will not receive to " + str(job.get_attr( iftfile.JOB_ATTR_DEST_NAME )) + ", since it is not in " + RECV_FILES_DIR)
      return E_INVAL


   send_host = job.get_attr( iftfile.JOB_ATTR_SRC_HOST )
   job_str = cPickle.dumps( job.attrs )
   m = hashlib.sha1()
   m.update( job_str )
   xmit_id = m.hexdigest()

   # ask sender for available protocols
   sender_available_protos = []
   active_flags = []
   send_xmlrpc = make_XMLRPC_client( host=send_host, port=iftd_remote_port, xmlrpc_dir=iftd_xmlrpc_path, timeout = user_timeout )
   
   xmlrpc_response_time = 0
   remote_iftd = True
   if job_attrs.get(iftfile.JOB_ATTR_REMOTE_IFTD) == False:
      remote_iftd = False

   best_proto = None
   try:
      if remote_iftd == False:
         raise socket.error(111)

      t1 = time.time()
      rc, remote_chunk_dir, file_size, file_hash, file_type, sender_available_protos, active_flags, chunk_hashes = send_xmlrpc.get_iftd_sender_data( xmit_id, job.attrs, receivers( available_protocols ), connect_dict )
      t2 = time.time()
      xmlrpc_response_time = t2 - t1      # how long did it take to respond?  use this to calculate receiver startup delay
      if rc != xmit_id:
         iftlog.log(5, "iftreceive: ERROR: corrupt data from sender!")
         return E_NO_CONNECT
      
      # record remote dir
      job.set_attr( iftfile.JOB_ATTR_SRC_CHUNK_DIR, remote_chunk_dir )
      
      # is the size tolerable?
      if job.get_attr( iftfile.JOB_ATTR_FILE_SIZE ) != None and job.get_attr( iftfile.JOB_ATTR_FILE_SIZE ) != file_size:
         iftlog.log(5, "iftreceive: ERROR: file size reported by remote host (" + str(file_size) + ") does not match given file size of " + str(job.get_attr( iftfile.JOB_ATTR_FILE_SIZE )) + "!")
         return E_NO_CONNECT
      
      if job.get_attr( iftfile.JOB_ATTR_FILE_MIN_SIZE ) != None and job.get_attr( iftfile.JOB_ATTR_FILE_MIN_SIZE ) > file_size:
         iftlog.log(5, "iftreceive: ERROR: file size reported by remote host (" + str(file_size) + ") is smaller than minimum file size of " + str(job.get_attr( iftfile.JOB_ATTR_FILE_MIN_SIZE )) + "!")
         return E_NO_CONNECT
     
      if job.get_attr( iftfile.JOB_ATTR_FILE_MAX_SIZE ) != None and job.get_attr( iftfile.JOB_ATTR_FILE_MAX_SIZE ) < file_size:
         iftlog.log(5, "iftreceive: ERROR: file size reported by remote host (" + str(file_size) + ") is bigger than maximum file size of " + str(job.get_attr( iftfile.JOB_ATTR_FILE_MAX_SIZE )) + "!")
         return E_NO_CONNECT
      
      # record file size
      job.set_attr( iftfile.JOB_ATTR_FILE_SIZE, file_size )
      
      # record the file hash
      if job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) == None:
         job.set_attr( iftfile.JOB_ATTR_FILE_HASH, file_hash )
      elif job.get_attr( iftfile.JOB_ATTR_FILE_HASH) != file_hash:
         iftlog.log(5, "iftreceive: ERROR: file hash reported by remote host (" + str(file_hash) + ") does not match given hash of " + str(job.get_attr( iftfile.JOB_ATTR_FILE_HASH) ) + "!")
         return E_NO_CONNECT
      
      # record file type
      if job.get_attr( iftfile.JOB_ATTR_FILE_TYPE ) == None:
         job.set_attr( iftfile.JOB_ATTR_FILE_TYPE, file_type )
      elif job.get_attr( iftfile.JOB_ATTR_FILE_TYPE ) != file_type:
         iftlog.log(5, "iftreceive: ERROR: file type reported by remote host (" + str(file_type) + ") does not match given file type of " + str(job.get_attr( iftfile.JOB_ATTR_FILE_TYPE) ) + "!")
         return E_NO_CONNECT
         
      # record chunk hashes
      job.set_attr( iftfile.JOB_ATTR_CHUNK_HASHES, chunk_hashes )
         
      #print "src chunk dir: " + remote_chunk_dir
      
      iftlog.log(5, "iftreceive: receive " + str(file_size) + " bytes from " + remote_chunk_dir + " via " + str(sender_available_protos))
   except socket.error, inst:
      # if the connection was simply refused or timed out, then there is no remote IFTD.
      # receive the file with any protocol
      iftlog.log(5, "iftreceive: no remote IFTD detected, attempting all active receivers to get " + str(job.get_attr( iftfile.JOB_ATTR_SRC_NAME )) + " from " + str(job.get_attr( iftfile.JOB_ATTR_SRC_HOST )) )
      job.set_attr( iftfile.JOB_ATTR_REMOTE_IFTD, False )
      sender_available_protos = available_protocols
      remote_iftd = False
      pass
         
   except Exception, inst:
      iftlog.exception( "iftreceive: could not get sender data", inst)
      return E_NO_CONNECT
   

   proto_active_table = {}
   
   if remote_iftd:
      for i in xrange( 0, len(sender_available_protos) ):
         proto_active_table[sender_available_protos[i]] = active_flags[i]      # map each protocol to its active or inactive boolean
      
   usable_protos = proto_names( available_protocols )
   if remote_iftd:
      if len(available_protocols) > 1:    # we have a choice...
         # now we have the protocols available to both of us.
         # get the best protocol
         features = iftstats.extract_features( job.attrs )
         best_proto = iftstats.best_protocol( features )
         if best_proto != None:
            usable_protos = proto_names( [best_proto] + sender_available_protos )
      
         
      # make chunk directory in preparation for receiving pieces (i.e. from an active sender)
      rc = iftfile.make_chunks_dir( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) )
      if rc != 0:
         iftlog.log(5, "iftreceive: could not make chunks directory")
         return rc
      
      # record local chunk dir
      job.set_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR, iftfile.get_chunks_dir( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.get_attr( iftfile.JOB_ATTR_FILE_HASH ), remote_iftd ))
      
   # start up a transfer processor with the available protocols
   proto_instances = []
   
   iftlog.log(1, "iftreceive: available receivers are " + str([proto + "_receiver" for proto in usable_protos]))
   iftlog.log(1, "iftreceive: file chunksize is " + str(job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE ) ) )
   
   for proto in usable_protos:
      proto = proto + "_receiver"   # if it's available, then there's a receiver available
      p = None
      try:
         p = copy.deepcopy( PROTOCOLS[proto] )
      except Exception, inst:
         iftlog.log(5, "iftreceive: ERROR: could not clone protocol " + proto)
         continue
      
      proto_instances.append( p )
   

   # make sure the directory exists
   if not os.path.exists( job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ) ):
      iftlog.log(1, "WARNING: path " + job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ) + " does not exist, creating...")
      os.system("mkdir -p " + job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ))
   
   elif not os.path.isdir( job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ) ):
      iftlog.log(1, "WARNING: path " + job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ) + " exists and is not a directory...")
      path = job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ) + ".iftd_" + str(os.getpid())
      job.set_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR, path )
      
   
   # get an iftfile reference
   iftfile_ref = iftfile.acquire_iftfile_recv( xmit_id, job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.attrs )
   job.set_attr( iftfile.JOB_ATTR_IFTFILE, iftfile_ref ) 
   
   # start up the receiving protocols
   iftlog.log(1, "iftreceive: protocol instances: " + str([p.name for p in proto_instances]))
   connected_protos = start_protos( user_job=job, connect_dict=connect_dict, protos=proto_instances, timeout=xmlrpc_response_time*2 )
   connected_proto_names = [p.name for p in connected_protos]
   if len(connected_proto_names) == 0:
      iftlog.log(5, "iftreceive: no receiving protocols could be activated")
      iftfile.release_iftfile_recv( xmit_id, iftfile_ref.path )
      iftfile.cleanup_chunks_dir( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) )
      return E_NO_CONNECT

   # begin listening for the sender (both actively and passively)
   rc = TransferCore.begin_ift_recv( xmit_id, job, connected_protos, remote_iftd, -1, job.get_attr( iftfile.JOB_ATTR_TRANSFER_TIMEOUT ) )
   if rc != 0:
      iftlog.log(1, "iftreceive: begin_ift_recv rc = " + str(rc))
      TransferCore.cleanup_recv( xmit_id )
      iftfile.release_iftfile_recv( xmit_id, iftfile_ref.path )
      iftfile.cleanup_chunks_dir( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) )
      return rc
      
   
   # tell active senders to start up
   if remote_iftd:
      
      # if both sender and receiver are active, prevent the active sender from starting.
      sender_known_protocols = []
      for p in proto_instances:
         proto_generic_name = proto_names( [p.name] )[0]
         proto_sender_name = proto_generic_name + "_sender"
         
         if not proto_active_table.has_key( proto_sender_name ):
            continue
         
         iftlog.log(1, p.name + " active=" + str(p.isactive()) + ", " + proto_sender_name + " active=" + str(proto_active_table.get(proto_sender_name)))
         if not (p.isactive() and proto_active_table[ proto_sender_name ] == True):
            sender_known_protocols.append( p.name )
         
      iftlog.log(1, "iftreceive: informing senders about " + str(proto_names(sender_known_protocols)))
      # inform the sender of our choice
      try:
         bp = best_proto
         if best_proto != None:
            bp = proto_names( [best_proto] )[0]
            
         # temporarily remove the iftfile reference in the job, since we can't send semaphores
         del job.attrs[ iftfile.JOB_ATTR_IFTFILE ]
         rc = send_xmlrpc.send_iftd_receiver_choice( xmit_id, job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ), bp, proto_names( sender_known_protocols ) )
         job.attrs[ iftfile.JOB_ATTR_IFTFILE ] = iftfile_ref
         if rc != xmit_id or chunk_hashes == None:
            iftlog.log(5, "iftreceive: ERROR: could not inform sender of our protocol choices!" )
            TransferCore.cleanup_recv( xmit_id )
            iftfile.release_iftfile_recv( xmit_id, iftfile_ref.path )
            iftfile.cleanup_chunks_dir( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) )
            return E_NO_CONNECT
         
         
      except Exception, inst:
         iftlog.exception( "iftreceive: could not begin to receive", inst)
         TransferCore.cleanup_recv( xmit_id )
         iftfile.release_iftfile_recv( xmit_id, iftfile_ref.path )
         iftfile.cleanup_chunks_dir( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) )
         return E_UNHANDLED_EXCEPTION
   
   # drive the receiver process!
   TransferCore.finish_negotiation( xmit_id )
   rc = TransferCore.run_ift_recv( xmit_id, send_xmlrpc )
   iftfile.release_iftfile_recv( xmit_id, iftfile_ref.path )
   iftfile.cleanup_chunks_dir( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) )
   
   if rc == TRANSMIT_STATE_SUCCESS:
      return 0
   else:
      iftlog.log(1, "iftreceive: run_ift_recv rc = " + str(rc))
      return rc
      
   



def prepare_sender( filename, chunksize ):
   """
   Prepare the sender to send.
   This includes creating all the chunks.
   Give back file and chunk data
   """
   
   
   if chunksize == None:
      chunksize = iftfile.DEFAULT_FILE_CHUNKSIZE
   
   file_hash = None
   chunk_hashes = None
   chunk_data = []    # array of (chunk data, chunk id, chunk local path, chunk remote path)
   
   # do we need to chunk the file?  i.e. is there at least one protocol that needs us to do chunking in advance?
   rc, file_hash, chunk_hashes, chunk_paths = iftfile.make_chunks( filename, chunksize )
   
   if rc != 0:
      iftlog.log(5, "prepare_sender: chunking file " + filename + " failed! (rc = " + str(rc) + ")" )
      cleanup_sender( filename, file_hash )
      return (rc, None, None, None)
   
   chunk_id = 0
   for chunk_path in chunk_paths:
      
      # get the chunk
      
      chunk = None
      try:
         fd = open(chunk_path, "rb" )
         chunk = fd.read()
         fd.close()
      except Exception, inst:
         iftlog.exception( "prepare_sender: could not read " + chunk_path, inst )
         return E_IOERROR
      
      chunk_data.append( [chunk, chunk_id, chunk_path, None] )
      chunk_id += 1
   
   
      
   return (rc, file_hash, chunk_hashes, chunk_data)
   



def recv_iftd_sender_data( xmit_id, job_attrs, available_protos, receiver_connect_dict, chunk_hashes, sender_xmlrpc_url ):
   """
   Called by the iftd sender (remote) on the iftd receiver (local), this begins
   an intelligent file transmission.
   
   @arg xmit_id:
      Numerical ID of this transfer (the SHA-1 hash of the job)
   
   @arg job_attrs:
      Known data about the file (dictionary).  Should contain JOB_ATTR_SRC_NAME and JOB_ATTR_FILE_HASH
      
   @arg available_protocols:
      List of protocol names that are available to the sender (these are all senders, and they don't end in "_sender")
   
   @arg receiver_connect_dict:
      Dictionary mapping receiver protocol name to connection attributes needed to start up
            
   @arg chunk_hashes:
      List of SHA-1 hashes of each file chunk
      
   @arg sender_xmlrpc_url
      URL for the receiver to contact the sender to ACK the transmission

   """
   
   error_rc = [xmit_id, None, None, None, None]
   
   # sanity check
   if iftfile.JOB_ATTR_SRC_NAME not in job_attrs or iftfile.JOB_ATTR_FILE_HASH not in job_attrs:
      return error_rc
   
   from iftdata import RECV_FILES_DIR
   
   # access control check
   if not os.path.abspath(job_attrs.get( iftfile.JOB_ATTR_DEST_NAME )).startswith( RECV_FILES_DIR, 0, len(RECV_FILES_DIR )):
      iftlog.log(5, "recv_iftd_sender_data: request for " + job_attrs.get(iftfile.JOB_ATTR_SRC_NAME) + " to " + job_attrs.get( iftfile.JOB_ATTR_DEST_NAME ) + " cannot be serviced, since it is not going to be sent to " + RECV_FILES_DIR )
      return error_rc

   features = iftstats.extract_features( job_attrs )
   
   my_protos = proto_names( receivers( list_protocols() ) )
   
   # get the rank
   best_proto_name = iftstats.best_protocol( features )
   best_proto = None
   if best_proto_name != None:
      best_proto = proto_names( [best_proto_name] )[0]
   
   available_proto_names = proto_names( available_protos )
   
   # remove any available protocols that we don't have receivers for
   usable_protos = []
   for proto in my_protos:
      if proto in available_proto_names:
         usable_protos.append( proto )
   
   
   # the best protocol may not be usable...
   if best_proto != None and best_proto not in usable_protos:
      best_proto = None
   
   # we're in trouble if there aren't any protocols usable...
   if len(usable_protos) == 0 and best_proto == None:
      return error_rc     # cannot receive!
   
   # make chunk directory in preparation for receiving pieces (i.e. from an active sender)
   rc = iftfile.make_chunks_dir( job_attrs.get( iftfile.JOB_ATTR_DEST_NAME ), job_attrs.get( iftfile.JOB_ATTR_FILE_HASH ) )
   if rc != 0:
      return error_rc  # error!
   
   # start up a transfer processor with the available protocols
   proto_instances = []
   for proto in usable_protos:
      proto = proto + "_receiver"   # if it's available, then there's a receiver available
      p = None
      try:
         p = copy.deepcopy( PROTOCOLS[proto] )
      except Exception, inst:
         iftlog.log(5, "ERROR: could not clone protocol " + proto)
         continue
      
      proto_instances.append( p )
   
   # start up the protocols
   job = iftfile.iftjob( job_attrs )
   
   # get an iftfile reference so we can write chunks
   iftfile_ref = iftfile.acquire_iftfile_recv( xmit_id, job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job_attrs )
   job.set_attr( iftfile.JOB_ATTR_IFTFILE, iftfile_ref )
   
   connected_protos = start_protos( user_job=job, connect_dict=receiver_connect_dict, protos=proto_instances, timeout=5.0 )
   if len(connected_protos) == 0:
      iftlog.log(5, "ERROR: no protocols could be started (tried " + str(usable_protos) + ")")
      return error_rc

   connected_proto_names = [p.name for p in connected_protos]
   if best_proto != None and best_proto not in connected_proto_names:
      best_proto = None    # best protocol failed to connect
   
   if best_proto != None and best_proto in usable_protos:
      usable_protos.remove( best_proto )
      
   if best_proto == None and len(usable_protos) == 0:
      return error_rc    # cannot connect
   
   # begin listening for the sender
   TransferCore.begin_ift_recv(xmit_id, job, connected_protos, True, -1, 1.0, job_attrs.get( iftfile.JOB_ATTR_TRANSFER_TIMEOUT ) )
   #thread.start_new_thread( TransferCore.run_ift_recv, (xmit_id, make_XMLRPC_client2(sender_xmlrpc_url, job_attrs.get( iftfile.JOB_ATTR_CHUNK_TIMEOUT)) ) )
   started = iftutil.ReceiverThreadPool.start_new_thread( TransferCore.run_ift_recv, (xmit_id, make_XMLRPC_client2(sender_xmlrpc_url, job_attrs.get( iftfile.JOB_ATTR_CHUNK_TIMEOUT)) ) )
   if not started:
      iftlog.log(5, "recv_iftd_sender_data: could not start new receiver thread")
      return error_rc
   
   # give back the information
   rc = [xmit_id, iftfile.get_chunks_dir( job_attrs.get( iftfile.JOB_ATTR_DEST_NAME ), job_attrs.get( iftfile.JOB_ATTR_FILE_HASH ), True ), best_proto, connected_proto_names]
   return rc




def get_iftd_sender_data( xmit_id, job_attrs, available_protos, connect_dict ):
   """
   Called by the receiver (remote) on the sender (local) to get
   the sender's capabilities--specifically, which protocols it
   has senders for, and where the chunks will be located.
   
   Return the list of protocols usable to both
   sender and receiver.
   """
   
   global TransferCore
   
   error_rc = (xmit_id, None, None, None, None, None, None, None) 
   file_name = job_attrs.get( iftfile.JOB_ATTR_SRC_NAME )
   user_job = iftfile.iftjob( job_attrs )
   
   # does the file exist?
   if not os.path.exists( file_name ):
      iftlog.log(5, "get_iftd_sender_data: file " + str(file_name) + " does not exist")
      return error_rc     # don't even bother
   
   # is the file readable?
   if not (stat.S_IWUSR & os.stat( file_name ).st_mode):
      iftlog.log(5, "get_iftd_sender_data: file " + str(file_name) + " is not readable")
      return error_rc     # don't bother--can't read
   
   # is the file accessible?
   from iftdata import SEND_FILES_DIR
   if SEND_FILES_DIR[-1] != '/':
      SEND_FILES_DIR = SEND_FILES_DIR + "/"
      
   if not os.path.abspath(file_name).startswith( SEND_FILES_DIR, 0, len(SEND_FILES_DIR)):
      iftlog.log(5, "get_iftd_sender_data: will not send " + str(file_name) + ", it is not in " + SEND_FILES_DIR )
      return error_rc     # access control violation


   # get our available protocols
   my_protos = proto_names( senders( list_protocols() ) )
   other_protos = []
   if available_protos:
      other_protos = proto_names( available_protos )
   
   # calculate intersection between both available protos
   my_protos_set = set( my_protos )
   other_protos_set = set( other_protos )
   usable_protos_set = my_protos_set.intersection( other_protos_set )
   file_size = iftfile.get_filesize( file_name )
 
   iftlog.log(1, "get_iftd_sender_data: file " + str(file_name) + ", size " + str(file_size))
   
   # start my passive senders
   sender_names = senders( list_protocols() )
   proto_insts = []
   for proto in sender_names:
      if PROTOCOLS.get(proto) != None and not PROTOCOLS.get(proto).isactive():
         p = None
         # start this passive sender
         try:
            p = copy.deepcopy( PROTOCOLS.get(proto) )
         except:
            iftlog.log(5, "get_iftd_sender_data: could not start passive sender " + proto)
            continue
         
         proto_insts.append(p)

   expected_fsize = user_job.get_attr( iftfile.JOB_ATTR_FILE_SIZE )
   min_fsize = user_job.get_attr( iftfile.JOB_ATTR_FILE_MIN_SIZE )
   max_fsize = user_job.get_attr( iftfile.JOB_ATTR_FILE_MAX_SIZE )
   
   # do some sanity checking...
   if min_fsize != None and max_fsize != None:
      if file_size < min_fsize or file_size > max_fsize:
         return error_rc      # wrong size expectation
   
   
   if expected_fsize != None and expected_fsize != file_size:
      return error_rc      # wrong size expectation
   

   # set up
   rc, file_hash, chunk_hashes, chunk_data = prepare_sender( file_name, user_job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE ) )
   if rc != 0:
      iftlog.log(5, "get_iftd_sender_data: could not prepare to send")
      return error_rc

   
   user_job.supply_attr( iftfile.JOB_ATTR_FILE_SIZE, file_size )
   user_job.supply_attr( iftfile.JOB_ATTR_FILE_HASH, file_hash )
   user_job.supply_attr( iftfile.JOB_ATTR_FILE_TYPE, iftstats.filetype( file_name ) )

   passive_protos = start_passive_protos( connect_dict, user_job, proto_insts, 1.0 )
   
   
   # start passive protocol handling thread
   TransferCore.begin_ift_send( xmit_id, user_job, chunk_data, user_job.get_attr( iftfile.JOB_ATTR_CHUNK_TIMEOUT ), connect_dict )
   TransferCore.run_ift_send_passive( xmit_id, user_job, passive_protos, user_job.get_attr( iftfile.JOB_ATTR_CHUNK_TIMEOUT ))
   
   proto_mask = [0] * len(sender_names)
   
   for i in xrange(0, len(sender_names)):
      p = sender_names[i]
      if PROTOCOLS[p].isactive():
         proto_mask[i] = True
      else:
         proto_mask[i] = False
   
   return (xmit_id, iftfile.get_chunks_dir( file_name, file_hash, True), file_size, file_hash, iftstats.filetype(file_name), sender_names, proto_mask, chunk_hashes)




def send_iftd_receiver_choice( xmit_id, receiver_chunk_dir, best_proto, available_protos ):
   """
   Called by receiver (remote) on the sender (local) to inform
   the sender to begin actively sending, with the prefered protocol.
   """
   
   # this had better be a valid call
   if not TransferCore.sender_valid_xmit_id( xmit_id ):
      iftlog.log(5, "iftapi.send_iftd_receiver_choice: invalid xmit ID " + str(xmit_id))
      return None
   
   connect_dict = TransferCore.get_connection_attrs( xmit_id )
   user_job_attrs = TransferCore.get_job_attrs( xmit_id )
   user_job_attrs[ iftfile.JOB_ATTR_DEST_CHUNK_DIR ] = receiver_chunk_dir
   
   user_job = iftfile.iftjob( user_job_attrs )
   if not available_protos:
      available_protos = []
  
   iftlog.log(1, "send_iftd_receiver_choice: will use " + str(available_protos) + ", where " + str(best_proto) + " is the best" )
   #print "send_iftd_receiver_choice( " + str(xmit_id) + ", " + str(user_job_attrs) + ", " + str(best_proto) + ", " + str(available_protos) + ", " + str(connect_dict) + ")"
   
   usable_protos = []
   if not best_proto:
      usable_protos = available_protos
   else:
      usable_protos = [best_proto] + available_protos
   
   # just in case
   if "unknown" in usable_protos:
      usable_protos.remove("unknown")
      
   if len(usable_protos) != 0:
      # start up a transfer processor with the available active protocols
      proto_instances = []
      for proto in usable_protos:
         proto = proto + "_sender"   # if it's available, then there's a receiver available
         if not PROTOCOLS[proto].isactive():
            continue    # only start up active protocols.
            
         p = None
         try:
            p = copy.deepcopy( PROTOCOLS[proto] )
         except Exception, inst:
            iftlog.log(5, "ERROR: could not clone protocol " + proto)
            continue
         
         proto_instances.append( p )
      
      # start up the active protocols
      connected_protos = start_active_protos( user_job=user_job, connect_dict=connect_dict, protos=proto_instances, timeout=0.01 )
      rc = iftutil.SenderThreadPool.start_new_thread( TransferCore.run_ift_send_active, (xmit_id, user_job, connected_protos, user_job.get_attr( iftfile.JOB_ATTR_TRANSFER_TIMEOUT ), best_proto == None ) )
      if not rc:
         iftlog.log(5, "send_iftd_receiver_choice: could not start new active sender thread")
         xmit_id = None    # error rc
      
   # we're sending as we speak
   return (xmit_id)
   


def ack_sender( xmit_id, rc ):
   """
   Once the remote receiver has everything, it will call this on the local sender to acknowledge it
   so it can clean up.
   """
   TransferCore.do_receiver_ack( xmit_id, rc )
   


   
def list_protocols():
   """
   Get a list of protocol names (as strings) available for use.
   """
   global PROTOCOLS
   if PROTOCOLS:
      return PROTOCOLS.keys()
   else:
      return []


def senders( proto_list ):
   """
   Given a list of protocol names, extract the senders.
   """
   ret = []
   for proto in proto_list:
      if proto.find("_sender") >= 0:
         ret.append(proto)
      
   
   return ret



def receivers( proto_list ):
   """
   Given a list of protocol names, extract the receivers.
   """
   ret = []
   for proto in proto_list:
      if proto.find("_receiver") >= 0:
         ret.append(proto)
      
   
   return ret


def proto_names( proto_list ):
   """
   Given a list of protocol names, remove the _sender or _receiver suffixes.
   """
   ret = []
   for proto in proto_list:
      if "_" in proto:
         ret.append( proto.rsplit("_", 1)[0] )
      else:
         ret.append( proto )
   
   return list(set(ret))
   
"""
Simple XMLRPC function to test connection
"""
def hello_world():
   return 'hello world'


class TimeoutHTTPConnection(httplib.HTTPConnection):

   def connect(self):
       httplib.HTTPConnection.connect(self)
       self.sock.settimeout(self.timeout)
       

class TimeoutHTTP(httplib.HTTP):
   _connection_class = TimeoutHTTPConnection

   def set_timeout(self, timeout):
       self._conn.timeout = timeout

class TimeoutTransport(xmlrpclib.Transport):
   """
   Transport implementation with a socket that can timeout
   """
   def make_connection(self, host):
       conn = TimeoutHTTP(host)
       conn.set_timeout(self.timeout)
       return conn


def make_XMLRPC_client(host="localhost", port=USER_PORT, xmlrpc_dir = "RPC2", timeout=20):
   """
   Create a client interface to the iftd XMLRPC server.
   
   
   @return
      An xmlrpclib.Server instance filled in with the appropriate parameters
      to talk to iftd
   """
   
   t = TimeoutTransport()
   t.timeout = timeout
   if xmlrpc_dir[0] == "/":
      xmlrpc_dir = xmlrpc_dir[1:]
      
   return xmlrpclib.Server("http://" + host + ":" + str(port) + "/" + xmlrpc_dir, allow_none=True, transport = t)
   
   
def make_XMLRPC_client2( url, timeout ):
   """
   Create a client interface to the iftd XMLRPC server
   """
   t = TimeoutTransport()
   t.timeout = timeout
      
   return xmlrpclib.Server(url, allow_none=True, transport = t)
   
   
def pack_attrs( job_attrs, connect_attrs ):
   """
   Given job attributes and connection attributes, convert them into a Pragma
   HTTP header understandable to iftutil.iftd_HTTPServer_handler.
   """
   
   # which protocols?
   proto_list = job_attrs.get(iftfile.JOB_ATTR_PROTOS)
   
   packed_attrs = []
   if proto_list:
      for proto in proto_list:
         packed_attrs.append("protocol=" + str(proto))
      
   # which connection attributes?
   for (proto, connect_dict) in connect_attrs.items():
      for (k, v) in connect_dict.items():
         packed_attrs.append( str(proto) + "=" + str(k) + ":" + str(v))
         
   # load in job attributes
   for (k, v) in job_attrs.items():
      packed_attrs.append( str(k) + "=" + str(v) )
      
   # pack values together into a single string
   packed_str = ""
   for attr in packed_attrs:
      packed_str += attr + "\x01"
      
   packed_str = packed_str[:-1]
   
   return packed_str
   iftfile.acquire_iftfile_recv( self, self.ift_job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), self.ift_job.attrs )
