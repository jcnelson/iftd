#!/usr/bin/env python

"""
iftscp.py
Copyright (c) 2009 Jude Nelson

This package defines a transfer protocol using scp.
It can be both an active sender and an active receiver.
It uses its own chunking and file I/O schemes.
There is 1 chunk--the entire file.

DEPENDENCIES: both sender and receiver must have sshd and scp.
"""


import os
import sys
import protocols

import iftcore
import iftcore.iftsender
import iftcore.iftreceiver
from iftcore.consts import *

import iftfile
import cPickle
import iftlog
import copy
from iftdata import *

import subprocess

# option to supply identity file to scp
IFTSCP_IDENTITY_FILE = "IFTSCP_IDENTITY_FILE"

# option to supply remote login name
IFTSCP_REMOTE_LOGIN = "IFTSCP_REMOTE_LOGIN"

"""
Sender--SCP a file over
"""
class iftscp_sender( iftcore.iftsender.sender ):
   def __init__(self):
      iftcore.iftsender.sender.__init__(self)
      self.name = "iftscp_sender"
      self.port = 22       # default port
      self.file_to_send = ""
      self.remote_host = ""
      self.remote_path = ""
      self.identity_file = ""
      
      # sender is active
      self.setactive(True)
      
      # non-resumable
      self.set_chunking_mode( PROTO_NO_CHUNKING )
   
   
   # need nothing to set up
   def get_setup_attrs(self):
      return []
   
   def get_connect_attrs(self):
      return []    # need nothing by default to connect
   
   def get_send_attrs(self):
      return [iftfile.JOB_ATTR_SRC_NAME, iftfile.JOB_ATTR_DEST_HOST, iftfile.JOB_ATTR_DEST_NAME]
   
   # what attributes does the sender recognize?
   def get_all_attrs(self):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_send_attrs() + [PROTO_PORTNUM, IFTSCP_IDENTIFY_FILE, IFTSCP_REMOTE_LOGIN]
   
   # one-time setup
   def setup( self, setup_attrs ):
      try:
         if setup_attrs.has_key( PROTO_PORTNUM ):
            self.port = setup_attrs[PROTO_PORTNUM]
         
         if setup_attrs.has_key( IFTSCP_IDENTITY_FILE ):
            self.identity_file = setup_attrs[ IFTSCP_IDENTITY_FILE ]
            
         iftlog.log(1, "iftscp_sender.setup: will send on port " + str(self.port))
      except Exception, inst:
         return E_NO_VALUE
      
      return 0    # nothing to really do here
   
   
   # get connection attributes
   def await_receiver( self, connection_attrs, timeout ):
      p = connection_attrs.get( PROTO_PORTNUM )
      if p != None:
        self.port = p
       
      return 0
   
   # send job attrs to receiver
   def send_job( self, job ):
      # need nothing, set nothing
      return 0
   
   # prepare for transmission--get the path to the file we'll send
   def prepare_transmit( self, job ):
      # NOW get the data
      self.file_to_send = job.get_attr( iftfile.JOB_ATTR_SRC_NAME )
      self.remote_host = job.get_attr( iftfile.JOB_ATTR_DEST_HOST )
      self.remote_path = job.get_attr( iftfile.JOB_ATTR_DEST_NAME )
      self.remote_user = job.get_attr( IFTSCP_REMOTE_LOGIN )
      if self.remote_user == None:
         self.remote_user = os.getlogin()
      return 0
   
   # clean up 
   def proto_clean( self ):
      self.port = 22
      return
   
   # transmission has stopped or suspended (either way, kill transmission if it still is going)
   def end_transmit( self, suspend ):
      return 0
   
   # send the whole file, and tell iftd we sent every chunk (e.g. return 0 to indicate there are 0 bytes left to send)
   def send_chunk( self, chunk, chunk_id, chunk_path, remote_chunk_path ):
      # if chunk paths are given, then we are supposed to send just the chunks, not the file entirely
      
      local = self.file_to_send
      remote = self.remote_path
      
      if chunk_path:
         local = chunk_path
         remote = remote_chunk_path
         
      # shell out and scp
      cmd = "/usr/bin/scp -P " + str(self.port)
      if self.identity_file != "":
         cmd += "-i " + self.identity_file
      
      cmd += " " + local + " " + self.remote_user + "@" + self.remote_host + ":" + remote
      
      iftlog.log(1, self.name + ": " + cmd)
      pipe = os.popen( cmd )
      rc = pipe.close()
      #pipe = subprocess.Popen( cmd )
      #rc = pipe.wait()
      if rc != None:
         iftlog.log(5, "iftscp_sender: scp returned " + str(rc))
         return -rc
      
      return 0
   
   
"""
The receiver--actively request a file to be received.
"""
class iftscp_receiver( iftcore.iftreceiver.receiver ):
   
   def __init__(self):
      iftcore.iftreceiver.receiver.__init__(self)
      self.name = "iftscp_receiver"
      self.port = 22    # default SCP port
      self.file_to_recv = ""
      self.identity_file = ""
      self.remote_user = os.getenv("USER")
      if self.remote_user == None:
         self.remote_user = "nobody"
      
      # we're active
      self.setactive(True)
      
      # we're not resumable
      self.set_chunking_mode( PROTO_NO_CHUNKING )
   
   
   # need nothing to set up
   def get_setup_attrs(self):
      return []
   
   # give everything up front so we can create a fake job
   def get_connect_attrs(self):
      return []
    
   def get_recv_attrs( self ):
      return [iftfile.JOB_ATTR_DEST_NAME, iftfile.JOB_ATTR_SRC_HOST, iftfile.JOB_ATTR_SRC_NAME]     # we're covered by get_connect_attrs
   
   # which attributes does the receiver recognize?
   def get_all_attrs( self ):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_recv_attrs() + [PROTO_PORTNUM, IFTSCP_IDENTIFY_FILE, IFTSCP_REMOTE_LOGIN, iftfile.JOB_ATTR_CHUNKSIZE, iftfile.JOB_ATTR_FILE_HASH]
   
   # one-time setup
   def setup( self, connection_attrs ):
      return 0
   
   
   # per-file setup--get our port number, etc.
   # overwrite what's in the job, as given in recv_job
   def await_sender( self, connection_attrs, timeout ):
      if connection_attrs == None:
         return 0

      self.connect_args = connection_attrs
      
      if connection_attrs.has_key( PROTO_PORTNUM ):
         self.port = connection_attrs[ PROTO_PORTNUM ]
      
      if connection_attrs.has_key( IFTSCP_IDENTITY_FILE ):
         self.identity_file = connection_attrs[ IFTSCP_IDENTITY_FILE ]
         
      if connection_attrs.has_key( IFTSCP_REMOTE_LOGIN ) and connection_attrs[ IFTSCP_REMOTE_LOGIN ] != None:
         self.remote_user = connection_attrs[ IFTSCP_REMOTE_LOGIN ]
      
      return 0
   
   
   # receive file attributes 
   def recv_job( self, job ):
      
      self.file_to_recv = job.get_attr( iftfile.JOB_ATTR_DEST_NAME )
      self.remote_host = job.get_attr( iftfile.JOB_ATTR_SRC_HOST ).strip("/")
      
      if job.get_attr( IFTSCP_REMOTE_LOGIN ) != None:
         self.remote_user = job.get_attr( IFTSCP_REMOTE_LOGIN )
      
      self.chunk_size = job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE )
      
      if job.defined( [IFTSCP_IDENTITY_FILE] ):
         self.identity_file = job.get_attr( IFTSCP_IDENTITY_FILE )

      self.remote_iftd = job.get_attr( iftfile.JOB_ATTR_REMOTE_IFTD )
      self.file_name = job.get_attr( iftfile.JOB_ATTR_DEST_NAME )
      self.file_hash = job.get_attr( iftfile.JOB_ATTR_FILE_HASH )
      
      return 0
   
   
   def recv_files( self, remote_files, local_dir ):
      # shell out and scp
      cmd = "/usr/bin/scp -P " + str(self.port)
      if self.identity_file != "":
         cmd += "-i " + self.identity_file + " " 
      
      
      max_rc = 0
      for file_list in remote_files:
         filename = file_list[1]
         this_cmd = copy.copy( cmd )
         
         this_cmd += " " + self.remote_user + "@" + self.remote_host + ":" + os.path.abspath(filename) + " " + os.path.abspath(local_dir) + "/"
         iftlog.log(1, self.name + ": '" + this_cmd + "'")
      
         pipe = os.popen( this_cmd )
         rc = pipe.close()
         #pipe = subprocess.Popen( this_cmd )
         #rc = pipe.wait()
         if rc != None:
            iftlog.log(5, self.name + ": scp returned " + str(rc) + " for file " + str(filename))
            max_rc = E_NO_CONNECT
            continue
         
         else:
            self.add_file( file_list[0], file_list[1])
            
      return max_rc
