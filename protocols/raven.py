#!/usr/bin/env python

"""
raven.py
Copyright (c) 2009 Jude Nelson

This package defines a transfer protocol using raven plugins.
Unlike iftraven, this plugin does NOT assume that there is
a remote iftd instance.

DEPENDENCIES: raven!
"""

import tempfile
import os
import os.path
import sys
import protocols

import iftcore
import iftcore.iftsender
import iftcore.iftreceiver
from iftcore.consts import *

import iftfile
import cPickle
import iftlog
import shutil

from iftdata import *

# init_transfer_program() arguments
TRANSFER_PROGRAM_PACKAGE = "RAVEN_TRANSFER_PROGRAM_PACKAGE"
RAVEN_TRANSFER_PACKAGE_DIR = "RAVEN_TRANSFER_PACKAGE_DIR"
INIT_TRANSFER_PROGRAM_ARG1 = "RAVEN_INIT_TRANSFER_PROGRAM_ARG1"
INIT_TRANSFER_PROGRAM_ARG2 = "RAVEN_INIT_TRANSFER_PROGRAM_ARG2"
INIT_TRANSFER_PROGRAM_ARG3 = "RAVEN_INIT_TRANSFER_PROGRAM_ARG3"
INIT_TRANSFER_PROGRAM_ARG4 = "RAVEN_INIT_TRANSFER_PROGRAM_ARG4"
HASH_FUNCS = "RAVEN_HASH_FUNCS"


"""
Sender--does nothing
"""
class raven_sender( iftcore.iftsender.sender ):
   def __init__(self):
      iftcore.iftsender.sender.__init__(self)
      self.name = "raven_sender"
      
      # sender is passive
      self.setactive(False)
      # non-resumable
      self.set_chunking_mode( PROTO_NO_CHUNKING )
   
   def __deepcopy__( self, memo ):
      """
      On deep copy, only replicate the setup attributes.
      Ignore everything else.
      """
      ret = raven_sender()
      ret.name = self.name
      ret.setup_attrs = copy.deepcopy( self.setup_attrs, memo )
      return ret
   
   # need nothing to set up
   def get_setup_attrs(self):
      return [RAVEN_TRANSFER_PACKAGE_DIR]
      
   # we only need what a Raven plugin needs to start up
   def get_connect_attrs(self):
      return []
   
   def get_send_attrs(self):
      return [iftfile.JOB_ATTR_SRC_NAME]
   
   # what attributes does this sender recognize?
   def get_all_attrs( self ):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_send_attrs()
   
   # one-time setup
   def setup( self, setup_attrs ):
      if not setup_attrs.get(RAVEN_TRANSFER_PACKAGE_DIR) in sys.path:
         sys.path.append( setup_attrs.get(RAVEN_TRANSFER_PACKAGE_DIR) )
      return 0    # nothing to really do here
   
   
   # nothing to do
   def send_job( self, job ):
      return 0
   
   # prepare for transmission--make sure the file that will be received is available
   def prepare_transmit( self, job ):
      if not os.path.exists( job.get_attr(iftfile.JOB_ATTR_SRC_NAME) ):
         iftlog.log(5, "raven: file " + job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) + " does not exist!" )
         return E_FILE_NOT_FOUND
      
      if not os.access( job.get_attr(iftfile.JOB_ATTR_SRC_NAME), os.R_OK ):
         iftlog.log(5, "raven: file " + job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) + " is not readable!" )
         return E_FILE_NOT_FOUND
      
      return 0
   
   # clean up 
   def proto_clean( self ):
      return
   
   # transmission has stopped or suspended (either way, kill transmission if it still is going)
   def end_transmit( self, suspend ):
      return 0
   
   # nothing to send; receiver is active
   def send_chunk( self, chunk, chunk_id, chunk_path, remote_chunk_path ):
      # nothing to do; we're passive
      return 0
   
   
"""
The receiver--actively request a file to be received.
"""
class raven_receiver( iftcore.iftreceiver.receiver ):
   
   def __init__(self):
      iftcore.iftreceiver.receiver.__init__(self)
      self.name = "raven_unknown_receiver"
      self.file_to_recv = ""
      self.recved = False
      self.arizonafetch = None
      
      # we're active
      self.setactive(True)
      
      # non-resumable
      self.set_chunking_mode( PROTO_NO_CHUNKING )
   
   def __deepcopy__( self, memo ):
      """
      On deep copy, only replicate the setup attributes.
      Ignore everything else.
      """
      ret = raven_receiver()
      ret.name = self.name
      if self.setup_attrs.has_key( PROTO_PORTNUM ):
         ret.port = self.setup_attrs[ PROTO_PORTNUM ]
         
      ret.arizonafetch = self.arizonafetch
      ret.setup_attrs = copy.deepcopy( self.setup_attrs, memo )
      return ret
   
   
   # get setup attrs--the arguments and package name of the transfer module
   def get_setup_attrs(self):
      return [TRANSFER_PROGRAM_PACKAGE, INIT_TRANSFER_PROGRAM_ARG1, INIT_TRANSFER_PROGRAM_ARG2, INIT_TRANSFER_PROGRAM_ARG3, INIT_TRANSFER_PROGRAM_ARG4, RAVEN_TRANSFER_PACKAGE_DIR]
   
   # Need nothing to connect
   def get_connect_attrs(self):
      return []
   
   def get_recv_attrs( self ):
      # note: supply absolute file paths
      return [iftfile.JOB_ATTR_DEST_NAME, iftfile.JOB_ATTR_SRC_HOST, iftfile.JOB_ATTR_SRC_NAME, iftfile.JOB_ATTR_FILE_SIZE, iftfile.JOB_ATTR_FILE_HASH]
   
   # which attributes to we recognize?
   def get_all_attrs( self ):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_recv_attrs() + [PROTO_PORTNUM, HASH_FUNCS]
   
   # get our port number
   def setup( self, setup_attrs ):
      self.setup_attrs = setup_attrs
      
      rtp_dir = setup_attrs.get(RAVEN_TRANSFER_PACKAGE_DIR)
      if rtp_dir != None and not rtp_dir in sys.path:
         sys.path.append( rtp_dir )
      
      if setup_attrs.has_key( PROTO_PORTNUM ):
         self.port = setup_attrs[ PROTO_PORTNUM ]
      
      if not setup_attrs.has_key( INIT_TRANSFER_PROGRAM_ARG1 ):
         setup_attrs[ INIT_TRANSFER_PROGRAM_ARG1 ] = None
      
      if not setup_attrs.has_key( INIT_TRANSFER_PROGRAM_ARG2 ):
         setup_attrs[ INIT_TRANSFER_PROGRAM_ARG2 ] = None
      
      if not setup_attrs.has_key( INIT_TRANSFER_PROGRAM_ARG3 ):
         setup_attrs[ INIT_TRANSFER_PROGRAM_ARG3 ] = None
      
      if not setup_attrs.has_key( INIT_TRANSFER_PROGRAM_ARG4 ):
         setup_attrs[ INIT_TRANSFER_PROGRAM_ARG4 ] = None
      
      # grab raven's plugins
      try:
         import arizonareport
         exec( "import transfer." + str(setup_attrs[TRANSFER_PROGRAM_PACKAGE]) + " as arizonafetch" )
         
         self.arizonafetch = locals()['arizonafetch']
         
         # arizonaconfig is broken, so we need to make sure it never gets invoked...
         arizonareport.set_verbosity = my_set_verbosity
         arizonareport.get_verbosity = my_get_verbosity
         arizonareport.arizonaconfig = None
         
         # incorporate the transfer name into this protocol name, so we can have many instances of this protocol
         self.name = "raven_" + self.arizonafetch.transfer_name() + "_receiver"
      
         # now initialize it
         try:
            rc = self.arizonafetch.init_transfer_program( setup_attrs[INIT_TRANSFER_PROGRAM_ARG1], setup_attrs[INIT_TRANSFER_PROGRAM_ARG2], setup_attrs[INIT_TRANSFER_PROGRAM_ARG3], setup_attrs[INIT_TRANSFER_PROGRAM_ARG4] )
            if rc == True or rc != False:     # None is appropriate in coblitz 
               return 0
            else:
               return E_NO_CONNECT # problem!
            
         except Exception, inst:
            iftlog.exception( self.name + ": could not init_transfer_program", inst)
            return E_NO_CONNECT
      except Exception, inst:
         iftlog.exception( self.name + ": could not import " + str(setup_attrs[TRANSFER_PROGRAM_PACKAGE]), inst )
         return E_NO_CONNECT
      
      
      return 0
   
   # receive file attributes
   def recv_job( self, job ):
      self.file_to_recv = job.get_attr( iftfile.JOB_ATTR_DEST_NAME )
      self.remote_host = job.get_attr( iftfile.JOB_ATTR_SRC_HOST )
      self.remote_path = job.get_attr( iftfile.JOB_ATTR_SRC_NAME )
      
      # optional; these can be null
      self.raven_file_hash = job.get_attr( iftfile.JOB_ATTR_FILE_HASH )
      self.raven_file_size = job.get_attr( iftfile.JOB_ATTR_FILE_SIZE )
      self.raven_file_hashfuncs = job.get_attr( HASH_FUNCS )
      if self.raven_file_hashfuncs == None:
         self.raven_file_hashfuncs = "default"
      
      return 0
   
   def proto_clean( self ):
      self.recved = False
      return
   
   def recv_files( self, remote_paths, local_dir ):
      
      for rp in remote_paths:
         chunk_id = rp[0]
         remote_path = rp[1]
         
         max_rc = 0
         try:
            # get the file, and store it at the given path
            filedict = {
               "filename":os.path.basename( self.file_to_recv ),
               "size":self.raven_file_size,
               "hash":self.raven_file_hash,
               "hashfuncs":self.raven_file_hashfuncs
            }
            
            # note: file_list is a list of dictionaries in the same format as filedict
            tempdir = tempfile.mkdtemp()
            
            result, file_list = self.arizonafetch.retrieve_files( self.remote_host + "/" + os.path.dirname( remote_path ), [filedict], tempdir, None )
            
            if result == False:
               iftlog.log(5, self.name + ": could not receive file " + remote_path + " from " + self.remote_host)
               max_rc = E_NO_DATA
         
            else:
               bsname = os.path.basename( self.file_to_recv )
               dst = iftfile.get_chunks_dir( bsname, self.raven_file_hash ) + "/" + bsname
               shutil.move( tempdir + "/" + os.path.basename( self.file_to_recv ), dst )
               self.add_file( chunk_id, dst )
            
            
         except Exception, inst:
            iftlog.exception( self.name + ": could not get file!", inst)
            self.recv_finished( TRANSMIT_STATE_FAILURE )
            return (E_UNHANDLED_EXCEPTION, None)
      
      # we finish after this
      os.popen("rm -rf " + tempdir)
            
      return max_rc
   
   # clean up
   def kill( self, kill_args ):
      if self.arizonafetch:
         self.arizonafetch.close_transfer_program()



def my_set_verbosity( val ):
   return
   
def my_get_verbosity():
   return 2
   
   