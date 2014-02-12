#!/usr/bin/env python

"""
nest_helper IFTD protocol module
Copyright (c) 2010 Jude Nelson

This module is specific to the nest implementation of IFTD.  It runs an HTTP server
which can invoke IFTD to receive files on its behalf, and then foreward them back
to the original requester.  Stork clients do this to get the nest to receive data
for them.

The sender and receiver modules do absolutely nothing.  They're only here so the HTTP
server can start up correctly.
"""

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import os
import copy
import thread
import threading
import heapq
import time
import urllib2
import socket
import iftutil

import iftapi
import iftcore
import iftcore.iftsender
import iftcore.iftreceiver
from iftcore.consts import *

import iftfile
import iftlog
from iftdata import *


HTTP_RECV_DIR = "HTTP_RECV_DIR"
HTTP_PORTNUM = "HTTP_PORTNUM"

http_server = None
http_dir = None

"""
nest_helper_sender:  does nothing (sentinel)
"""
class nest_helper_sender( iftcore.iftsender.sender ):
   def __init__(self):
      iftcore.iftsender.sender.__init__(self)
      self.name = "nest_helper_sender"
      
      # sender does nothing
      self.setactive(False)
      
      # non-resumable
      self.set_chunking_mode( PROTO_NO_CHUNKING )
      
   
   # we need nothing to start up
   def get_setup_attrs(self):
      return []
   
   # we need nothing to connect
   def get_connect_attrs(self):
      return []
   
   # we need the source name to send the file to the cache
   def get_send_attrs(self):
      return []
   
   # what are the attributes this sender recognizes?
   def get_all_attrs(self):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_send_attrs(self)
   
   # one-time setup
   def setup( self, setup_attrs ):
      iftlog.log(5, "nest_helper_sender is supposed to do nothing...")
      return E_NOT_IMPLEMENTED
      
   
   # nothing to do
   def send_job( self, job ):
      return E_NOT_IMPLEMENTED
   
   # do nothing
   def prepare_transmit( self, job, resume ):
      return E_NOT_IMPLEMENTED
   
   # clean up 
   def proto_clean( self ):
      return
   
   # do nothing
   def end_transmit( self, suspend ):
      return E_NOT_IMPLEMENTED
   
   # do nothing
   def send_chunk( self, chunk, chunk_id, chunk_path, remote_chunk_path ):
      return E_NOT_IMPLEMENTED

      
   # clean up
   def kill( self, kill_args ):
      return 0


""" 
nest_helper_receiver--do nothing except for set up the http server
"""
class nest_helper_receiver( iftcore.iftreceiver.receiver ):
   
   def __init__(self):
      iftcore.iftreceiver.receiver.__init__(self)
      self.name = "nest_helper_receiver"
      
      # we're passive
      self.setactive(False)
      
      # we're not resumable
      self.set_chunking_mode( PROTO_NO_CHUNKING )
      
   
   # need to know nothing to set up
   def get_setup_attrs(self):
      return [HTTP_RECV_DIR, HTTP_PORTNUM]
   
   # give everything up front so we can create a fake job
   def get_connect_attrs(self):
      return []
   
   def get_recv_attrs( self ):
      return []
   
   # what attributes does this receiver recognize?
   def get_all_attrs( self ):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_recv_attrs()
   
   
   # start up the cache if it is not running
   def setup( self, setup_attrs ):
      
      # start up the nest helper
      if setup_attrs.has_key(HTTP_RECV_DIR) == False:
         setup_attrs[HTTP_RECV_DIR] = "/tmp/nest_helper_" + str(os.getpid())
      
      self.setup_attrs = setup_attrs
      
      self.http_port = 6648
      if setup_attrs.get( HTTP_PORTNUM ) != None:
         self.http_port = setup_attrs.get( HTTP_PORTNUM )
 
      return stork_server_startup( setup_attrs[HTTP_RECV_DIR], self.http_port )
      
      
   # receive file attributes
   def recv_job( self, job ):
      return 0
   
   def proto_clean( self ):
      return
   
   def recv_files( self, remote_file_paths, local_file_dir ):
      return E_NOT_IMPLEMENTED
   
   # clean up
   def kill( self, kill_args ):
      stork_server_shutdown()



"""
HTTP stork client request handler
Since this server (and this protocol) are on the nest, it must foreward the data to the client.
"""
class HTTPStorkServerHandler( BaseHTTPRequestHandler ):
   
   
   def do_GET(self):
      
      global http_dir 
      
      # only pay attention to local requests
      if self.client_address[0] != "127.0.0.1" and self.client_address[0] != "localhost":
         iftlog.log(5, "HTTPStorkServerHandler: unauthorized GET from " + self.client_address[0])
         self.send_response( 403 )
         return

      filename_and_args = self.path
      
      work = filename_and_args.split('?')
      
      # name of file that was GET'ed
      get_filename = os.path.basename( work[0] )
       
      # arguments
      args = work[1].split('&')
      
      connect_args = {}
      job_attrs = {}
      
      for arg in args:
         work = arg.split('=')
         if work[0] == "connect_args":
            # arg is connect_args listing
            try:
               # strings are sent in raw mode...
               work[1] = work[1].strip("'").decode( 'string-escape' )
               connect_args = iftutil.SafeUnpickler.loads( work[1] )
            except Exception, inst:
               iftlog.exception("HTTPStorkServerHandler: exception unpacking connect_args", inst)
               self.send_response( 400 )
               return
            
         elif work[0] == "job_attrs":
            # arg is job_attrs listing
            try:
               # strings are sent in raw mode...
               work[1] = work[1].strip("'").decode( 'string-escape' )
               job_attrs = iftutil.SafeUnpickler.loads( work[1] )
            except Exception, inst:
               iftlog.exception("HTTPStorkServerHandler: exception unpacking job_attrs", inst )
               self.send_response( 400 )
               return
      
      #connect_args = tmp_connect_args.get( get_filename )
      #job_attrs = tmp_job_attrs.get( get_filename )
      
      if job_attrs == None:
         # not found...
         iftlog.log(5, "HTTPStorkServerHandler: job_attrs not found for '" + get_filename + "'" )
         self.send_response( 404 )
         return
      
      if connect_args == None:
         # not found
         iftlog.log(5, "HTTPStorkServerHandler: connect_args not found for '" + get_filename + "'" )
         print tmp_connect_args
         self.send_response( 404 )
         return
      
      chunks_dir = job_attrs.get( iftfile.JOB_ATTR_DEST_CHUNK_DIR )
      file_name = job_attrs.get(iftfile.JOB_ATTR_DEST_NAME)
      
      # file request for cache miss!
      try:
         # get the file via iftd, but don't check the cache
         all_protolist = iftapi.list_protocols()
         protolist = []
         
         if "iftcache_receiver" in all_protolist:
            protolist = ["iftcache_receiver"]  # only use the cache protocol if it is available
         
         else:
            # don't use any nest_helper protocol
            for p in all_protolist:
               if p.find("nest_helper") == -1:
                  protolist.append( p )
         
         # map each protocol to the given connection args
         proto_connect_args = {}
         for proto_name in protolist:
            proto_connect_args[proto_name] = connect_args
         
         # put in a request to get the file
         iftlog.log(5, "nest_helper: Attempting to receive " + get_filename + " to " + file_name)
         
         # receive to the cache directory
         http_dir_filepath = http_dir.rstrip("/") + "/" + os.path.basename( job_attrs.get( iftfile.JOB_ATTR_DEST_NAME ))
         job_attrs[ iftfile.JOB_ATTR_DEST_NAME ] = http_dir_filepath
         job_attrs[ iftfile.JOB_ATTR_PROTOS ] = protolist
         rc = iftapi.begin_ift( job_attrs, proto_connect_args, False, True, connect_args[iftapi.CONNECT_ATTR_REMOTE_PORT], connect_args[iftapi.CONNECT_ATTR_REMOTE_RPC], connect_args[iftapi.CONNECT_ATTR_USER_TIMEOUT] )
         
         # success or failure?
         if rc != TRANSMIT_STATE_SUCCESS and rc != 0:
            iftlog.log(5, "nest_helper: could not receive file " + file_name + " (rc = " + str(rc) + ")")
            self.send_response(400)
            return
         
         # open the file and write it back
         file_buff = []
         try:
            fd = open( http_dir_filepath, "rb" )
            file_buff = fd.read()
            fd.close()
         except Exception, inst:
            iftlog.exception("nest_helper: received file to " + http_dir_filepath + ", but could not read it")
            self.send_response(500)
            return
         
         # reply the file
         self.send_response(200)
         self.send_header( 'Content-type', 'application/octet-stream' ) # force raw bytes
         self.end_headers()
         self.wfile.write( file_buff )
         
         # recreate the chunks directory, since we might have lost it...
         if chunks_dir != None and not os.path.exists( chunks_dir ):
            try:
               os.popen("mkdir -p " + chunks_dir).close()
            except:
               pass
            
         # done with this...
         return
         
      except Exception, inst:
         iftlog.exception( "nest_helper: could not retrieve " + get_filename, inst)
         self.send_response(500)
         return
         
         
         
def stork_server_startup( http_basedir, http_server_portnum ):
   """
   Start up the stork http server if not running.
   
   @return
      0 on success, negative on failure
   """
   
   global http_dir
   global http_server
   
   try:
   
   # attempt to make base directory, but fall back to a sensible default if that doesn't work
      try:
         if not os.path.exists( http_basedir ):
            os.makedirs( http_basedir )
         else:
            iftlog.log(3, "nest_helper: WARNING: using existing directory " + http_basedir + " as HTTP directory")
         
         http_dir = http_basedir
         
      except Exception, inst:
         iftlog.exception("iftcache: Could not create HTTP directory!", inst)
         cache_sem.release()
         cache_shutdown()
         return E_UNHANDLED_EXCEPTION
      
      # start the HTTP server
      if http_server == None:
         http_server = HTTPServer( ('', http_server_portnum), HTTPStorkServerHandler )
         thread.start_new_thread( http_server.serve_forever, () )
      else:
         # HTTP server is already running...
         iftlog.log(3, "nest_helper: stork server system is already running...")
      
      iftlog.log(3, "nest_helper: started")
   
   except Exception, inst:
      iftlog.exception("nest_helper: could not start server", inst)
      return E_UNHANDLED_EXCEPTION
   
   return 0



def stork_server_shutdown():
   """
   Shut down the stork server system 
   
   @return
      0 on success; negative on failure
   
   """
   
   try:
      # TODO: less kludgy way?
      http_server.socket.close()
      os.popen("rm -rf " + http_dir).close()
   except Exception, inst:
      iftlog.log("nest_helper: could not shut down stork server", inst)
      return E_UNHANDLED_EXCEPTION

   return 0
