#!/usr/bin/env python

"""
http.py
Copyright (c) 2009 Jude Nelson

HTTP protocol plugin.
Communicate with a vanilla HTTP server to receive a file.
The sender, if used, merely implements an HTTP 1.1 server.

Unlike ifthttp, this protocol plugin does not require a remote iftd instance.

The receiver actively requests ranges of the file (if the server
is HTTP 1.1) to get chunks, or will download the entire file
and break it into chunks for iftd if the server is an old HTTP 1.0 server.
"""

import urllib
import urllib2
import os
import re
import cgi
import httplib
import thread
import iftlog
import time
import copy
import iftfile
import iftcore
import iftcore.iftsender
import iftcore.iftreceiver
from iftcore.consts import *

from collections import deque
from iftdata import *
from cStringIO import StringIO

from Queue import Queue
import cPickle
from BaseHTTPServer import HTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler

import socket

USE_PARTIAL_GETS = "USE_PARTIAL_GETS"
HTTP_SERVER_VERSION = "HTTP_SERVER_VERSION"

class ift_http_server( HTTPServer ):
   """
   Default IFTD http server (if cherrypy is not available)
   """
      
   # available files (chunk directories)
   available_files = []
   
   def __init__(self, server_addr, handler):
      os.chdir("/")
      self.protocol_version = 'HTTP/1.0'
      HTTPServer.__init__(self, server_addr, handler)


   
class ift_http_request_handler( SimpleHTTPRequestHandler ):
   """
   Request handler specific to iftd.
   """
   
   def do_GET(self):
      # file request?
      try:
         
         # which pieces were requested?
         if True or os.path.dirname( self.path.strip() ) in self.server.available_files:
            if os.path.basename( self.path.strip() ) == "done":
               # receiver is done with this file
               #del self.server.available_files[ os.path.dirname( self.path.strip() ) ]
               self.send_response( 200 )
            
            else:
               # file has been made available
               SimpleHTTPRequestHandler.do_GET(self)
            
            return
         
         else:
            self.send_response(404)    # nothing to send
            return
      
      except Exception, inst:
         iftlog.exception( "Could not fully transmit " + self.path, inst)
         self.send_response(404)
         return
         
         


class http_sender( iftcore.iftsender.sender ):
   """
   http sender--start the server, give out the job, serve until told to stop
   """
   def __init__(self):
      iftcore.iftsender.sender.__init__(self)
      # not active sender
      self.setactive(False)
      self.job_attrs = None
      self.name = "http_sender"
      self.available_files = []
      self.set_chunking_mode( PROTO_DETERMINISTIC_CHUNKING )

   # what do we need to know about setting up?
   def get_setup_attrs(self):
      return [PROTO_PORTNUM]
   
   # what do we need to know about sending?
   def get_connect_attrs(self):
      return []
   
   # what does the sender need to know about the file?
   def get_sender_attrs(self):
      return [iftfile.JOB_ATTR_SRC_NAME, iftfile.JOB_ATTR_FILE_SIZE, iftfile.JOB_ATTR_FILE_HASH]
   
   # what attributes does the sender recognize?
   def get_all_attrs(self):
      return self.get_connect_attrs() + self.get_sender_attrs()
   
   # when we are initialized, we should set up but not start our HTTP server
   def setup( self, setup_attrs ):
      try:
         self.port = setup_attrs[ PROTO_PORTNUM ]
         server_addr = ('', self.port)
         self.http_server = ift_http_server( server_addr, ift_http_request_handler )
         
         thread.start_new_thread( self.http_server.serve_forever, () )
         return 0
      except Exception, inst:
         iftlog.exception( "http_sender.setup: could not start HTTP server", inst )
         return E_NO_CONNECT

   # get the job
   def send_job( self, job ):
      # this file is to be made available
      try:
         self.http_server.available_files.append( job.get_attr( iftfile.JOB_ATTR_SRC_CHUNK_DIR ) )
         return 0
      except Exception, inst:
         iftlog.exception( self.name + ": could not make " + str(job.get_attr( iftfile.JOB_ATTR_SRC_CHUNK_DIR )) + " available", inst)
         return E_UNHANDLED_EXCEPTION
   
   
   # read a file chunk--as in, mark it available
   def send_chunk(self, chunk, chunk_id, chunk_path, remote_chunk_path ):
      
      if chunk != None:
         return len(chunk)
      else:
         return 0
   
   # clean up (but don't shut down)
   def proto_clean(self):
      self.job_attrs = None
      
   # on shutdown, kill the http server
   def kill(self, shutdown_args):
      if self.http_server != None:
         try:
            self.http_server.socket.close()
         except Exception, inst:
            iftlog.exception( "http_sender: could not shut down http server", inst)
            
      


"""
http-receiver--actively request chunks of a remote file
"""
class http_receiver( iftcore.iftreceiver.receiver ):
   
   def __init__(self):
      iftcore.iftreceiver.receiver.__init__(self)
      self.name = "http_receiver"
      self.job_attrs = None
      self.remote_host = ""         # URL without the protocol prefix
      self.http_version = 10       # we assume 1.0 until told otherwise
      self.bytes_received = 0
      self.use_chunking = True      # if we know how big the file is, we can use chunking for HTTP 1.1 servers.  Otherwise, we need to get the whole file at once.
      self.received = False         # set to true once the file has been received (if we don't use chunking)
      self.file_size = -1           # size of remote file
      self.portnum = 80       # sensible default
      # receiver is active
      self.setactive(True)
      self.set_chunking_mode( PROTO_DETERMINISTIC_CHUNKING )
   
   # what do we need to know about setting up?
   def get_setup_attrs(self):
      return []
   
   # what do we need to know about connecting?
   def get_connect_attrs(self):
      return [iftfile.JOB_ATTR_SRC_HOST, iftfile.JOB_ATTR_SRC_NAME, iftfile.JOB_ATTR_DEST_NAME]
   
   # what we need to know to receive
   def get_recv_attrs(self):
      return [iftfile.JOB_ATTR_SRC_HOST, iftfile.JOB_ATTR_SRC_NAME, iftfile.JOB_ATTR_DEST_NAME]
   
   # what are the attributes the receiver recognizes?
   def get_all_attrs(self):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_recv_attrs() + [PROTO_PORTNUM, iftfile.JOB_ATTR_CHUNKSIZE, iftfile.JOB_ATTR_FILE_SIZE, iftfile.JOB_ATTR_FILE_HASH, HTTP_SERVER_VERSION]
   
   
   def setup( self, setup_attrs ):
      self.portnum = -1
      if setup_attrs.has_key( PROTO_PORTNUM ):
         self.portnum = setup_attrs[ PROTO_PORTNUM ]
         
      return 0
     
         
   # read data from the job
   def recv_job( self, job ):
      self.job_attrs = job.attrs #copy.deepcopy( job.attrs )    # get a reference so we can check out other data other protocols add 

      if self.job_attrs.has_key( PROTO_PORTNUM ):
         self.portnum = self.job_attrs[ PROTO_PORTNUM ]
      elif self.portnum == -1:
         self.job_attrs[ PROTO_PORTNUM ] = 80
         self.portnum = 80
      
      
      # try to get sender information, such as server version
      try:
         self.remote_host = job.get_attr( iftfile.JOB_ATTR_SRC_HOST ).strip()
         
         # remove http:// and/or ftp:// and/or https://
         if self.remote_host.find( "http://" ) == 0:
            self.remote_host = self.remote_host.lstrip( "http://" )
         
         if self.remote_host.find( "ftp://" ) == 0:
            self.remote_host = self.remote_host.lstrip( "ftp://" )
            
         if self.remote_host.find( "https://" ) == 0:
            self.remote_host = self.remote_host.lstrip( "http://" )
         
         self.file_size = -1
         if self.job_attrs and self.job_attrs.has_key( iftfile.JOB_ATTR_FILE_SIZE ):
            self.file_size = self.job_attrs[ iftfile.JOB_ATTR_FILE_SIZE ]
            
          
         if self.file_size < 0:
            # can't do chunking...
            self.set_chunking_mode( PROTO_NO_CHUNKING )
         
      except Exception, inst:
         iftlog.exception( "http: could not await_sender", inst)
         return E_UNHANDLED_EXCEPTION
         
      return 0
   
   # wait for a sender
   def await_sender( self, connect_attrs, connect_timeout ):
      
      self.connect_args = copy.deepcopy(connect_attrs)
      
      # supply port number and chunk size if not given
      if not self.connect_args.has_key( PROTO_PORTNUM ):
         self.connect_args[ PROTO_PORTNUM ] = 80
         
      if not self.connect_args.has_key( iftfile.JOB_ATTR_CHUNKSIZE ):
         self.connect_args[ iftfile.JOB_ATTR_CHUNKSIZE ] = iftfile.DEFAULT_FILE_CHUNKSIZE
      
      return 0
   
   
   
   def __write_file( self, path, bytes ):
      try:
         fd = open( path, "wb" )
         fd.write( bytes )
         fd.close()
         return 0
      except Exception, inst:
         iftlog.exception( self.name + ".__write_file: could not write to " + path, inst )
         return E_IOERROR
         
         
   # get files from remote host
   def recv_files( self, remote_file_paths, local_dir ):
      # get each chunk from the remote host
      if local_dir[-1] == '/':
         local_dir = local_dir[:-1]
         
      num_recved = 0
      try:
         
         rc = 0
         write_rc = 0
         for file_id, file_path in remote_file_paths:
            connection = urllib2.Request( "http://" + self.remote_host + ":" + str(self.portnum) + file_path )
            response = urllib2.urlopen( connection )
            
            if response.code == 200:
               # got chunk
               local_path = local_dir + "/" + os.path.basename( file_path )
               rc = self.__write_file( local_path, response.read() )
               if rc == 0:
                  self.add_file( file_id, local_path )
                  num_recved += 1
               else:
                 iftlog.log(3, self.name + ": ERROR: could not write file to " + str(local_path) + ", file write rc = " + str(rc) )
                 write_rc = rc
                 
            else:
               iftlog.log(3, self.name + ": ERROR: could not request file to " + str(local_path) + ", status = " + str(response.code) )
               rc = E_NO_CONNECT
               
            response.close()
         
         if num_recved == 0 and write_rc == 0:
            return E_NO_DATA
         
         if num_recved == 0 and write_rc != 0:
            self.recv_finished( TRANSMIT_STATE_FAILURE )
            
         return write_rc
         
      except urllib2.URLError, inst:
         if inst.args[0][0] == 111:
            iftlog.log(5, self.name + ": 111 connection refused (port " + str(self.portnum) + ")")
            pass
         else:
            iftlog.exception( self.name + ": ERROR: could not get all chunks from " + str(self.remote_host) + ", error on file " + str(file_path), inst)
      except Exception, inst:
         iftlog.exception(self.name + ": ERROR: could not get all chunks from " + str(self.remote_host) + ", error on file " + str(file_path), inst)
         return E_NO_CONNECT
         
     
  
   # get chunks from the remote host.
   # chunks can be actual IFTD-generated chunks, or files in a common directory
   def recv_chunks( self, remote_chunk_dir, desired_chunks ):
      
      # receive chunks in fragments, since no remote IFTD and the server is HTTP/1.1
      try:
         byte_ranges = []     # determine byte ranges corresponding to the chunk
         
         if len(desired_chunks) > 1:
            desired_chunks.sort()
            curr_range = -1
            for chunk in desired_chunks:
               byte_ranges.append( [ self.job_attrs.get( iftfile.JOB_ATTR_CHUNKSIZE ) * chunk, min( self.job_attrs.get( iftfile.JOB_ATTR_CHUNKSIZE ) * (chunk+1) - 1, self.file_size - 1 ) ] )
         
         else:
            byte_ranges = [[ self.job_attrs.get( iftfile.JOB_ATTR_CHUNKSIZE ) * desired_chunks[0], min( self.job_attrs.get( iftfile.JOB_ATTR_CHUNKSIZE ) * (desired_chunks[0]+1) - 1, self.file_size - 1 ) ]]
            
         # translate the byte range into a string
         byte_range_str = "bytes="
         
         for brange in byte_ranges:
            byte_range_str += str(brange[0]) + "-" + str(brange[1]) + ","

         byte_range_str = byte_range_str[:-1]      # remove last ,
         
         remote_file = self.job_attrs.get( iftfile.JOB_ATTR_SRC_NAME )
         if remote_file[0] != '/':
            remote_file = '/' + remote_file
         
         iftlog.log(3, self.name + ": request " + byte_range_str + " of " + remote_file + " from " + self.job_attrs.get( iftfile.JOB_ATTR_SRC_HOST ) )
         
         req = urllib2.Request( "http://" + self.job_attrs.get( iftfile.JOB_ATTR_SRC_HOST ) + ":" + str(self.portnum) + remote_file )
         req.add_header( "range", byte_range_str )
         resp = urllib2.urlopen( req )
         
         if resp.code < 200 or resp.code > 400:
            # error!
            return E_NO_CONNECT
         
         # get the data
         #resp_data = resp.read()
         #data = StringIO( resp_data )
         #resp.close()
         data = resp

         num_chunks = 0

         content_type = resp.headers.getheader('content-type')
         content_range = resp.headers.getheader('content-range')
         if "multipart/byteranges" in content_type:
            # multipart response
            # get xxxxx from boundary=xxxxx

            boundary = content_type[ content_type.find("boundary=") + 9 : ].strip()
            chunk_dict = {}
            CRLF = "\r\n"
            
            # read the data
            read_offset = 0
            while True:

               line = data.readline()
               if len(line) == 0:
                  break

               if line == CRLF:
                  continue
               
               line = line[ : len(line) - 2 ]
               
               start_byte = -1
               stop_byte = -1
               
               if line == "--" + boundary + "--":
                  break
               
               elif line == "--" + boundary:
                  while True:
                     line = data.readline()
                     if line.lower().startswith("content-range"):
                        content_range = line[ line.find("bytes ") + 6 : line.find("/") ]
                        start_byte = int( content_range[0: content_range.find("-")] )
                        stop_byte = int( content_range[ content_range.find("-") + 1 : ] ) + 1
                        
                     if line == CRLF:
                        break
                     
                  
                  # read data
                  chunk_id = start_byte / self.job_attrs.get( iftfile.JOB_ATTR_CHUNKSIZE )
                  #t1 = time.time()
                  chunk = data.read( stop_byte - start_byte )
                  #t2 = time.time()
                  #print "chunk took " + str(t2 - t1)
                  num_chunks += 1
                  self.add_chunk( chunk_id, chunk )
            
            if num_chunks > 0:
               resp.close()
               return 0
            else:
               resp.close()
               return E_NO_DATA
         
         elif content_range != None:
            # only one range given, and it's only one chunk
            start_byte = int( content_range[ 6 : content_range.find("-") ] )
            chunk_id = start_byte / self.job_attrs.get( iftfile.JOB_ATTR_CHUNKSIZE )
            self.add_chunk( chunk_id, resp.read() )
            resp.close()
            return 0

         # not a multipart response, even though we expected it!
         else:
            # byterange request either encompassed the whole file, or this was an HTTP/1.0 server and we got the whole file back.
            local_path = iftfile.get_chunks_dir( self.job_attrs.get( iftfile.JOB_ATTR_DEST_NAME ), self.job_attrs.get( iftfile.JOB_ATTR_FILE_HASH ) ) + "/" + os.path.basename( self.job_attrs.get( iftfile.JOB_ATTR_DEST_NAME ) )
            rc = self.__write_file( local_path, resp.read() )
            if rc == 0:
               self.whole_file( local_path )
            else:
               iftlog.log(5, self.name + ": could not save " + local_path + ", rc = " + str(rc) )
               self.recv_finished( TRANSMIT_STATE_FAILURE )
               
            return rc
            
      except Exception, inst:
         iftlog.exception( self.name + ": could not receive chunks " + str(desired_chunks), inst)
         return E_UNHANDLED_EXCEPTION
            
            
   # clean up
   def proto_clean(self):
      # let the remote host know to remove access to this file
      if self.job_attrs and self.job_attrs.get( iftfile.JOB_ATTR_REMOTE_IFTD ):
         try:
            connection = httplib.HTTPConnection( self.remote_host + ":" + str(self.portnum) )
            connection.request( "GET", os.path.join( self.job_attrs.get( iftfile.JOB_ATTR_SRC_CHUNK_DIR ), "done") )
            response = connection.getresponse()
            if response.status != 200:
               iftlog.log(5, self.name + ": WARNING: could not tell sender to stop sending!")
               
            response.close()
            connection.close()
         except socket.error, inst:
            if inst.args[0] == 111:
               iftlog.log(5, self.name + ".proto_clean: 111 connection refused (port " + str(self.portnum) + ")")
               pass
            else:
               iftlog.exception( self.name + ": WARNING: could not tell sender to stop sending!  Exception encountered.", inst)
         except Exception, inst:
            iftlog.exception( self.name + ": WARNING: could not tell sender to stop sending!  Exception encountered.", inst)
            pass
         
      
      self.job_attrs = None
      self.remote_host = ""
      self.bytes_received = 0
      self.http_version = 10
      if self.use_chunking == False:
         # restore IFTD's automatic capabilities
         self.use_chunking = True
   

