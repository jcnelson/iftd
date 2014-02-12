#!/usr/bin/env python

"""
iftcache.py
Copyright (c) 2009 Jude Nelson

Squid proxy plugin.
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

import iftapi
import iftcore
import iftcore.iftsender
import iftcore.iftreceiver
from iftcore.consts import *

import iftfile
import iftlog
from iftdata import *


# file cache base directory
# (the PID will be appended to it to make it unique)
IFTCACHE_BASEDIR = "IFTCACHE_BASEDIR"

# squid port number attribute tag
IFTCACHE_SQUID_PORTNUM = "IFTCACHE_SQUID_PORTNUM"

# http port
IFTCACHE_HTTP_PORTNUM = "IFTCACHE_HTTP_PORTNUM"

# Squid port
IFTCACHE_SQUID_PORT = 31128      # not 3128, since its blocked on PlanetLab

# remote RPC dir
IFTCACHE_REMOTE_RPC_DIR = "IFTCACHE_REMOTE_RPC_DIR"

# remote IFTD port
IFTCACHE_REMOTE_IFTD_PORT = "IFTCACHE_REMOTE_IFTD_PORT"

# user timeout
IFTCACHE_USER_TIMEOUT = "IFTCACHE_USER_TIMEOUT"

# HTTP cache server instance
cache_server = None

# cache reference count
cache_ref = 0

# cache semaphore for reference counting
cache_sem = threading.Semaphore(1)

# cache directory
cache_dir = ""

# maximum allowable age for a file
IFTCACHE_MAX_AGE = "IFTCACHE_MAX_AGE"

# temporary connection attributes and job attributes
# used only in receiving, between the call-out to Squid and the file retrieval.
tmp_job_attrs = {}
tmp_connect_args = {}

"""
Cache sender--put a file into the Squid cache
"""
class iftcache_sender( iftcore.iftsender.sender ):
   def __init__(self):
      iftcore.iftsender.sender.__init__(self)
      self.name = "iftcache_sender"
      self.file_to_send = ""
      self.sent = False
      
      # sender is active
      self.setactive(True)
      
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
      return [iftfile.JOB_ATTR_SRC_NAME]
   
   # what are the attributes this sender recognizes?
   def get_all_attrs(self):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_send_attrs() + [IFTCACHE_BASEDIR]
   
   # one-time setup
   def setup( self, setup_attrs ):
      # start up the cache
      if setup_attrs.has_key(IFTCACHE_BASEDIR) == False:
         setup_attrs[IFTCACHE_BASEDIR] = "/tmp/iftcache_" + str(os.getpid())
         
      self.http_port = 18090
      if setup_attrs.get( IFTCACHE_HTTP_PORTNUM ) != None:
         self.http_port = int(setup_attrs.get( IFTCACHE_HTTP_PORTNUM ) )
         
      return cache_startup( setup_attrs[IFTCACHE_BASEDIR], self.http_port )
      
   
   # nothing to do
   def send_job( self, job ):
      return 0
   
   # prepare for transmission--make sure the file that will be received is available
   def prepare_transmit( self, job, resume ):
      if not os.path.exists( job.get_attr(iftfile.JOB_ATTR_SRC_NAME) ):
         iftlog.log(5, self.name + ": file " + job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) + " does not exist!" )
         return E_FILE_NOT_FOUND
      
      if not os.access( job.get_attr(iftfile.JOB_ATTR_SRC_NAME), os.R_OK ):
         iftlog.log(5, self.name + ": file " + job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) + " is not readable!" )
         return E_FILE_NOT_FOUND
      
      self.file_to_send = job.get_attr( iftfile.JOB_ATTR_SRC_NAME )
      self.squid_portnum = job.get_attr( IFTCACHE_SQUID_PORTNUM )
      if self.squid_portnum == None:
         self.squid_portnum = IFTCACHE_SQUID_PORT

      return 0
   
   # clean up 
   def proto_clean( self ):
      self.sent = False
      self.file_to_send = ""
      return
   
   # transmission has stopped or suspended (either way, kill transmission if it still is going)
   def end_transmit( self, suspend ):
      return 0
   
   # put the file into the cache
   def send_chunk( self, chunk, chunk_id, chunk_path, remote_chunk_path ):
      
      # we cache the entire file, so we'll ignore chunks altogether
      
      rc = cache_put_file( self.file_to_send, self.squid_portnum, self.http_port )
      if rc < 0:
         return E_NO_DATA  # couldn't put the file
      
      self.sent = 1
      return 0

      
   # clean up
   def kill( self, kill_args ):
      cache_shutdown()


"""
Cache receiver--get a file if it is cached and not stale
"""
class iftcache_receiver( iftcore.iftreceiver.receiver ):
   
   def __init__(self):
      iftcore.iftreceiver.receiver.__init__(self)
      self.name = "iftcache_receiver"
      self.file_to_recv = ""
      self.max_age = 0
      self.chunk_size = 0
      
      # we're active
      self.setactive(True)
      # we're not resumable
      self.set_chunking_mode( PROTO_NO_CHUNKING )
      
   
   # get path we save the file to
   def get_local_file_path(self):
      return self.file_to_recv
   
   # need to know nothing to set up
   def get_setup_attrs(self):
      return []
   
   # give everything up front so we can create a fake job
   def get_connect_attrs(self):
      return [IFTCACHE_REMOTE_RPC_DIR, IFTCACHE_REMOTE_IFTD_PORT, IFTCACHE_USER_TIMEOUT]
   
   def get_recv_attrs( self ):
      # note: supply absolute file paths
      return [iftfile.JOB_ATTR_SRC_NAME, iftfile.JOB_ATTR_DEST_NAME]
   
   # what attributes does this receiver recognize?
   def get_all_attrs( self ):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_recv_attrs() + [iftfile.JOB_ATTR_CHUNKSIZE, IFTCACHE_HTTP_PORTNUM, IFTCACHE_SQUID_PORTNUM, IFTCACHE_BASEDIR]
   
   
   # start up the cache if it is not running
   def setup( self, setup_attrs ):
      
      # start up the cache
      if setup_attrs.has_key(IFTCACHE_BASEDIR) == False:
         setup_attrs[IFTCACHE_BASEDIR] = "/tmp/iftcache_" + str(os.getpid())
      
      # port number
      if setup_attrs.has_key(IFTCACHE_SQUID_PORTNUM) == False:
         global IFTCACHE_SQUID_PORT
         setup_attrs[IFTCACHE_SQUID_PORTNUM] = IFTCACHE_SQUID_PORT

      self.setup_attrs = setup_attrs
      
      self.http_port = 18090
      if setup_attrs.get( IFTCACHE_HTTP_PORTNUM ) != None:
         self.http_port = setup_attrs.get( IFTCACHE_HTTP_PORTNUM )
      
      return cache_startup( setup_attrs[IFTCACHE_BASEDIR], self.http_port )
      
   
   def await_sender( self, connect_attrs, timeout ):
      self.connect_args = connect_attrs
      return 0
   
   # receive file attributes
   def recv_job( self, job ):
      
      self.file_to_recv = job.get_attr( iftfile.JOB_ATTR_SRC_NAME )
      self.file_hash = job.get_attr( iftfile.JOB_ATTR_FILE_HASH )
      self.remote_iftd = job.get_attr( iftfile.JOB_ATTR_REMOTE_IFTD )
      self.max_age = job.get_attr( IFTCACHE_MAX_AGE )
      self.job_attrs = job.attrs
      if self.max_age == None:
         self.max_age = -1
      self.chunk_size = job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE )
      self.squid_port = job.get_attr( IFTCACHE_SQUID_PORTNUM )
      if self.squid_port == None:
         self.squid_port = self.setup_attrs[IFTCACHE_SQUID_PORTNUM]
      if self.squid_port == None:
         self.squid_port = IFTCACHE_SQUID_PORT 

      return 0
   
   def proto_clean( self ):
      self.file_to_recv = ""
      self.max_age = 0
      self.chunk_size = 0
      return
   
   def recv_files( self, remote_file_paths, local_file_dir ):
      global cache_dir
      
      # get the file from the cache and write it to disk, if possible
      file_fd = cache_get_file( cache_path(self.file_to_recv), self.max_age, self.connect_args, self.job_attrs, self.squid_port, self.http_port )
      
      if file_fd == None:
         iftlog.log(3, self.name + ": could not receive " + self.file_to_recv )
         self.recv_finished( TRANSMIT_STATE_FAILURE )
         return E_NO_DATA      # not in cache ==> protocol failure
      
      data = file_fd.read()
      file_fd.close()
      
      tmp_file_name = ""
      
      
      try:
         tmp_file_name = local_file_dir + "/" + os.path.basename( self.job_attrs.get( iftfile.JOB_ATTR_DEST_NAME ) )
         fd = open( tmp_file_name, "wb" )
         fd.write( data )
         fd.close()
      except Exception, inst:
         iftlog.exception(self.name + ".recv_files: failed to save " + self.file_to_recv + " to " + tmp_file_name, inst)
         return E_IOERROR
      
      self.whole_file( tmp_file_name )
      self.recv_finished( TRANSMIT_STATE_SUCCESS )
      
      return 0
   
   # clean up
   def kill( self, kill_args ):
      cache_shutdown()



"""
HTTP cache server request handler
Squid will request files from this server when it gets a cache miss.
"""
class HTTPCacheServerHandler( BaseHTTPRequestHandler ):
   
   
   def do_GET(self):
      global cache_dir
      global tmp_connect_args
      global tmp_job_attrs
      
      # only pay attention to local requests
      if self.client_address[0] != "127.0.0.1" and self.client_address[0] != "localhost":
         iftlog.log(5, "HTTPCacheServerHandler: unauthorized GET from " + self.client_address[0])
         self.send_response( 403 )
         return

      get_filename = os.path.basename( self.path )
      
      connect_args = tmp_connect_args.get( get_filename )
      job_attrs = tmp_job_attrs.get( get_filename )
      
      if job_attrs == None:
         # not found...
         iftlog.log(5, "HTTPCacheServerHandler: job_attrs not found for '" + get_filename + "'" )
         self.send_response( 404 )
         return
      
      if connect_args == None:
         # not found
         iftlog.log(5, "HTTPCacheServerHandler: connect_args not found for '" + get_filename + "'" )
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
         
         # don't use any iftcache protocol
         for p in all_protolist:
            if p.find("iftcache") == -1:
               protolist.append( p )
         
         # map each protocol to the given connection args
         proto_connect_args = {}
         for proto_name in protolist:
            proto_connect_args[proto_name] = connect_args
         
         # put in a request to get the file
         iftlog.log(5, "iftcache: Squid cache miss, so trying all other available protocols")
         
         # receive to the cache directory
         cached_filepath = cache_dir.rstrip("/") + "/" + os.path.basename( job_attrs.get( iftfile.JOB_ATTR_DEST_NAME ))
         job_attrs[ iftfile.JOB_ATTR_DEST_NAME ] = cached_filepath
         job_attrs[ iftfile.JOB_ATTR_PROTOS ] = protolist
         rc = iftapi.begin_ift( job_attrs, proto_connect_args, False, True, connect_args[iftapi.CONNECT_ATTR_REMOTE_PORT], connect_args[iftapi.CONNECT_ATTR_REMOTE_RPC], connect_args[iftapi.CONNECT_ATTR_USER_TIMEOUT] )
         
         # success or failure?
         if rc != TRANSMIT_STATE_SUCCESS and rc != 0:
            iftlog.log(5, "iftcache: could not receive file " + file_name + " (rc = " + str(rc) + ")")
            self.send_response(500)
            return
         
         # open the file and write it back
         file_buff = []
         try:
            fd = open( cached_filepath, "rb" )
            file_buff = fd.read()
            fd.close()
         except Exception, inst:
            iftlog.exception("iftcache: received file to " + cached_filepath + ", but could not read it")
            self.send_response(500)
            return
         
         # reply the file
         self.send_response(200)
         self.send_header( 'Content-type', 'application/octet-stream' ) # force raw bytes
         self.send_header( 'Last-Modified', time.ctime( os.stat( cached_filepath ).st_mtime ) )
         self.end_headers()
         self.wfile.write( file_buff )
         
         # recreate the chunks directory, since we might have lost it...
         if chunks_dir != None and not os.path.exists( chunks_dir ):
            try:
               os.popen("mkdir -p " + chunks_dir).close()
            except:
               pass
            
         # done with this...
         tmp_connect_args[get_filename] = None
         tmp_job_attrs[get_filename] = None
         return
         
      except Exception, inst:
         iftlog.exception( "iftcache: could not retrieve " + self.path, inst)
         self.send_response(500)
         



def cache_startup( cache_basedir, cache_server_portnum ):
   """
   Start up the caching system if not running.  Start the HTTP cache server and ensure that Squid is running.
   
   @return
      0 on success, negative on failure
   """
   
   global cache_ref
   global cache_sem
   global cache_server
   global cache_dir
   
   try:
      # are we running?
      cache_sem.acquire()
      if cache_ref != 0 or cache_server != None:
         iftlog.log(3, "iftcache: already running (refs = " + str(cache_ref) + ")")
         cache_sem.release()
         return 0    # already running
   
      # is Squid running?
      # TODO: is there a less kludgy way to do this?
      procs = os.popen( "ps -A | grep -i squid" ).readlines()
      if len(procs) <= 1:
         # only found our "grep" statement
         iftlog.log(5, "iftcache: Squid does not appear to be running...")
         cache_sem.release()
         return E_INVAL
   
      # attempt to make base directory, but fall back to a sensible default if that doesn't work
      try:
         if not os.path.exists( cache_basedir ):
            os.makedirs( cache_basedir )
         else:
            iftlog.log(3, "iftcache: WARNING: using existing directory " + cache_basedir + " as cache directory")
         cache_dir = cache_basedir
      except Exception, inst:
         try:
            os.makedirs( "/tmp/iftcache_" + str(os.getpid()))
            cache_dir = "/tmp/iftcache_" + str(os.getpid())
            iftlog.log(5, "iftcache: WARNING: " + cache_basedir + " is not valid, using /tmp/iftcache_" + str(os.getpid()) + " as base cache directory path")
         except Exception, inst:
            iftlog.exception("iftcache: Could not create cache directory!", inst)
            cache_sem.release()
            cache_shutdown()
            return E_UNHANDLED_EXCEPTION
      
      # start the HTTP server
      if cache_server == None:
         cache_server = HTTPServer( ('', cache_server_portnum), HTTPCacheServerHandler )
         thread.start_new_thread( cache_server.serve_forever, () )
      else:
         # HTTP server is already running...
         iftlog.log(3, "iftcache: caching system is already running...")
      
      iftlog.log(3, "iftcache: started")
      
      cache_ref += 1
      cache_sem.release()
   except Exception, inst:
      iftlog.exception("iftcache: could not start cache", inst)
      return E_UNHANDLED_EXCEPTION
   
   return 0



def cache_shutdown():
   """
   Shut down the caching system if no one is using it
   
   @return
      0 on success; negative on failure
   
   """
   
   global cache_ref
   global cache_sem
   global cache_server
   global cache_dir 
   
   try:
      cache_sem.acquire()
      cache_ref -= 1

      # TODO: less kludgy way?
      os.popen("rm -rf " + cache_dir).close()

      if cache_ref == 0:
         # stop our cache server
         if cache_server != None:
            cache_server.socket.close()

         del cache_server
         cache_server = None
   
   
      
      cache_sem.release()
   except Exception, inst:
      iftlog.log("iftcache: could not shut down cache", inst)
      return E_UNHANDLED_EXCEPTION

   return 0




def cache_purge_file( file_path, http_port ):
   """
   Eliminate the given file from the cache.
   
   @return
      0 on success; negative on error
   """
   
   # tell Squid to lose the file
   rc = os.popen( "squidclient -m PURGE http://127.0.0.1:" + str(http_port) + "/" + os.path.abspath(file_path) ).close()
   
   if rc != 0:
      iftlog.log(5, "iftcache.cache_purge_file: squidclient rc=" + str(rc) )
   
   return rc
   
   


def cache_put_file( file_path, squid_port, http_port ):
   """
   Put the given file into the cache
   
   @return
      0 on success; negative on error
   """
   
   # get the file from HTTPCacheServer through Squid so it goes into Squid's cache
   try:
      file_path = file_path.lstrip('/')
      request = urllib2.Request( "http://127.0.0.1:" + str(http_port) + "/" + file_path )
      request.set_proxy( "127.0.0.1:" + str(squid_port), "http" )
      urllib2.urlopen( request ).close()
      return 0
   
   except Exception, inst:
      iftlog.exception("iftcache.cache_put_file: could not cache file " + file_path, inst)
      return -1
   


def cache_get_file( file_path, max_age, connect_args, job_attrs, squid_port, http_port ):
   """
   Get the given file (file_path) from the cache and return a file handle to it (via urllib2) if it is
   younger than max_age (where max_age is in seconds)
   
   """
   
   # get the file from squid
   global tmp_connect_args
   global tmp_job_attrs
   try:
      if tmp_job_attrs == None:
         tmp_job_attrs = {}
      if tmp_connect_args == None:
         tmp_connect_args = {}
         
      tmp_connect_args[os.path.basename(file_path)] = copy.deepcopy(connect_args)
      tmp_job_attrs[os.path.basename(file_path)] = iftfile.iftjob.get_attrs_copy(job_attrs)
      
      proxy_handler = urllib2.ProxyHandler( {'http': 'http://127.0.0.1:' + str(squid_port)} )
      opener = urllib2.build_opener( proxy_handler )
      
      if max_age > 0:
         opener.addheaders = [("Cache-Control","max-age=" + str(max_age))]
      
      cached_file_fd = opener.open( "http://127.0.0.1:" + str(http_port) + os.path.abspath( file_path ) )
      return cached_file_fd
   
   except urllib2.HTTPError, inst:
      iftlog.log(1, "iftcache.cache_get_file: file not in cache")
      tmp_connect_args[os.path.basename(file_path)] = None
      tmp_job_attrs[os.path.basename(file_path)] = None
      return None
   
   except socket.error, inst:
      # if the connection was simply refused, then just fail
      if inst.args[0] == 111:
         # receive the file with any protocol
         iftlog.log(5, "iftcache.cache_get_file: could not query cache on port " + str(http_port) + ", 111 connection refused" )
         return None
         
      else:
         iftlog.exception( "iftcache.cache_get_file: socket error", inst)
         return None     # some other error
         
   
   except Exception, inst:
      iftlog.exception( "iftcache.cache_get_file: could not query cache on port " + str(http_port), inst)
      tmp_connect_args[os.path.basename(file_path)] = None
      tmp_job_attrs[os.path.basename(file_path)] = None
      return None
   


def cache_path( filename ):
   """
   Given a filename, find out where it would go in the cache.
   """
   
   return cache_dir + "/" + os.path.basename(filename)

