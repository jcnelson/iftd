#!/usr/bin/env python

"""
iftutil.py
Copyright (c) 2009 Jude Nelson

This package provides utility (e.g. not explicitly server-related) functions for iftd
"""

import os
import sys
import imp
import iftlog
import thread
import iftloader
import iftapi
import iftfile
import protocols
from collections import deque
import threading

import StringIO
import cPickle

import traceback

from iftdata import *

import SimpleXMLRPCServer
from BaseHTTPServer import HTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
import SocketServer

import xmlrpclib
from xmlrpclib import Fault

try:
    import fcntl
except ImportError:
    fcntl = None


# courtesy of Nadia Alramli
# http://nadiana.com/python-pickle-insecure
class SafeUnpickler(object):
    PICKLE_SAFE = {
        'copy_reg': set(['_reconstructor']),
        '__builtin__': set(['object'])
    }
 
    @classmethod
    def find_class(cls, module, name):
        if not module in cls.PICKLE_SAFE:
            raise cPickle.UnpicklingError(
                'Attempting to unpickle unsafe module %s' % module
            )
        __import__(module)
        mod = sys.modules[module]
        if not name in cls.PICKLE_SAFE[module]:
            raise cPickle.UnpicklingError(
                'Attempting to unpickle unsafe class %s' % name
            )
        klass = getattr(mod, name)
        return klass
 
    @classmethod
    def loads(cls, pickle_string):
        pickle_obj = cPickle.Unpickler(StringIO.StringIO(pickle_string))
        pickle_obj.find_global = cls.find_class
        return pickle_obj.load()



class iftthreading:
   """
   Implementation of a thread control mechanism in which IFTD can
   limit the total number of threads that are running.  
   
   This implementation prevents a malicious user from placing 
   arbitrarily many transfer requests, crashing IFTD or the system or both.
   """

   __threads = None
   __alive = None
   __num_threads = 0
   __threads_lock = threading.BoundedSemaphore(1)
   
   def __purge_dead(self):
      """
      Remove all non-running threads from the threadpool
      """
      
      self.__alive.clear()
      for t in self.__threads:
         if t.isAlive():
            self.__alive.append( t )
         
      
      self.__threads = copy.copy(self.__alive)
            
      
   def __init__(self, num_threads):
      self.__threads = deque( [] )
      self.__alive = deque( [] )
      self.__num_threads = num_threads
   
   
   def start_new_thread(self, func, arguments, block=False):
      """
      Start a new thread to call the given function with
      the given arguments.
      """
      try:
         good = True
         self.__threads_lock.acquire()
         self.__purge_dead()
         if len(self.__threads) >= self.__num_threads:
            good = False
         self.__threads_lock.release()
         
         if not good:
            return good
            
         t = threading.Thread( target=func, args=arguments )
         self.__threads.append(t)
         t.start()
         
         return True
      except:
         print "fatal exception"
         traceback.print_exc()
         return False      # no space left on queue
      
      

SenderThreadPool = None
ReceiverThreadPool = None

MAX_SENDER_THREADS = 10
MAX_RECEIVER_THREADS = 10

def init_threadpools( num_sending_threads, num_receiving_threads ):
   """
   Initialize the global thread pools for senders and receivers
   """
   global SenderThreadPool
   global ReceiverThreadPool
   
   SenderThreadPool = iftthreading( num_sending_threads )
   ReceiverThreadPool = iftthreading( num_receiving_threads )



class local_request_handler( SimpleXMLRPCServer.SimpleXMLRPCRequestHandler ):
   """
   Implementation of a SimpleXMLRPCRequestHandler
   that restricts RPC calls to a particular path.
   """

   rpc_paths = ('/' + RPC_DIR,)
   
   def do_POST(self):
      # ignore all requests except for localhost
      host, port = self.client_address
      if host != "127.0.0.1" and host != "localhost":
         return
      
      # Check that the path is legal
      if not self.is_rpc_path_valid():
         self.report_404()
         return

      try:
         # Get arguments by reading body of request.
         # We read this in chunks to avoid straining
         # socket.read(); around the 10 or 15Mb mark, some platforms
         # begin to have problems (bug #792570).
         max_chunk_size = 10*1024*1024
         size_remaining = int(self.headers["content-length"])
         L = []
         while size_remaining:
             chunk_size = min(size_remaining, max_chunk_size)
             L.append(self.rfile.read(chunk_size))
             size_remaining -= len(L[-1])
         data = ''.join(L)

         # In previous versions of SimpleXMLRPCServer, _dispatch
         # could be overridden in this class, instead of in
         # SimpleXMLRPCDispatcher. To maintain backwards compatibility,
         # check to see if a subclass implements _dispatch and dispatch
         # using that method if present.
         response = self.server._marshaled_dispatch(
                 data, getattr(self, '_dispatch', None)
             )
      except Exception, inst: # This should only happen if the module is buggy
         # internal error, report as HTTP server error
         iftlog.exception( "local_request_handler: exception encountered", inst)
         self.send_response(500)
         self.end_headers()
      else:
         # got a valid XML RPC response
         self.send_response(200)
         self.send_header("Content-type", "text/xml")
         self.send_header("Content-length", str(len(response)))
         self.end_headers()
         self.wfile.write(response)

         # shut down the connection
         self.wfile.flush()
         self.connection.shutdown(1)

"""
Get our supported protocols.

@return
   A list of protocol module names
"""
def get_available_protocols():
   return protocols.__all__


      
class iftdXMLRPCDispatcher(SimpleXMLRPCServer.SimpleXMLRPCDispatcher):
    """Mix-in class that dispatches XML-RPC requests.

    This class is used to register XML-RPC method handlers
    and then to dispatch them. There should never be any
    reason to instantiate this class directly.
    
    The only modification here over SimpleXMLRPCDispatcher is that
    this dispatcher handles exceptions by also reporting them on the
    local host, with IFTD's logging utility.
    """
    
    # overwritten
    def _marshaled_dispatch(self, data, dispatch_method = None):
        """Dispatches an XML-RPC method from marshalled (XML) data.
        """

        try:
            params, method = xmlrpclib.loads(data)

            # generate response
            if dispatch_method is not None:
                response = dispatch_method(method, params)
            else:
                response = self._dispatch(method, params)
            # wrap response in a singleton tuple
            response = (response,)
            response = xmlrpclib.dumps(response, methodresponse=1,
                                       allow_none=self.allow_none, encoding=self.encoding)
        except Fault, fault:
            response = xmlrpclib.dumps(fault, allow_none=self.allow_none,
                                       encoding=self.encoding)

            iftlog.log(5, "XMLRPC Dispatcher Fault:")
            iftlog.log(5, response)
        except Exception, inst:
            # report exception back to server
            response = xmlrpclib.dumps(
                xmlrpclib.Fault(1, "%s:%s" % (sys.exc_type, sys.exc_value)),
                encoding=self.encoding, allow_none=self.allow_none,
                )
            
            iftlog.exception("IFTD XMLRPC Dispatcher exception", inst)

        return response


class iftdXMLRPCServer(SocketServer.TCPServer,
                       iftdXMLRPCDispatcher):
    """IFTD XML-RPC server.
    """

    allow_reuse_address = True

    def __init__(self, addr, requestHandler=SimpleXMLRPCServer.SimpleXMLRPCRequestHandler,
                 logRequests=True, allow_none=False, encoding=None):
        self.logRequests = logRequests

        SimpleXMLRPCServer.SimpleXMLRPCDispatcher.__init__(self, allow_none, encoding)
        SocketServer.TCPServer.__init__(self, addr, requestHandler)

        # [Bug #1222790] If possible, set close-on-exec flag; if a
        # method spawns a subprocess, the subprocess shouldn't have
        # the listening socket open.
        if fcntl is not None and hasattr(fcntl, 'FD_CLOEXEC'):
            flags = fcntl.fcntl(self.fileno(), fcntl.F_GETFD)
            flags |= fcntl.FD_CLOEXEC
            fcntl.fcntl(self.fileno(), fcntl.F_SETFD, flags)



# multithreaded xmlrpc server using threading mixin
class MultithreadedXMLRPCServer( SocketServer.ThreadingMixIn, iftdXMLRPCServer ): pass

def create_server( portnum, funcs, request_handler = local_request_handler ):
   """
   Set up a local XMLRPC server
   @arg port
      Port on which to listen for applications
   @arg funcs
      List of functions that we respond to
   @return
      MultithreadedXMLRPCServer instance on success, None on failure
   """
   server = MultithreadedXMLRPCServer( ("", portnum), requestHandler = request_handler, allow_none=True )
   server.register_introspection_functions()
   
   for func in funcs:
      server.register_function( func )
   
   return server




"""
# HTTP proxy server
# front-end to the XMLRPC api
# uses Pragma HTTP headers to fill in attributes
class iftd_HTTPServer( HTTPServer ):
   
   def __init__(self, server_addr, handler):
      os.chdir("/")
      self.protocol_version = 'HTTP/1.0'
      HTTPServer.__init__(self, server_addr, handler)
      
   

# Request handler specific to iftd.
class iftd_HTTPServer_handler( SimpleHTTPRequestHandler ):
   
   def do_GET(self):
      # get the file!  This is for receivers.  Senders use do_POST()
      try:
         # format of headers:
         # protocol=<protocol to use>
         # <protocol to use>=<connect key>:<connect attr>
         
         print str(self.headers)
         
         # first, construct the job from the Pragma headers
         packed_attrs = self.headers.getheader("Pragma")
         pragma_headers = []
         if packed_attrs != None:
            pragma_headers = packed_attrs.split("\x01")
         
         # each header is Pragma: key=value
         header_dict = {}
         for h in pragma_headers:
            key, value = h.split("=")
            
            if header_dict.get(key) == None:
               header_dict[key] = [value]
            else:
               header_dict[key].append( value )
            
         
         # get desired protocols
         protocols = header_dict.get("protocol")
         if protocols == None:
            protocols = iftapi.list_protocols()
         connect_dict = {}
         
         # construct the connection attributes super-dictionary
         if protocols != None and len(protocols) != 0:
            # find <name>=<key>:<value>, where <name> is a protocol name, <key> is a connection attribute key, and <value> is the value for the key
            for (proto, value_list) in header_dict.items():
               if proto in protocols:
                  # stuff the value_list elements into a dict
                  connect_dict[proto] = {}
                  
                  for connect_attr in value_list:
                     connect_key, connect_value = connect_attr.split(":")
                     if len(connect_key) == 0 or len(connect_value) == 0:
                        continue    # can't use an empty string
                     
                     connect_dict[proto][connect_key] = connect_value.strip("\r\n")
                  
               
            
         # construct the job next
         job_attrs = {}
         for (key, value_list) in header_dict.items():
            if key in protocols:
               # don't use this
               continue
            
            job_attrs[key] = value_list[-1].strip("\r\n")
         
         
         # make sure we have something for the host
         if job_attrs.get(iftfile.JOB_ATTR_SRC_HOST) == None and self.headers.getheader("Host") != None:
            job_attrs[iftfile.JOB_ATTR_SRC_HOST] = self.headers.getheader("Host").split(":")[0]
         
         job_attrs[iftfile.JOB_ATTR_SRC_NAME] = os.path.normpath(self.path)
         
         dest_path = job_attrs.get(iftfile.JOB_ATTR_DEST_NAME)
         if dest_path == None:
            iftlog.log(5, "Cannot get " + str(job_attrs.get(iftfile.JOB_ATTR_SRC_NAME)) + " from " + str(job_attrs.get(iftfile.JOB_ATTR_SRC_HOST)) + ", destination path not specified")
            self.send_response(404)
            return
         
         dest_path = os.path.normpath( dest_path )
         
         # call out to IFTD to get the file!
         print "protocols:    " + str(protocols)
         print "connect dict: " + str(connect_dict)
         print "job attrs:    " + str(job_attrs)
         
         rc = iftapi.begin_ift( job_attrs, connect_dict, False, True, 4001, "/RPC2", True, 300 )
         iftlog.log(3, "begin_ift: return code " + str(rc) )
         if rc != iftproto.ifttransmit.TRANSMIT_STATE_SUCCESS:
            iftlog.log(5, "HTTPServer: could not fetch " + str(job_attrs.get(iftfile.JOB_ATTR_SRC_NAME)) + " from " + str(job_attrs.get(iftfile.JOB_ATTR_SRC_HOST)) )
            self.send_response(404)
            return
         
         # get the file and send it back
         fd = open( job_attrs.get(iftfile.JOB_ATTR_DEST_NAME), "r" )
         self.send_response(200)
         
         while(True):
            buff = fd.read(iftfile.DEFAULT_FILE_CHUNKSIZE)
            if len(buff) == 0:
               break
            
            self.wfile.write( buff )
         
         fd.close()
         return
         
      except Exception, inst:
         iftlog.exception( "Could not fully transmit " + self.path, inst)
         self.send_response(404)
         return
"""