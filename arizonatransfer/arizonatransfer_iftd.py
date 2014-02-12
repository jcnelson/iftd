#! /usr/bin/env python
"""
Stork Project (http://www.cs.arizona.edu/stork/)
Module: arizonatransfer_http
Description:   Provides a general file transferring by iftd.  Designed to be run by the nest, not the client!

"""

import urllib
import urllib2
import os
import time
import socket
import arizonareport
import arizonageneral
import shutil
import cPickle
import sys
sys.path.append("/usr/lib/python2.5/site-packages/iftd")

import iftfile
import protocols
import iftutil
import iftlog
import iftloader
import iftapi

import protocols
import protocols.raven
import protocols.iftcache

import xmlrpclib

# XMLRPC server connection to iftd
iftd_server = None

def log_transfer(function, pid, timestamp, timestampend):
   try:
      iftlog.log(3, "Retrieved file")
      import storklog
      storklog.log_transfer(function, pid, timestamp, timestampend)
   except:
      pass

def close_transfer_program():
   """
   <Purpose>
      This closes a connection (shuts down iftd)

   <Arguments>
      None.
   
   <Exceptions>
      None.

   <Side Effects>
      None.

   <Returns>
      True.
   """
   return True



def init_transfer_program(port, ignore2=None, ignore3=None, ignore4=None):
   """
   <Purpose>
      This initializes a connection.  It starts up iftd's filewriter thread

   <Arguments>
      None.
   
   <Exceptions>
      None.

   <Side Effects>
      None.

   <Returns>
      True.
   """
   global iftd_server
   if port == None:
      port = 4000
   iftd_server = iftapi.make_XMLRPC_client( "127.0.0.1", port, "RPC2", 60 )
   return True




def retrieve_files(host, filelist, destdir='.', indicator=None, nestmode=False):
   """
   <Purpose>
      This retrieves files from a host to a destdir.

   <Arguments>
      host:
         'host' holds two things, a server name and target directory.
         For example, if you want to retrieve files from '/tmp/' directory
         in 'quadrus.cs.arizona.edu' server, the 'host' will be
         'quadrus.cs.arizona.edu/tmp'.

      filelist:
         'filelist' is a list of files which need to be retrieved.

      destdir:
         'destdir' is a destination directory where retrieved files will
         be placed. A user should have 'destdir' exist before retrieving 
         files. 'destdir' should be a string. Default is a current dir.

      indicator:
         'indicator' is a module which has set_filename and 
         download_indicator functions. 'indicator' will be passed in 
         'urlretrieve' function so that progress bar will be shown 
         while downloading files. Default is 'None'.

      nestmode:
         False ==> This is a client, so don't use iftcache_receiver; connect to nest slice on localhost
         True  ==> This is a nest, so use iftcache_receiver; (try to) connect to repository IFTD (if there is one)

   <Exceptions>
      None.

   <Side Effects>
      None.

   <Returns>
      (True, grabbed_list)
      'grabbed_list' is a list of files which are retrieved
   """
   global iftd_server
   
   arizonareport.send_out(4, "[DEBUG] arizonatransfer_iftd.retrieve_files: started")

   # set grabbed_list as a empty list. Later it will be appended with retrieved files
   grabbed_list = []

   # check if host is a string
   if not isinstance(host, str):
      arizonareport.send_syslog(arizonareport.ERR, "retrieve_files(): host should be a string")
      # return false and empty list
      return (False, grabbed_list)

   # check if destdir is a string
   if not isinstance(destdir,str):
      arizonareport.send_syslog(arizonareport.ERR, "retrieve_files(): destdir should be a string")
      # return false and empty list
      return (False, grabbed_list)

   # check that the destination directory exists
   if not os.path.isdir(destdir):
      arizonareport.send_syslog(arizonareport.ERR, "\nretrieve_files(): The destination directory '" + destdir + "' for a requested does not exist")
      # return false and empty list
      return (False, grabbed_list)

   # if destdir is a empty string, then make it as a current directory
   if destdir == '':
      destdir = '.'

   # populate connection arguments that don't change
   job_attrs = {}
   job_attrs[ iftfile.JOB_ATTR_CHUNKSIZE ] = 65536
      
   # cache...
   cache_connect_args = {}
   
   iftd_rpc_dir = "/RPC2"
   iftd_remote_port = 4005
   iftd_user_timeout = 3600

   proto_list = iftd_server.list_protocols()
   
   if nestmode:
      # we're a client, so the first attempt should be to get the file via the cache.
      # if the cache protocol is unavailable, then use anything we can.
      if "iftcache_receiver" in proto_list:
         proto_list = ["iftcache_receiver"]
      
         cache_connect_args[ protocols.iftcache.IFTCACHE_MAX_AGE ] = 10 * 24 * 3600 * 365    # ~10 years
         cache_connect_args[ protocols.iftcache.IFTCACHE_SQUID_PORTNUM ] = 31128  # specific to our installation
         cache_connect_args[ protocols.iftcache.IFTCACHE_USER_TIMEOUT ] = iftd_user_timeout
         cache_connect_args[ protocols.iftcache.IFTCACHE_REMOTE_IFTD_PORT ] = iftd_remote_port
         cache_connect_args[ protocols.iftcache.IFTCACHE_REMOTE_RPC_DIR ] = iftd_rpc_dir
   
   else:
      # we're the nest, so do not use the cache
      if "iftcache_receiver" in proto_list:
         proto_list.remove("iftcache_receiver")
   
   # remove all non-senders
   protolist = []
   for proto in proto_list:
      if proto.find("receiver") >= 0:
         protolist.append(proto)

   iftlog.log(1, "retrieve_files(): will attempt protocols " + str(protolist))
   
   job_attrs[ iftfile.JOB_ATTR_PROTOS ] = protolist
   
   for file in filelist:
      filename = file['filename']
      starttime = time.time()

      # fill in src and dest parameters
      hostname, srcpath = __extract_host_path( host, filename )

      job_attrs[ iftfile.JOB_ATTR_SRC_HOST ] = hostname
      job_attrs[ iftfile.JOB_ATTR_SRC_NAME ] = srcpath
      job_attrs[ iftfile.JOB_ATTR_DEST_NAME ] = "/tmp/iftd-recv/" + filename # os.path.join( destdir, filename )
      job_attrs[ iftfile.JOB_ATTR_DEST_HOST ] = socket.gethostname()
      job_attrs[ iftfile.JOB_ATTR_FILE_SIZE ] = file.get("size")

      # raven...
      job_attrs[ protocols.raven.HASH_FUNCS ] = file.get("hashfuncs")
      
      # fill in a hash, if we have it
      hash = file.get("hash", None)
      if hash:
         job_attrs[ iftfile.JOB_ATTR_FILE_HASH ] = hash
      
      # populate protocol connection arguments dict
      proto_args_dict = {}

      if nestmode:
         # nest needs squid details; client does not
         proto_args_dict[ "iftcache_receiver" ] = cache_connect_args

      rc = -1
      
      try:
         # have iftd attempt to receive it
         if not nestmode:
            rc = iftd_server.begin_ift( job_attrs, proto_args_dict, False, True, iftd_remote_port, iftd_rpc_dir, iftd_user_timeout )
         else:
            proto_args_dict[ iftapi.CONNECT_ATTR_REMOTE_PORT ] = iftd_remote_port
            proto_args_dict[ iftapi.CONNECT_ATTR_REMOTE_RPC ] = iftd_rpc_dir
            proto_args_dict[ iftapi.CONNECT_ATTR_USER_TIMEOUT ] = iftd_user_timeout
            packed_job_attrs = __pickle_raw( job_attrs )
            packed_connect_args = __pickle_raw( proto_args_dict )
            
            sep = "/"
            if job_attrs[ iftfile.JOB_ATTR_SRC_NAME ][0] == "/":
               sep = ""

            req = urllib2.Request("http://127.0.0.1:6650" + sep + job_attrs[ iftfile.JOB_ATTR_SRC_NAME ] + "?job_attrs=" + packed_job_attrs + "&connect_args=" + packed_connect_args)

            try:
               resp = urllib2.urlopen( req )
               if resp.code != 200:
                  rc = -8
         
               else:
                  # write file out
                  fd = open("/tmp/iftd-recv/" + filename, "wb")
                  fd.write( resp.read() )
                  fd.close()
                  rc = 0
               
            except Exception, inst:
               rc = -16


      except Exception, inst:
         iftlog.exception("retrieve_files(): iftd failure", inst)
         arizonareport.send_syslog( arizonareport.ERR, 'retrieve_files(): XMLRPC error, could not receive file '  + filename )
         continue
      
      if rc != 0:
         # failed to transmit
         # try a different protocol
         iftlog.log(5, "retrieve_files(): rc = " + str(rc))
         arizonareport.send_syslog( arizonareport.ERR, 'retrieve_files(): could not receive file ' + filename + ", rc = " + str(rc) )
         continue
      else:
         # file was successfully received
         shutil.move( "/tmp/iftd-recv/" + filename, destdir )
         grabbed_list = grabbed_list + [file]
         endtime = time.time()
         log_transfer("iftd", str(os.getpid()), str(starttime), str(endtime))
         
   

   if (grabbed_list) :
      return (True, grabbed_list)
   # if nothing in grabbed_list
   else:
      return (False, grabbed_list)



def __pickle_raw( obj ):
   obj_str = cPickle.dumps( obj )
   tmp = "%r" % obj_str
   tmp = tmp[1:-1]
   return tmp


def transfer_name():
   """
   <Purpose>
      This gives the name of this transfer method.

   <Arguments>
      None.

   <Exceptions>
      None.

   <Side Effects>
      None.

   <Returns>
      'arizona_iftd' as an string
   """

   return 'arizona_iftd'




def __extract_host_path( host, filename ):
   """
   <Purpose>
      Extracts the path components from the host and prepends them to the filename.

   <Arguments>
       host:
         'host' holds two things, a server name and target directory.
         For example, if you want to retrieve files from '/tmp/' directory
         in 'quadrus.cs.arizona.edu' server, the 'host' will be
         'quadrus.cs.arizona.edu/tmp'.
      filename:
         'filename' holds the base name of the file

   <Exceptions>
      None.

   <Side Effects>
      None.

   <Returns>
      The hostname and the path + filename strings
   """
   
   # remove the protocol if there is one
   host = arizonageneral.lcut(host, "http://")
   host = arizonageneral.lcut(host, "https://")
   host = arizonageneral.lcut(host, "ftp://")
   
   index=host.find("/")

   hostname = ""
   pathname = ""
   
   # set hostname to hold only a server name
   if index != -1:
      hostname = host[:index]
      pathname = host[index:]
   else :
      hostname = host

   return (hostname, os.path.join( pathname, filename ))


def __extract_hostname(host):
   """
   <Purpose>
      Extracts the hostname from a host string

   <Arguments>
       host:
         'host' holds two things, a server name and target directory.
         For example, if you want to retrieve files from '/tmp/' directory
         in 'quadrus.cs.arizona.edu' server, the 'host' will be
         'quadrus.cs.arizona.edu/tmp'.

   <Exceptions>
      None.

   <Side Effects>
      None.

   <Returns>
      The hostname
   """

   # remove the protocol if there is one
   host = arizonageneral.lcut(host, "http://")
   host = arizonageneral.lcut(host, "https://")
   host = arizonageneral.lcut(host, "ftp://")

   index=host.find("/")

   # set hostname to hold only a server name
   if index != -1:
      hostname = host[:index]
   else :
      hostname = host

   return hostname





def __build_url(host, fname, protocol="http"):
   """
   <Purpose>
      This builds a url string with Http address.

   <Arguments>
       host:
         'host' holds two things, a server name and target directory.
         For example, if you want to retrieve files from '/tmp/' directory
         in 'quadrus.cs.arizona.edu' server, the 'host' will be
         'quadrus.cs.arizona.edu/tmp'.
      fname:
         A file name to be retrieved

   <Exceptions>
      None.

   <Side Effects>
      None.

   <Returns>
      A whole url string created
   """

   host = _strip_protocol(host)

   if (protocol != "http") and (protocol != "https"):
      return TypeError, "unknown protocol"

   # add '/' at the end of the host if there is not, so that file name is added properly
   if not host.endswith("/"):
      host = host + '/'

   # return url which contains host and filename
   return protocol + "://" + host + fname


def _strip_protocol(s):
   s = arizonageneral.lcut(s, "http://")
   s = arizonageneral.lcut(s, "https://")
   s = arizonageneral.lcut(s, "ftp://")
   return s
