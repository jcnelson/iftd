#!/usr/bin/python

"""
=============
Experiment 2:  3-way comparison for tolerating an outright protocol failure mid-transfer on a LAN.
=============
Purpose:
    To measure how quickly a (large) file can be transferred to client if the protocol in use encounters an irrecoverable error (i.e. sudden port blockage) roughly half-way into a file transfer.

Expected result:
    urllib2 will time out and fail to fully receive the file from the server.  Arizonatransfer will fall back to its secondary HTTP protocol (which listens on port 81) and re-download the file once its first HTTP protocol fails.  
    IFTD, since it has no training data, will download data concurrently with both protocols, but will depend only on its second HTTP protocol (listening on port 81) once the first one is blocked.  
    IFTD will transfer the file the fastest, followed by arizonatransfer.  urllib2 will not finish.

Setup:
    There are two hosts:  the server and the client.  The server has a single file of size 100MB filled with random noise on a tmpfs mount (to avoid I/O delays).  
    It will be running the same HTTP server from experiment 1, but it will be configured to listen on ports 80 and 81 for HTTP GET messages.  
    It will otherwise be a "dumb" HTTP server in that it will not cap bandwidth and it will connect to at most one client at a time.
    The client will have a running IFTD instance with two HTTP protocols--one that communicates on port 80, and one that communicates on port 81.  
    It will also have an additional arizonatransfer HTTP protocol module that is no different from its default HTTP protocol module, except that it will communicate on port 81 instead of port 80. 
    Like in Experiment 1, the client will have a tmpfs mount big enough to receive the server's file, and the script to carry out the experiment will reside on the tmpfs mount as well.
    Arizonatransfer will be configured to receive via HTTP on port 80, and to fall back to HTTP on port 81.  IFTD will be configured to select protocols only from HTTP on port 80 and HTTP on port 81.
    IFTD's chunksize will be 1000KB.

Procedure:
1.  Calculate the following:
* Let T_u be the average time required to transfer a 100MB file from server to client using urllib2, as determined in Experiment 1.
* Let T_a be the average time required to transfer a 100MB file from server to client using arizontransfer, as determined in Experiment 1.
* Let T_i be the average time required to transfer a 100MB file from server to client using IFTD, as determined by Experiment 1.
* Let T_start be the time recorded just before invoking the transfer method (calculated by Python's time module)
2.  Download the file to the client via urllib2.  At time T_start + 0.5 * T_u, block port 80 via iptables.  Record the time from T_start to the point where urllib2 fails.  Do this 10 times.
3.  Download the file to the client via arizonatransfer.  At time T_start + 0.5 * T_a, block port 80 via iptables.  Record the time from T_start to the point where arizonatransfer transfers the file completely (once it falls back to its second HTTP protocol on port 81).  Do this 10 times.
4.  Download the file to the client via iftd.  At time T_start + 0.5 * T_i, block port 80 via iptables.  Record the time from T_start to the point where iftd transfers the file completely.  Do this 10 times.
5.  Record the data from steps 3-5.



This is the server setup script
"""

import os
import sys

#sys.path.append("/usr/local/lib/python2.5/site-packages/iftd")
sys.path.append("/home/jnelson/iftd")

import urllib2

import iftlog
import iftfile
import iftapi
import time
import thread

import BaseHTTPServer
import SimpleHTTPServer


class Root: pass


def run_server( tmpfs_dir, port ):

   import cherrypy
   cherrypy.root = Root()

   cherrypy.config.update({
        'server.environment': 'development',
        'tools.staticdir.root' : '/'
   })
   conf = {
        '/' : {
            'tools.staticdir.on' : True,
            'tools.staticdir.dir' : '',
         }
   }
   cherrypy.server.socket_host = '0.0.0.0'
   cherrypy.server.socket_port = port

   cherrypy.quickstart( Root(), '/', config=conf)


def make_files( tmpfs_dir, file_prefix, pattern="0123456789" ):
   # make files between 1000 and 100000000 bytes
   sizes = [int(1e3), int(1e4), int(1e5), int(1e6), int(1e7), int(1e8)]
   names = []
   for size in sizes:
      fd = open( os.path.join( tmpfs_dir, file_prefix + "_" + str(size)), "wb" )
      
      for w in xrange(0, size/len(pattern)):
         fd.write(pattern)
      
      fd.close()
      names.append( file_prefix + "_" + str(size) )
   
   return names


def cleanup_files( tmpfs_dir, files ):
   # remove the given files from the tmpfs directory
   for file in files:
      path = os.path.join( tmpfs_dir, file )
      os.popen("rm -rf " + path).close()


def Main():
   # usage: experiment1_server.py <tmpfs_dir> [start|stop]
   
   tmpfs_dir = sys.argv[1]
   opt = sys.argv[2]
   
   if opt == "start":
      
      files = make_files( tmpfs_dir, "file" )
      
      # write the filenames to a place where the client can find them
      fd = open( os.path.join( tmpfs_dir, "files.txt" ), "wb" )
      for f in files:
         fd.write(f + "\n")
      
      # write the file hashes to a place where the client can find them
      fd = open( os.path.join( tmpfs_dir, "hashes.txt" ), "wb" )
      for f in files:
         fd.write( str(iftfile.get_hash(os.path.join( tmpfs_dir, f))) + "\n")
      
      # write the filenames to a place where the client can find them
      fd = open( os.path.join( tmpfs_dir, "sizes.txt" ), "wb" )
      for f in files:
         fd.write( str(os.stat( os.path.join(tmpfs_dir, f) ).st_size) + "\n")
      
      fd.close()
     
   if opt == "start" or opt == "go": 
      print "serving..."     
      os.chdir("/") 
      run_server( tmpfs_dir, 8000 )
      
      return 0
   
   elif opt == "stop":
      fd = open( os.path.join( tmpfs_dir, "files.txt" ) )
      files = fd.readlines()
      fd.close()
      
      cleanup_files( tmpfs_dir, files )
      os.popen("rm -rf " + os.path.join( tmpfs_dir, "files.txt") ).close()
      os.popen("rm -rf " + os.path.join( tmpfs_dir, "hashes.txt") ).close()
      os.popen("rm -rf " + os.path.join( tmpfs_dir, "sizes.txt") ).close()
      
      for f in files:
         os.popen("rm -rf " + os.path.join( tmpfs_dir, f ) ).close()
 
      return 0
   
   print "usage: experiment2_server.py <tmpfs_dir> [start|stop]"
   return 1

if __name__ == "__main__":
   Main()
