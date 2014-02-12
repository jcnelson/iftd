#!/usr/bin/python

"""
=============
Experiment 1:  3-way comparison for transferring a file from one host to another on a LAN.
=============
Purpose:
    To calculate the overhead required by urllib2, arizonatransfer, and IFTD to receive files (via HTTP) of variable sizes from a remote host,
    assuming unchanging network performance.

Expected result:
    urllib2 will be the fastest, and thus have the least overhead in all cases, followed by arizonatransfer, followed by IFTD.

Setup:
    There are two hosts:  the server and the client.  The server has files of sizes 1KB, 10KB, 100KB, 1MB, 10MB, and 100MB, 
    all filled with random noise, and all allocated in a tmpfs mount.  Also, the server runs the latest stable version of the
    simple Python HTTP server, and listens on port 80 for HTTP requests.  The server does not do anything extraordinary, such as 
    bandwidth-capping; it is effectively a "dumb" HTTP server that will communicate with at most one client and will send data 
    to the client as fast as possible and can serve any of these files.
    
    The client will have a running IFTD instance, as well as a script that retrieves each file via urllib2, arizonatransfer, and 
    IFTD 5 times.  The script will use the Python time module to record how long each transmission takes (specifically, the time 
    taken by the method call that does the transfer) and will output the timings upon termination.  The script will pre-allocate 
    the space needed by the data to minimize the unrelated Python VM overhead.  The client will also have a tmpfs mount, and the 
    script will receive the files to that tmpfs mount to avoid I/O delays.  Also, the script itself will reside on a tmpfs mount 
    to avoid unnecessary I/O.
    
    Since arizonatransfer and iftd both support checking the hashes of the files they transfer, and since in practice both make
    use of this frequently, checking the hash will be considered to be part of the overhead.
    

Procedure:
1.  Populate the server with files of size 1KB, 10KB, 100KB, 1MB, 10MB, 100MB on a tmpfs mount.
2.  Setup and start the "dumb" HTTP server, listening on port 80
3.  Download the file from the server to the client 10 times using each method and record the transmission time for each.  This is to be done by a script.
4.  Record the data printed by the client program



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

import BaseHTTPServer
import SimpleHTTPServer

import cherrypy

class Root: pass


def run_server( tmpfs_dir ):

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
   cherrypy.server.socket_port = 8000

   cherrypy.quickstart( Root(), '/', config=conf) 


def make_files( tmpfs_dir, file_prefix, pattern="0123456789" ):
   # make files of 1KB, 10KB, 100KB, 1MB, 10MB, and 100MB in the given directory
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
      print "serving..."     
      run_server( tmpfs_dir )
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
   
   print "usage: experiment1_server.py <tmpfs_dir> [start|stop]"
   return 1

if __name__ == "__main__":
   Main()
