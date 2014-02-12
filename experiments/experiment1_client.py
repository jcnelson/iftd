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
    Apache web server, and listens on port 80 for HTTP requests.  The server does not do anything extraordinary, such as 
    bandwidth-capping; it is effectively a "dumb" HTTP server that will communicate with at most one client and will send data 
    to the client as fast as possible and can serve any of these files.
    
    The client will have a running IFTD instance, as well as a script that retrieves each file via urllib2, arizonatransfer, and 
    IFTD 5 times.  The script will use the Python time module to record how long each transmission takes (specifically, the time 
    taken by the method call that does the transfer) and will output the timings upon termination.  The script will pre-allocate 
    the space needed by the data to minimize the unrelated Python VM overhead.  The client will also have a tmpfs mount, and the 
    script will receive the files to that tmpfs mount to avoid I/O delays.  Also, the script itself will reside on a tmpfs mount 
    to avoid unnecessary I/O.
    
    ??? Since arizonatransfer and iftd both support checking the hashes of the files they transfer, and since in practice both make
    use of this frequently, checking the hash will be considered to be part of the overhead.
    
    IFTD will have a chunksize of 100KB.

Procedure:
1.  Populate the server with files of size 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, and 1000MB on a tmpfs mount.
2.  Setup and start Apache as a "dumb" HTTP server, listening on port 80
3.  Download the file from the server to the client 5 times using each method and record the transmission time for each.  This is to be done by a script.
4.  Record the data printed by the client program



This is the client script
"""

"""arizonaconfig
   options=[
            ["",     "--transfermethod",  "transfermethod", "append", "string", ["http", "ftp"], "program", "use this method to transfer files (default http, ftp)"],
            ["",     "--metatransfermethod", "metatransfermethod", "append", "string", ["http", "ftp"], "program", "use this method to transfer metadata (default http, ftp)"],
            ["",     "--transfertempdir", "transfertempdir", "store", "string", "/tmp/iftd-experiments/stork_trans_tmp", "PATH", "use this path to save transferd files temporary (default is /tmp/stork_trans_tmp)"],
            ["",     "--metafilecachetime", "metafilecachetime", "store", "int", 60, None, "seconds to cache metafile without re-requesting"],
            ["",     "--disablesignedmetafile", "disablesignedmetafile", "store_true", None, False, None, "disable signed metafile"],
            ["",     "--disabletransferhashcheck", "disabletransferhashcheck", "store_true", None, True, None, "disable transfer hash checking"],
            ["-C",   "--configfile", "configfile", "store",       "string", "/home/jnelson/iftd/experiments/stork_1.conf", "FILE",   "use a different config file (/usr/local/stork/etc/stork.conf is the default)"],
            ["",     "--mask",       "mask",       "store",       "string", None, "MASK", "specify mask of files to download"]]
   includes=[]
"""

import os
import sys

#sys.path.append("/usr/local/lib/python2.5/site-packages/iftd")
sys.path.append("/home/jnelson/iftd")
sys.path.append("/home/jnelson/stork")
sys.path.append("/home/jnelson/stork/arizonalib")

import urllib2

import iftlog
import iftfile
import time

import arizonatransfer
import arizonaconfig

import iftapi
import iftproto
import hashlib

import protocols.http


def test_urllib2( file, remote_host, tmpfs_dir ):
   ts = 0
   te = 0
   file_url = "http://" + remote_host + ":8000" + file
   
   for i in xrange(0, 10):
      # transfer the file with urllib2 5 times
      
      ts = time.time()
#      m = hashlib.sha1()
      f = urllib2.urlopen( file_url )
      fd = open(file, "wb")
      buff = f.read()
#      m.update( buff )
#      m.hexdigest()
      fd.write( buff )
      fd.close()
      f.close()
      te = time.time()
      
      iftlog.log(5, "urllib2 " + file_url + " " + str(te - ts) )
      
   

def test_arizonatransfer( file, filehash, filesize, remote_host, tmpfs_dir ):
   ts = 0
   te = 0
   file_url = remote_host + file
   
   file_data = {
      "filename": file[1:],
      "hash": filehash,
      "size": int(filesize),
      "hashfuncs": [arizonatransfer.default_hashfunc]
   }
  
   for i in xrange(0, 10):
      # transfer the file with arizonatransfer 5 times, excluding getting the metafile
      
      
      ts = time.time()
      rc, downloaded_files = arizonatransfer.getfiles1(remote_host, [file_data], tmpfs_dir, None, True, prioritylist = ["http"])
      te = time.time()
      if not rc:
         print "arizonatransfer failed!"
         print "downloaded files: " + str(downloaded_files)

      
      iftlog.log(5, "arizonatransfer " + file_url + " " + str(te - ts) )
      
   

def test_iftd( file, filehash, filesize, remote_host, tmpfs_dir ):
   ts = 0
   te = 0
   http_connect_attrs = {
        iftproto.PROTO_PORTNUM:8000,
        iftfile.JOB_ATTR_SRC_NAME:file,
        iftfile.JOB_ATTR_DEST_NAME:file,
        iftfile.JOB_ATTR_SRC_HOST:remote_host
   }

   job_attrs = {
        iftfile.JOB_ATTR_SRC_HOST:remote_host,
        iftfile.JOB_ATTR_SRC_NAME:file,
        iftfile.JOB_ATTR_FILE_SIZE:int(filesize),
        iftfile.JOB_ATTR_CHUNKSIZE:int(filesize), # get the whole file at once
#        iftfile.JOB_ATTR_FILE_HASH:filehash,
        iftfile.JOB_ATTR_DEST_NAME:file,
        iftfile.JOB_ATTR_DEST_HOST:"localhost",
        protocols.http.HTTP_SERVER_VERSION:11
   }
   
   client = iftapi.make_XMLRPC_client()
   
   connects = {
      "http_receiver":http_connect_attrs,
      "http_sender":http_connect_attrs
   }
   
   for i in xrange(0, 10):
      # transfer the file with iftd 5 times
      ts = time.time()
      client.begin_ift( job_attrs, connects, False, True, 4001, "/RPC2", True, False, 60)
      te = time.time()
      
      iftlog.log(5, "iftd " + remote_host + file + " " + str(te - ts) )
      
   

def Main():
   # usage: experiment1_client.py <remote_host> <remote_dir> <tmpfs_dir>
   
   remote_host = sys.argv[1]
   remote_dir = os.path.join( "/", sys.argv[2] )
   tmpfs_dir = os.path.join( "/", sys.argv[3] )
   if tmpfs_dir[-1] != "/":
      tmpfs_dir += "/"
   
   # get the remote file listing
   f = urllib2.urlopen( "http://" + remote_host + ":8000" + os.path.join( remote_dir, "files.txt" ) )
   files = []
   dat = f.read()
   f.close()
   for f in dat.split("\n"):
      files.append( os.path.join( remote_dir, f ) )

   
   # get the remote file hashes
   f = urllib2.urlopen( "http://" + remote_host + ":8000" + os.path.join( remote_dir, "hashes.txt" ) )
   hashes = f.read().split("\n")
   f.close()
   
   # get the remote file sizes
   f = urllib2.urlopen( "http://" + remote_host + ":8000" + os.path.join( remote_dir, "sizes.txt" ) )
   sizes = f.read().split("\n")
   f.close()

   arizonaconfig.init_options("experiment1_client.py", configfile_optvar="configfile")
  
   print "files: " + str(files)
   print "sizes: " + str(sizes)
 
   for i in xrange(0, len(files)-1):
      file = files[i]
      size = sizes[i]
      fhash = hashes[i]
      
      print "Testing file " + str(file)
      test_urllib2( file, remote_host, tmpfs_dir )
      os.popen("rm -rf " + file).close()
      test_arizonatransfer( file, fhash, size, remote_host, tmpfs_dir )
      os.popen("rm -rf " + file).close()
      os.popen("rm -rf " + tmpfs_dir + "*")
      test_iftd( file, fhash, size, remote_host, tmpfs_dir )
      os.popen("rm -rf " + file).close()
      print ""
   
   return 0


if __name__ == "__main__":
   Main()
