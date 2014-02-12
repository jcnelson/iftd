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



This is the client script
"""

"""arizonaconfig
   options=[
            ["",     "--transfermethod",  "transfermethod", "append", "string", ["http", "ftp"], "program", "use this method to transfer files (default http, ftp)"],
            ["",     "--metatransfermethod", "metatransfermethod", "append", "string", ["http", "ftp"], "program", "use this method to transfer metadata (default http, ftp)"],
            ["",     "--transfertempdir", "transfertempdir", "store", "string", "/tmp/iftd-experiments/stork_trans_tmp", "PATH", "use this path to save transferd files temporary (default is /tmp/stork_trans_tmp)"],
            ["",     "--metafilecachetime", "metafilecachetime", "store", "int", 60, None, "seconds to cache metafile without re-requesting"],
            ["",     "--disablesignedmetafile", "disablesignedmetafile", "store_true", None, False, None, "disable signed metafile"],
            ["",     "--disabletransferhashcheck", "disabletransferhashcheck", "store_true", None, False, None, "disable transfer hash checking"],
            ["-C",   "--configfile", "configfile", "store",       "string", "/home/jnelson/iftd/experiments/stork_2.conf", "FILE",   "use a different config file (/usr/local/stork/etc/stork.conf is the default)"],
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

import protocols
import protocols.http
   
def test_urllib2( file, remote_host, tmpfs_dir, chunklen=5000000 ):
   ts = 0
   te = 0
   file_url = "http://" + remote_host + ":8001" + file
   
   for i in xrange(0, 1):
      
      ts = time.time()

      for j in [5, 20]:
         try:
            t1 = time.time()
            print "[", str(t1), "] urllib2 start, get", j, "chunks"
            f = urllib2.urlopen( file_url )
            fd = open(file, "wb")
            ti = time.time()
            cnt = 0
            ccnt = 0
            while ccnt < j:
               tmp = f.read( chunklen )
               if len(tmp) == 0:
                  break

               ccnt += 1
               fd.write( tmp )

               print "[", time.time(), "] received ", ccnt
            
            fd.close()
            f.close()
            t2 = time.time()
            print "[", str(t2), "] urllib2 end, took", str(t2 - t1), "for", j, "chunks"
            os.remove( file )
         except:
            pass
      
         file_url = "http://" + remote_host + ":8000" + file
      
      
      te = time.time()
      iftlog.log(5, "urllib2 took " + str(te - ts) )
   

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
  
   for i in xrange(0, 1):
      
      #arizonaconfig.init_options("experiment2_client.py", configfile_optvar="configfile")
      
      ts = time.time()
      rc, downloaded_files = arizonatransfer.getfiles1(remote_host, [file_data], tmpfs_dir, None, True, prioritylist = ["http2", "http"])
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
        iftfile.JOB_ATTR_SRC_HOST:remote_host,
        protocols.http.USE_PARTIAL_GETS:True
   }
   http2_connect_attrs = {
        iftproto.PROTO_PORTNUM:8001,
        iftfile.JOB_ATTR_SRC_NAME:file,
        iftfile.JOB_ATTR_DEST_NAME:file,
        iftfile.JOB_ATTR_SRC_HOST:remote_host,
        protocols.http.USE_PARTIAL_GETS:True
   }

   job_attrs = {
        iftfile.JOB_ATTR_SRC_HOST:remote_host,
        iftfile.JOB_ATTR_SRC_NAME:file,
        iftfile.JOB_ATTR_FILE_SIZE:int(filesize),
        iftfile.JOB_ATTR_DEST_NAME:file,
        iftfile.JOB_ATTR_CHUNKSIZE:5000000,
        iftfile.JOB_ATTR_DEST_HOST:"localhost"
   }
   
   client = iftapi.make_XMLRPC_client()
   
   connects = {
      "http_receiver":http_connect_attrs,
      "http_sender":http_connect_attrs,
      "http2_receiver":http2_connect_attrs,
      "http2_sender":http2_connect_attrs
   }
   
   for i in xrange(0, 1):
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

   arizonaconfig.init_options("experiment2_client.py", configfile_optvar="configfile")
  
   print "files: " + str(files)
   print "sizes: " + str(sizes)
   
   # empirically-calculated times for the 100MB file from experiment 1
   #urllib2_time = 1.646
   #arizonatransfer_time = 4.0068
   #iftd_time = 3.9965
   
   
   for i in xrange(len(files)-2, len(files)-1):
      file = files[i]
      size = sizes[i]
      fhash = hashes[i]
      
      # spawn a thread for urllib2 transfer, so we can block at the right time
      #test_urllib2( file, remote_host, tmpfs_dir )
      #os.popen("rm -rf " + file).close()
      #test_arizonatransfer( file, fhash, size, remote_host, tmpfs_dir )
      #os.popen("rm -rf " + file).close()
      #os.popen("rm -rf " + tmpfs_dir + "*")
      test_iftd( file, fhash, size, remote_host, tmpfs_dir )
      os.popen("rm -rf " + file).close()
      print ""
      
   
   return 0


if __name__ == "__main__":
   Main()
