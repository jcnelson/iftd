#!/usr/bin/python

"""
=============
Experiment 3:  Protocol favoratism based on file size
=============
Purpose:
    To determine how well IFTD learns to favor one protocol over another when file size influences the choice.

Expected Result:
    IFTD will learn to favor an HTTP protocol that responds immediately to HTTP GETs but caps bandwidth for retrieving
    small files, and will learn to favor an HTTP protocol that responds after a short delay, but does not cap bandwidth for retrieving large files.

Setup:
    Thre are two hosts:  the server and the client.  The server has a set of files of size 1KB, 10KB, 100KB, 1MB, 10MB, and 100MB on a RAM disk, as in Experiment 1.
    It runs the latest stable CherryPy server, which can serve any of these files.  It will listen on port 80 for HTTP requests, but will otherwise be a "dumb" server that does 
    nothing extraordinary such as bandwidth-capping; it serves files as fast as possible to at most one client.
    
    The client will have a running IFTD instance with two HTTP protocols.  The first will retrieve data and give it back to IFTD immediately, but will never allow itself to give 
    back data faster than 100KB/sec.  The second will retrieve data immediately, but will wait for 1 second before giving its first data back to IFTD (after which it will give data 
    to IFTD as fast as it can).  The client will give IFTD a choice between only these two protocols.  IFTD will be configured to retrieve data without the classifier for 45 transfers
    to get some initial training data, and then train its classifier with the data ad us it for 5 transfers where it chooses the protocol.
    
    IFTD's chunk size will vary such that it will receive each file in 20 chunks. It will be configured to, for
    each transfer, print out how long each chunk took in that transfer and which protocol was used.

Procedure:
1.  Retrieve each file 50 times via IFTD, using one iftjob instance (just give to IFTD 50 times).  After the 50th transfer, calculate a feature vector with IFTD's stats API from the invariant 
iftjob and have IFTD print out the posterior probabilities of both protocols given that feature vector (the protocol with the highest probability is given back by the classifier, but we can get the probabilities for each protocol).




This is the client script
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

   

def test_iftd( file, filehash, filesize, remote_host, tmpfs_dir ):
   ts = 0
   te = 0
   http3_connect_attrs = {
        iftproto.PROTO_PORTNUM:8000,
        iftfile.JOB_ATTR_SRC_NAME:file,
        iftfile.JOB_ATTR_DEST_NAME:file,
        iftfile.JOB_ATTR_SRC_HOST:remote_host
   }
   http4_connect_attrs = {
        iftproto.PROTO_PORTNUM:8000,
        iftfile.JOB_ATTR_SRC_NAME:file,
        iftfile.JOB_ATTR_DEST_NAME:file,
        iftfile.JOB_ATTR_SRC_HOST:remote_host
   }

   job_attrs = {
        iftfile.JOB_ATTR_SRC_HOST:remote_host,
        iftfile.JOB_ATTR_SRC_NAME:file,
        iftfile.JOB_ATTR_FILE_SIZE:int(filesize),
        iftfile.JOB_ATTR_FILE_HASH:filehash,
        iftfile.JOB_ATTR_DEST_NAME:file,
        iftfile.JOB_ATTR_CHUNKSIZE:int(filesize) / 20,
        iftfile.JOB_ATTR_DEST_HOST:"localhost"
   }
   
   client = iftapi.make_XMLRPC_client()
   
   connects = {
      "http3_receiver":http3_connect_attrs,
      "http3_sender":http3_connect_attrs,
      "http4_receiver":http4_connect_attrs,
      "http4_sender":http4_connect_attrs
   }
   
   client.clear_classifier()
 
   for i in xrange(0, 10):
      # transfer the file with iftd 
      ts = time.time()
      client.begin_ift( job_attrs, connects, False, True, 4001, "/RPC2", True, False, 60)
      te = time.time()
      
      iftlog.log(5, "iftd " + remote_host + file + " " + str(te - ts) )
   
   time.sleep(15)
   cls_data = client.get_proto_rankings( job_attrs, True )
   iftlog.log(5, "")
   iftlog.log(5, "Protocol rankings")
   for (proto, prob) in cls_data:
      iftlog.log(5, str(proto) + ":  " + str(prob))
   iftlog.log(5, "")
  
   

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

   print "files: " + str(files)
   print "sizes: " + str(sizes)
   
   
   for i in xrange( 0, len(files) - 1):
      file = files[i]
      size = sizes[i]
      fhash = hashes[i]
      
      test_iftd( file, fhash, size, remote_host, tmpfs_dir )
      os.popen("rm -rf " + file).close()
      print ""
      
   
   return 0


if __name__ == "__main__":
   Main()
