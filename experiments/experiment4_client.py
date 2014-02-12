#!/usr/bin/python

"""
=============
Experiment 4:  IFTD recoverability and resumability
=============
Purpose:
    To measure how quickly IFTD can resume file transmission in the event of a protocol failure.

Expected Result:
    IFTD will be slower at transferring the file if there are multiple protocol failures, but the file will successfully download without having to completely re-download data, as with arizonatransfer.

Setup:
    There are two hosts:  the server and the client.  The server has a file of size 1MB on a tmpfs mount.  Both the server and the client run IFTD, and both instances support HTTP, scp, and BitTorrent protocols.
    HTTP and BitTorrent support resuming natively, whereas scp does not.  Each of the protocols have been modified to fail after they give back their 5th received chunk to IFTD.
    IFTD will have a chunksize of 50 KB, so it will finish the transfer when the last protocol sends its last chunk.  We expect to see it fail and recover 3 times.  

Procedure:
1.  Have IFTD attempt to download the file 30 times, and record timestamps (with Python's time module) for each transfer and which protocol performed the transfer and when each protocol failed.  Have IFTD verify the file's integrity.



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
import iftproto
import time

import arizonatransfer
import arizonaconfig

import iftapi
import iftproto
import protocols
import protocols.bittorrent   

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
        iftfile.JOB_ATTR_FILE_HASH:filehash,
        iftfile.JOB_ATTR_DEST_NAME:file,
        iftfile.JOB_ATTR_CHUNKSIZE:int(filesize) / 200,
        iftfile.JOB_ATTR_DEST_HOST:"localhost",
        protocols.bittorrent.IFTBITTORRENT_TORRENT_PATH:file + ".torrent",
        iftfile.JOB_ATTR_PROTOS:['http5_receiver', 'iftscp2_receiver', 'bittorrent2_receiver']
   }

   bt_connect_attrs = {
      iftfile.JOB_ATTR_DEST_NAME:file,
      iftfile.JOB_ATTR_SRC_NAME:file,
      protocols.bittorrent.IFTBITTORRENT_PORTRANGE_LOW:1025,
      protocols.bittorrent.IFTBITTORRENT_PORTRANGE_HIGH:65534,
      protocols.bittorrent.IFTBITTORRENT_TORRENT_PATH:"/tmp/iftd-experiments/" + file + ".torrent"
   }
   
   client = iftapi.make_XMLRPC_client( timeout = 60 )
   
   connects = {
      "http5_receiver":http_connect_attrs,
      "http5_sender":http_connect_attrs,
      "iftscp2_sender":None,
      "iftscp2_receiver":None,
      "bittorrent2_sender":bt_connect_attrs,
      "bittorrent2_receiver":bt_connect_attrs
   }
   
   client.clear_classifier()
 
   for i in xrange(0, 1):
      # transfer the file with iftd 
      ts = time.time()
      rc = client.begin_ift( job_attrs, connects, False, True, 4001, "/RPC2", True, False, 60)
      te = time.time()
      
      iftlog.log(5, "iftd " + remote_host + file + " " + str(te - ts) )
      if rc != iftproto.ifttransmit.TRANSMIT_STATE_SUCCESS:
         iftlog.log(5, "iftd failed to transfer " + str(file))

      os.popen("rm -rf " + file).close()
   
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
   
   files = ["/tmp/iftd-experiments/file_1000000"]
   sizes = [1000000]
   hashes = ['7d24706d7228d63fde8291ceb345fa0c51f38aa4']
   
   
   for i in xrange(0, len(files)):
      file = files[i]
      size = sizes[i]
      fhash = hashes[i]
      
      test_iftd( file, fhash, size, remote_host, tmpfs_dir )
      os.popen("rm -rf " + file).close()
      print ""
      
   
   return 0


if __name__ == "__main__":
   Main()
