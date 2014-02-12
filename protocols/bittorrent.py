#!/usr/bin/env python

"""
bittorrent.py
Copyright (c) 2009 Jude Nelson

Implementation of a bittorrent sender and receiver.
The sender will make a .torrent file out of the source file, write it to a given location, and send it to a receiver as part of the job.
The receiver will use a .torrent file to retrieve a file.
Both sender and receiver are active, although the sender doesn't actually send anything except for a job.
The bittorrent plugin doesn't assume there is a remote iftd

DEPENDENCIES:  you'll need Rasterbar's libtorrent and its python bindings!
"""

import libtorrent as lt 

import os
import sys
import protocols

import iftapi
import iftcore
import iftcore.iftsender
import iftcore.iftreceiver
from iftcore.consts import *

import iftfile
import cPickle
import iftlog
import time
from iftdata import *

# path to the .torrent file on disk.
# to the sender, this is the .torrent output
# to the receiver, this is the .torrent input
IFTBITTORRENT_TORRENT_PATH = "IFTBITTORRENT_TORRENT_PATH"

# should I allow for a DHT?
IFTBITTORRENT_USE_DHT = "IFTBITTORRENT_USE_DHT"

# lower port range bound
IFTBITTORRENT_PORTRANGE_LOW = "IFTBITTORRENT_PORTRANGE_LOW"

# upper port range bound
IFTBITTORRENT_PORTRANGE_HIGH = "IFTBITTORRENT_PORTRANGE_HIGH"

# maximum download rate
IFTBITTORRENT_MAXIMUM_DOWN = "IFTBITTORRENT_MAXIMUM_DOWN"

# maximum upload rate
IFTBITTORRENT_MAXIMUM_UP = "IFTBITTORRENT_MAXIMUM_UP"

# tracker list for the sender to use in creating the .torrent file
IFTBITTORRENT_TRACKER = "IFTBITTORRENT_TRACKER"

# http seeds for the file to send
IFTBITTORRENT_HTTP_SEEDS = "IFTBITTORRENT_HTTP_SEEDS"

# job output path
IFTBITTORRENT_JOB_OUTPUT = "IFTBITTORRENT_JOB_OUTPUT"

# torrent bits
IFTBITTORRENT_TORRENT_BITS = "IFTBITTORRENT_TORRENT_BITS"

"""
Bittorrent sender (e.g. seeder)
"""
class bittorrent_sender( iftcore.iftsender.sender ):
   def __init__(self):
      iftcore.iftsender.sender.__init__(self)
      self.name = "bittorrent_sender"
      self.file_to_send = ""
      self.torrent_handle = None
      self.torrent_info = None
      self.portrange_low = 0
      self.portrange_high = 0
      self.bt_session = None
      self.max_down = -1    # arbitrarily huge
      self.max_up = -1      # arbitrarily huge
      self.torrent_str = None    # string representation of a .torrent file
      
      # sender is passive
      self.setactive(False)
      self.set_chunking_mode( PROTO_NONDETERMINISTIC_CHUNKING )
   
   def __deepcopy__( self, memo ):
      """
      On deep copy, only replicate the setup attributes.
      Ignore everything else.
      """
      ret = bittorrent_sender()
      ret.setup_attrs = copy.deepcopy( self.setup_attrs, memo )
      ret.bt_session = self.bt_session
      return ret
   
   
   # what do I need to know to set up?
   def get_setup_attrs(self):
      return []
   
   # what do I need to know to connect?
   def get_connect_attrs(self):
      return []
   
   # what do I need to seed?
   def get_send_attrs(self):
      return [iftfile.JOB_ATTR_SRC_NAME]
   
   # what are all of the possible parameters?
   def get_all_attrs(self):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_send_attrs() + [IFTBITTORRENT_PORTRANGE_LOW, IFTBITTORRENT_PORTRANGE_HIGH, IFTBITTORRENT_TRACKER, IFTBITTORRENT_HTTP_SEEDS, IFTBITTORRENT_TORRENT_PATH, IFTBITTORRENT_JOB_OUTPUT, IFTBITTORRENT_TORRENT_BITS]
   
   # one-time setup
   def setup( self, connect_attrs ):
      self.bt_session = lt.session()   # start up the session
      
      low = self.setup_attrs.get( IFTBITTORRENT_PORTRANGE_LOW )
      high = self.setup_attrs.get( IFTBITTORRENT_PORTRANGE_HIGH )
      
      if low == None:
         low = 6000
      
      if high == None:
         high = 7000
      
      self.bt_session.listen_on( low, high )
      iftlog.log(1, "libtorrent session created!")

      return 0    # we're ready for torrent files
   
   # prepare to transmit (e.g. respond to suspend/resume)
   def prepare_transmit( self, job ):
      # create the .torrent file from the given file
      fs = lt.file_storage()
      
      lt.add_files( fs, job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) )
      
      ct = lt.create_torrent( fs, job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE ) )
      
      ct.set_creator("iftd: " + self.name )
      
      self.file_to_send = job.get_attr( iftfile.JOB_ATTR_SRC_NAME )
      
      # if we were given a tracker or list of trackers, add them
      if job.get_attr( IFTBITTORRENT_TRACKER ) != None:
         if type(job.get_attr( IFTBITTORRENT_TRACKER )) == str:
            ct.add_tracker( job.get_attr( IFTBITTORRENT_TRACKER ), 0 )
         
         if type(job.get_attr( IFTBITTORRENT_TRACKER )) == list:
            for tracker in job.get_attr( IFTBITTORRENT_TRACKER ):
               ct.add_tracker(tracker, 0)
            
         
      
      else:
         # add some default trackers
         ct.add_tracker("http://tracker.openbittorrent.com/announce", 0)
         ct.add_tracker("udp://tracker.openbittorrent.com:80/announce", 0)
         ct.add_tracker("http://tracker.publicbt.com/announce", 0)
         ct.add_tracker("udp://tracker.publicbt.com:80/announce", 0)
      
      # if we were given one or more http seeds, add them too
      if job.get_attr( IFTBITTORRENT_HTTP_SEEDS ) != None:
         if type(job.get_attr( IFTBITTORRENT_HTTP_SEEDS )) == str:
            ct.add_url_seed( job.get_attr( IFTBITTORRENT_HTTP_SEEDS ) )
          
         if type(job.get_attr( IFTBITTORRENT_HTTP_SEEDS )) == list:
            for seed in job.get_attr( IFTBITTORRENT_HTTP_SEEDS ):
               ct.add_url_seed( seed )
      
      lt.set_piece_hashes( ct, os.path.dirname( job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) ) )
      
      # encode the torrent into a .torrent buffer
      self.torrent_str = lt.bencode( ct.generate() )
      
      # if given a torrent path, write out the torrent
      if job.get_attr( IFTBITTORRENT_TORRENT_PATH ) != None:
         try:
            fd = open( job.get_attr( IFTBITTORRENT_TORRENT_PATH ), "wb" )
            fd.write( self.torrent_str )
            fd.close()
         except Exception, inst:
            iftlog.exception( self.name + ": could not output torrent data to " + job.get_attr( IFTBITTORRENT_TORRENT_PATH ), inst)
            return E_IOERROR
      
      
      # begin to seed
      entry = lt.bdecode( self.torrent_str )
      tinfo = lt.torrent_info( entry )
     
      self.torrent_handle = self.bt_session.find_torrent( tinfo.info_hash() )
      if not self.torrent_handle.is_valid():
         self.torrent_handle = self.bt_session.add_torrent( tinfo, os.path.dirname( job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) ) )
         print "seeding " + job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) + " for the first time"
      else:
         print "already seeding " + job.get_attr( iftfile.JOB_ATTR_SRC_NAME )
 
      return 0
   
   # send job
   def send_job( self, job ):
      return 0
   
   # send a chunk (really, do nothing but start sending this file)
   def send_chunk( self, chunk, chunk_id, chunk_path, remote_chunk_path ):
      return 0
   
   
"""
BitTorrent receiver (a peer that is not yet a seed)
"""
class bittorrent_receiver( iftcore.iftreceiver.receiver ):
   def __init__(self):
      iftcore.iftreceiver.receiver.__init__(self)
      self.name = "bittorrent_receiver"
      self.torrent_handle = None
      self.torrent_info = None
      self.portrange_low = 0
      self.portrange_high = 0
      self.bt_session = None
      self.max_down = -1    # arbitrarily huge
      self.max_up = -1      # arbitrarily huge
      self.recv_prev = set([])   # what has been received on the last call to recv()
      self.job = None
      
      # receiver is passive
      self.setactive(False)
      self.set_chunking_mode( PROTO_NONDETERMINISTIC_CHUNKING )
      
   
   def __deepcopy__( self, memo ):
      """
      On deep copy, only replicate the setup attributes.
      Ignore everything else.
      """
      ret = bittorrent_receiver()
      ret.setup_attrs = copy.deepcopy( self.setup_attrs, memo )
      ret.bt_session = self.bt_session
      return ret
   
   
   # get the list of chunks received
   def get_chunk_list(self):
      if self.torrent_handle != None:
         torrent_status = self.torrent_handle.status()
         if torrent_status == None:
            return []
         
         pieces_sent = list(torrent_status.pieces)
         
         # convert bitfield into list of indices within it that are True
         chunks_sent = []
         chunk_id = 0
         for piece in pieces_sent:
            if piece == True:
               chunks_sent.append( chunk_id )
            chunk_id += 1
         
         return chunks_sent
      else:
         return []

   # what do I need to know to set up?
   def get_setup_attrs(self):
      return []
   
   # what do I need to know to connect?
   def get_connect_attrs(self):
      return [IFTBITTORRENT_PORTRANGE_LOW, IFTBITTORRENT_PORTRANGE_HIGH]
   
   # what do I need in the job to start receiving?
   def get_recv_attrs(self):
      return [IFTBITTORRENT_TORRENT_PATH, iftfile.JOB_ATTR_DEST_NAME]
   
   # get all attributes bittorrent recognizes
   def get_all_attrs(self):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_recv_attrs() + [IFTBITTORRENT_JOB_RETRIEVER, IFTBITTORRENT_TORRENT_PATH, iftfile.JOB_ATTR_FILE_SIZE]
   
   # one-time setup
   def setup( self, setup_attrs ):
      self.setup_attrs = setup_attrs
      
      # start bt session
      self.bt_session = lt.session()
      
      # DHT?
      if setup_attrs.has_key( IFTBITTORRENT_USE_DHT ) and setup_attrs[ IFTBITTORRENT_USE_DHT ] == True:
         self.bt_session.start_dht( "" )
      
      return 0
   
   # per-file setup
   def await_sender( self, connect_attrs, timeout ):
      self.connect_args = connect_attrs
      self.bt_session.listen_on( self.setup_attrs[ IFTBITTORRENT_PORTRANGE_LOW ], self.setup_attrs[ IFTBITTORRENT_PORTRANGE_HIGH ] )
      return 0
   
   # receive a job
   def recv_job( self, job ):
      
      self.file_to_recv = job.get_attr( iftfile.JOB_ATTR_DEST_NAME )
      
      # begin to receive
      torrent_path = job.get_attr( IFTBITTORRENT_TORRENT_PATH )
      torrent_bits = None
      try:
         torrent_fd = open( torrent_path, "r" )
         torrent_bits = torrent_fd.read()
      except Exception, inst:
         iftlog.exception( self.name + ": could not read torrent file " + str(torrent_path), inst)
         return E_NO_DATA
      
      entry = lt.bdecode( torrent_bits )
      tinfo = lt.torrent_info( entry )
      
      # make sure this dir exists
      bt_dir = iftfile.get_chunks_dir( job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), job.get_attr( iftfile.JOB_ATTR_FILE_HASH ) ) + ".bt." + str(tinfo.info_hash())[:10]
      self.bt_dir = bt_dir
      
      if not os.path.exists( bt_dir ):
         try:
            rc = os.popen("mkdir -p " + bt_dir ).close()
            if rc != None:
               iftlog.log(5, self.name + ": ERROR: could not create directory " + bt_dir )
               return E_IOERROR
         except Exception, inst:
            iftlog.exception( self.name + ": ERROR: could not create directory " + bt_dir )
            return E_IOERROR


      job.set_attr( iftfile.JOB_ATTR_FILE_SIZE, tinfo.total_size() )
      job.set_attr( iftfile.JOB_ATTR_CHUNKSIZE, tinfo.piece_length() )
      
      self.torrent_handle = self.bt_session.add_torrent( tinfo, bt_dir )
      self.torrent_info = tinfo
      self.chunksize = job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE )
      return 0
      
   
   # clean up
   def proto_clean( self ):
      if self.torrent_handle != None:
         iftlog.log( 3, self.name + ": purging " + self.torrent_handle.save_path())
         os.popen("rm -rf " + self.torrent_handle.save_path() ).close()
         self.bt_session.remove_torrent( self.torrent_handle )
      
      self.torrent_handle = None
      self.torrent_info = None
      self.chunksize = None
      
   # kill myself
   def kill( self, kill_args ):
      self.proto_clean()
   
   
   # receive data
   def recv_chunks( self, remote_chunk_dir, desired_chunks ):
      
      # determine what has been received since the last time this was called
      chunk_list = self.get_chunk_list()
      active_set = set( chunk_list )
      
      # wait until we actually receive something
      while len(active_set) - len(self.recv_prev) == 0:
         time.sleep(0.5)
         active_set = active_set | set( self.get_chunk_list() )
         
         if self.torrent_handle.is_seed():
            # we have all chunks
            active_set = set([i for i in xrange(0, self.torrent_handle.status().num_pieces)])
            break
         
         
         
         s = self.torrent_handle.status()
         state_str = ['queued', 'checking', 'downloading metadata', \
                'downloading', 'finished', 'seeding', 'allocating']
         print '%.2f%% complete (down: %.1f kb/s up: %.1f kB/s peers: %d) %s' % \
                (s.progress * 100, s.download_rate / 1000, s.upload_rate / 1000, \
                s.num_peers, state_str[s.state])
  
         
         if not iftapi.is_alive():
            return E_FAILURE
         continue
      
      # indicate what we have received
      new_chunks = active_set - self.recv_prev
      
      iftlog.log(3, self.name + ": received " + str(len(new_chunks)) + " more chunks")
     
      self.recv_prev = active_set
         
      print "have now received " + str(len(self.recv_prev)) + " chunks" 
      # convert to dictionary
      bt_dir = self.torrent_handle.save_path()
      rc = 0
      for chunk_id in new_chunks:
         file_slices = self.torrent_info.map_block( chunk_id, 0, self.chunksize )    # which file(s) did this chunk correspond to?
         for fs in file_slices:
            recv_file = self.torrent_info.file_at( fs.file_index )
            try:
               chunk_fd = open( bt_dir + "/" + recv_file.path, "r")
               chunk_fd.seek( fs.offset )
               chunk_data = chunk_fd.read( self.chunksize )
               if chunk_data:
                  self.add_chunk( chunk_id, chunk_data )
               chunk_fd.close()
               print "copy chunk " + str(chunk_id) + " from " + str(recv_file.path) + " at offset " + str(fs.offset) + ", length " + str(len(chunk_table[chunk_id])) + " (chunksize is " + str(self.chunksize) + ")"
            except Exception, inst:
               iftlog.exception( self.name + ": could not get chunk " + str(chunk_id) + " from " + str(recv_file) + " at offset " + str(fs.offset), inst)
               rc = E_IOERROR
               continue
      
      return rc
      
