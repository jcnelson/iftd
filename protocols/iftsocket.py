#!/usr/bin/env python

"""
iftsocket.py
Copyright (c) 2009 Jude Nelson

This package defines a transfer protocol over a simple TCP socket.
It is push technology--the sender initiates the connection, and moves the data.
"""

import os
import sys
import protocols
import socket
import cPickle
import iftfile

import iftlog

from iftdata import *

import iftcore
import iftcore.iftsender
import iftcore.iftreceiver
from iftcore.consts import *

"""
Additional constants
"""
IFTSOCKET_LEN_DELIM = chr(0x01)        # delimits between length of chunk and message content
IFTSOCKET_CHUNK_DELIM = chr(0x02)      # delimits between chunk ID and length

IFTSOCKET_TIMEOUT = "IFTSOCKET_TIMEOUT"

"""
Sender for a TCP socket.
Requires:
   PROTO_PORTNUM          the port on which to send
"""
class iftsocket_sender( iftcore.iftsender.sender ):
   
   def __init__(self):
      iftcore.iftsender.sender.__init__(self)
      self.soc = None
      self.name = "iftsocket_sender"
      self.port = 0
      # sender is active
      self.setactive(True)
      self.set_chunking_mode( PROTO_DETERMINISTIC_CHUNKING )
   
   def get_setup_attrs(self):
      return [PROTO_PORTNUM]
   
   def get_connect_attrs(self):
      return []
   
   def get_send_attrs(self):
      return [iftfile.JOB_ATTR_SRC_NAME, iftfile.JOB_ATTR_SRC_HOST, iftfile.JOB_ATTR_FILE_SIZE]
   
   # what does this sender recognize?
   def get_all_attrs(self):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_send_attrs() + [PROTO_PORTNUM]
   
   def setup(self, connect_args):
      self.port = connect_args.get(PROTO_PORTNUM)
      return 0
         
      
   # prepare for transmission
   def prepare_transmit( self, job ):
      # file should have remote hostname
      p = job.get_attr( PROTO_PORTNUM )
      if p != None:
         self.port = p
         
      remote_host = None
      try:
         remote_host = job.attrs[ iftfile.JOB_ATTR_DEST_HOST]
      except Exception, inst:
         iftlog.exception( self.name + ": No remote host specified", inst )
         return E_NO_CONNECT
      
      timeout = job.get_attr( IFTSOCKET_TIMEOUT )
      if timeout == None:
         timeout = 5
      
      self.soc = None
      if remote_host != None and self.port != None:
         # try to connect to the remote host
         self.soc = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
         self.soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
         
         connect_rc = 0
         try:
            iftlog.log(1, self.name + ": connecting to " + remote_host + " on port " + str(self.port))
            self.soc.settimeout( 5 )
            self.soc.connect( (remote_host, self.port) )
         except Exception, inst:
            iftlog.exception( self.name + ": could not connect", inst)
            return E_NO_CONNECT
         
         if connect_rc != 0:
            return E_NO_CONNECT
         else:
            return 0
      else:
         return E_NO_VALUE
         
   
   # clean up
   def proto_clean( self ):
      # close up and nullify
      if self.soc != None:
         self.soc.close()
      self.soc = None
      
   
   # Send a chunk
   def send_chunk( self, chunk, chunk_id, chunk_path, remote_chunk_path ):
      # ignore chunk paths, since we send actual chunks
      
      if self.soc != None:
         total_sent = 0
         # send the length and chunk id
         header_str = str(chunk_id) + IFTSOCKET_CHUNK_DELIM + str(len( chunk )) + IFTSOCKET_LEN_DELIM
         header_sent = self.soc.send( header_str )
         if header_sent != len( header_str ):
            # problem...
            return E_NO_CONNECT
         
         while total_sent < len(chunk):
            sent = self.soc.send( chunk[total_sent:] )
            if sent == 0:
               return total_sent
            total_sent = total_sent + sent
         
         return total_sent
      else:
         return E_INVAL
      


"""
Receiver for a socket
Requires:
   PROTO_PORTNUM            The port to listen on
 
"""

class iftsocket_receiver( iftcore.iftreceiver.receiver ):
   
   def __init__(self):
      iftcore.iftreceiver.receiver.__init__(self)
      self.client_soc = None
      self.soc = None
      self.name = "iftsocket_receiver"
      self.port = 0
      self.job = None
      self.connected = False
      # receiver is not active
      self.setactive(False)
      self.set_chunking_mode( PROTO_DETERMINISTIC_CHUNKING )
      
      
   def get_setup_attrs(self):
      return [PROTO_PORTNUM]
   
   def get_connect_attrs(self):
      return []
   
   def get_recv_attrs( self ):
      return [iftfile.JOB_ATTR_DEST_NAME, iftfile.JOB_ATTR_DEST_HOST, iftfile.JOB_ATTR_FILE_SIZE, iftfile.JOB_ATTR_FILE_HASH]
   
   # what does this receiver recognize?
   def get_all_attrs( self ):
      return self.get_setup_attrs() + self.get_connect_attrs() + self.get_recv_attrs() + [iftfile.JOB_ATTR_CHUNKSIZE, IFTSOCKET_TIMEOUT]
   
   def setup( self, setup_attrs ):
      try:
         self.port = setup_attrs[PROTO_PORTNUM]
         iftlog.log(1, "iftsocket_receiver.setup: will receive on port " + str(self.port))
      except:
         return E_NO_VALUE
      
      # nothing to do...
      return 0
   
   # receive file attributes from an iftsocket_sender
   def recv_job( self, job ):
      self.job = job
      if job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE ) == None:
         self.job.attrs[ iftfile.JOB_ATTR_CHUNKSIZE ] = iftfile.DEFAULT_FILE_CHUNKSIZE
      
      return 0
   
   # wait for a connection (we're on our own thread by now)
   def await_sender( self, connection_attrs, timeout ):
      if connection_attrs != None:
         p = connection_attrs.get( PROTO_PORTNUM )
         if p != None:
            try:
               self.port = int(p)
            except:
               pass
      
      # set up a server socket to the remote host
      self.soc = None
      self.connected = False
      
      if connection_attrs.get( IFTSOCKET_TIMEOUT ) != None:
         self.timeout = connection_attrs.get( IFTSOCKET_TIMEOUT )
      else:
         self.timeout = 1
         
      if self.port != None:
         
         try:
            self.soc = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
            self.soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.soc.settimeout( self.timeout )      # use the given timeout
            self.soc.bind( ("localhost", self.port) )
            self.soc.listen(1)      # only one remote host should talk to me
            iftlog.log(1, "iftsocket_receiver: Listening on localhost:" + str(self.port) )
         except Exception, inst:
            iftlog.exception( "iftsocket_receiver: could not set up server socket", str(inst) )
            return E_NO_CONNECT
         
         return 0
      else:
         return E_NO_VALUE
      
   
   def proto_clean( self ):
      if self.soc != None:
         self.soc.close()
         
      if self.client_soc != None:
         self.client_soc.close()
      
      
      self.client_soc = None
      self.soc = None
      self.connected = False
      
      
   
   def recv_chunks( self, remote_chunk_dir, desired_chunks ):
   
      # wait for data...
      if not self.connected:   
         try:
            (self.client_soc, addr) = self.soc.accept()
            self.connected = True
         except socket.timeout:
            iftlog.log(5, "iftsocket_receiver: timed out (waited " + str(self.timeout) + " seconds)")
            return E_TIMEOUT
         
      status = 0
      
      # receive every chunk in desired_chunks
      while status == 0:
         
         # receive a chunk
         c = ''
         chunk_str = ''
         len_str = ''
      
         # read chunk id
         while True:
            
            recved = ''
            try:
               recved = self.client_soc.recv( 1 )
               if recved == IFTSOCKET_CHUNK_DELIM:
                  break
            
               if recved == '':
                  status = E_EOF
                  break
               
            except:
               status = E_NO_DATA
               break
            
            chunk_str = chunk_str + recved
         
         # can't go on if we have nonzero status
         if status != 0:
            break
         
         # read length
         while True:
            
            try:
               recved = self.client_soc.recv( 1 )
               if recved == IFTSOCKET_LEN_DELIM:
                  break
            
               if recved == '':
                  status = E_EOF
                  break
               
            except:
               status = E_NO_DATA
               break
            
            len_str = len_str + recved
         
         # can't go on if we have nonzero status
         if status != 0:
            break
         
         # read remainder of chunk
         chunk_len = int(len_str)
         chunk_id = int(chunk_str)
         chunk = ''
         recv_cnt = 0
         while chunk_len > recv_cnt:
            recved = ''
            
            try:
               recved = self.client_soc.recv( chunk_len - recv_cnt )
               if recved == '':
                  status = E_EOF
                  break
               
            except:
               status = E_NO_DATA
               break
            
            # store chunk bytes
            chunk = chunk + recved
            recv_cnt = recv_cnt + len(recved)
         
         # store our chunk in the table
         if status == 0:
            self.add_chunk( chunk_id, chunk )
            
            # clear the chunk_id in the desired_chunks array
            for i in range(0,len(desired_chunks)):
               if desired_chunks[i] == chunk_id:
                  desired_chunks[i] = 0
                  break
            
            # do we have everything?  sum(desired_chunks) == 0 if so
            sum = 0
            for i in desired_chunks:
               if i != 0:
                  sum = i
                  break
            
            if sum == 0:
               break    # got everything!
            
            
      return status

