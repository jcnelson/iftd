
import os
import sys
import thread
import time
import math
import copy
import types
import hashlib

# fix deepcopy problem with cloning instance methods (fixed in python2.6?)
def _deepcopy_method(x, memo):
    return type(x)(x.im_func, copy.deepcopy(x.im_self, memo), x.im_class)
copy._deepcopy_dispatch[types.MethodType] = _deepcopy_method



from collections import deque

from iftdata import *
import iftcore
import iftcore.ifttransmit
from iftcore.ifttransmit import transmitter

from iftcore.consts import *

import iftlog
import iftfile
import iftutil
import iftloader
import iftstats

class sender( iftcore.ifttransmit.transmitter ):
   
   """
   Base class for a data sender (needed by iftproto).
   NOTE:  You should pay attention to get_send_attrs() output
   before actually trying to send stuff--be prepared to catch exceptions.
   
   Runs run() in a separate thread and sends data out to a receiver.
   
   Defines the internal variable ift_job for the file transfer job
   """

   # constants
   IFTSENDER_ANY_CHUNKSIZE = -1
   
   # set the job to process
   def assign_job( self, job ):
      if self.ift_job == None:
         self.ift_job = copy.deepcopy(job)
   
   def __init__(self):
      transmitter.__init__(self)
      self.connect_args = None
      self.ift_job = None
      self.ready_to_send = False    # set once we call prepare_transmit
      self.send_finish = False
      self.send_status = 0
   
      # job to process
      ift_job = None
      
      # protocol name
      name = "iftsender"
   
      self.set_handler( PROTO_MSG_START, self.on_start )
      self.set_handler( PROTO_MSG_END, self.on_end )
      self.set_handler( PROTO_MSG_ERROR, self.on_error )
      self.set_handler( PROTO_MSG_ERROR_FATAL, self.on_fatal_error )
      self.set_handler( PROTO_MSG_TERM, self.on_term )
      self.set_handler( PROTO_MSG_USER, self.set_state )
      self.set_default_behavior( self.__send_file_chunks )
   
   
   def set_connection_attrs( self, connect_args ):
      """
      Set up our connection attributes.
      Return 0 if they are valid; nonzero if not.
      """
      
      rc = self.validate_attrs( connect_args, self.get_connect_attrs() )
      if rc == 0:
         self.connect_args = connect_args
         return 0
      return rc
   
   
   def on_start( self, job=None, connect_args=None, connect_timeout=0.01 ):
      """
      When we start, we should await a receiver (if needed) and then prepare to transmit
      Args are connection properties as a dict.
      
      You must call assign_job() or pass a non-None job to this function before calling this!
      """
      print "on_start"   
      
      self.transmit_state = TRANSMIT_STATE_DEAD
      
      # check connection args
      rc = self.validate_attrs( connect_args, self.get_connect_attrs() )
      if rc != 0:
         return rc
      
      
      # validate the job
      rc = self.validate_attrs( job.attrs, self.get_send_attrs() )
      if rc != 0:
         return rc
      
      
      if job != None:
         self.assign_job( job )
      
      
      # give the job to the sender
      result = 0
      try:
         result = self.send_job( self.ift_job )
      except Exception, inst:
         iftlog.exception( self.name + ": could not send_job", inst )
         
         self.ift_job = None
         self.transmit_state = TRANSMIT_STATE_FAILURE
         return result
     
      if result != 0:
         iftlog.log(5, self.name + ": ERROR: send_job rc=" + str(result) )
         return result
 
      try:
         self.connect_args = connect_args
         rc = self.await_receiver( connect_args, connect_timeout )
         if rc < 0:
            self.connect_args = None
            iftlog.log(5, self.name + ".alert_receiver rc=" + str(rc))
            self.transmit_state = TRANSMIT_STATE_FAILURE
            ifttransmit.on_end( self, connect_args )
            return rc
         
         else:
            
            rc = self.open_connection( self.ift_job )
            if rc < 0:
               iftlog.log(5, self.name + ".on_start: open_connection rc=" + str(rc))
               self.transmit_state = TRANSMIT_STATE_FAILURE
               iftcore.ifttransmit.transmitter.on_end( self, connect_args )
               return rc
            
            # good to start
            self.connect_args = connect_args
            self.transmit_state = TRANSMIT_STATE_CHUNKS
            iftcore.ifttransmit.transmitter.on_start( self, connect_args )
            
            return 0
      except Exception, inst:
         iftlog.exception( "iftsender.on_start failed!", inst)
         self.__transmit_state = TRANSMIT_STATE_FAILURE
         return E_UNHANDLED_EXCEPTION
      
   
   def on_end( self, args ):
      """
      Override on_end() so as to end transmission
      """
      self.__end_transmit( )
      ifttransmit.on_end( self, args )
      self.clean()
   
   
   
   def on_term(self, args):
      """
      Override on_term() to shutdown the protocol
      """
      if self.ready_to_send == True:
         self.__end_transmit( )
      self.kill(args)
   
   
   
   def get_send_attrs(self):
      """
      Get a list of file attribute keys that the protocol needs for sending a file
      @Return
         The list of keys
      """
      return None
   
   
   
   def await_receiver(self, connection_attrs, timeout=0.01 ):
      """
      Inform the receiver that we are preparing to send.
      Return 0 on success, negative on error
      """
      
      return 0
   
   
   
   def send_job( self, job ):
      """
      Initiate a transfer with a receiver by sending over the necessary file attributes.
      Return 0 on success, negative on error.
      """
      return E_NO_CONNECT
   
   
   def prepare_transmit( self, job ):
      """
      Prepare for sending a file (e.g. open the connection, negotiate with remote host, etc).
      This will be called by iftd to allow the protocol to set up a connection on a per-file basis.
      When this is called, it will execute in its own thread (as will subsequent calls to this instance).
      
      @arg job
         iftjob instance with data for file transmission 
         
         
      @return
         0 on success, negative on error
      """
      
      return 0
   
   
   
   def __prepare_transmit( self, job ):
      """
      The *actual* prepare_transmit
      """
   
      if self.ready_to_send == False:
         rc = self.prepare_transmit( job )
         if rc == 0:
            self.ready_to_send = True
            return 0
         else:
            self.ready_to_send = False
            return rc
      
      return E_BAD_STATE  # call this ONCE
   
   
   def end_transmit( self ):
      """
      @return
         0 if stopped; negative on error
      """
      
      return 0
   
   
   def __end_transmit( self ):
      """
      The *actual* end_transmit
      """
      
      rc = self.end_transmit()
      if rc == 0:
         self.ready_to_send = False
      
      return rc
   
   
   def open_connection( self, job ):
      """
      Initiate a connection with the receiving iftd instance on behalf of this iftd instance.
      This needs to succeed in only one of the protocols in use; then all protocols can send chunks.
      """
      
      # prepare to send
      try:
         send_rc = self.__prepare_transmit( self.ift_job )
         if send_rc < 0:
      
            iftlog.log(5, self.name + ": prepare_transmit rc=" + str(send_rc))
            self.ift_job = None
            return send_rc
         
      except Exception, inst:
         iftlog.exception( self.name + ": could not prepare to transmit!", inst)
         
         self.ift_job = None
         return E_UNHANDLED_EXCEPTION
      
      # we're ready to send chunks!
      self.start_time = time.time()
      return 0
      
   
   
   
   def close_connection( self, final_state=TRANSMIT_STATE_SUCCESS ):
      """
      Close up a connection.
      Invalidate any locks or references we have acquired.
      """
      self.__end_transmit()
      self.transmit_state = final_state
      self.ready_to_send = False
      self.end_time = time.time()
      iftlog.log(5, self.name + ": Transmission took " + str(self.end_time - self.start_time) + " ticks" )
      
   
   
   def send_chunk( self, chunk, chunk_id, chunk_path, remote_chunk_path ):
      """
      Send a chunk.
      One or more of the arguments will be None.
      Only used when there is a remote IFTD.
      Return number of bytes sent
      """
      return 0
   
   
   def __next_chunk( self ):
      """
      Get the next chunk to send.  Return (chunk, chunk_id)
      """
      if self.ift_job.get_attr( iftfile.JOB_ATTR_GIVEN_CHUNKS ) == True:
         # we have been given chunks in advance, so check the queue
         (chunk, chunk_id, chunk_path, remote_chunk_path) = self.ift_job.get_attr( iftfile.JOB_ATTR_GIVEN_CHUNKS ).get(True)
         return (chunk, chunk_id, chunk_path, remote_chunk_path)
      
      # chunks not given in advance, so there is only one chunk: the file
      return (None, 0, self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_NAME), self.ift_job.get_attr( iftfile.JOB_ATTR_DEST_NAME ) )
   


   def send_one_chunk( self, chunk, chunk_id, chunk_path, remote_chunk_path, job = None ):
      """
      Send a single chunk.
      
      @arg chunk
         The chunk data to send.  None is interpreted as "send the entire file"
      
      @arg chunk_id
         The numerical id of the chunk.  0 if chunk == None
      
      @arg chunk_path
         The path to the chunk on disk, or None if chunk != None
      
      @arg remote_chunk_path
         The path to which to send the chunk on the remote host.
      """
      
      stime = time.time()
      rc = self.send_chunk( chunk, chunk_id, chunk_path, remote_chunk_path )
      etime = time.time()
      
      status = False
      chunk_len = None
      if rc >= 0:
         status = True
      
      if chunk:
         chunk_len = max( len(chunk), rc )
      else:
         chunk_len = iftfile.get_filesize( self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) )
   
      if job == None:
         job = self.ift_job
         
      iftstats.log_chunk( job, self.name, status, stime, etime, chunk_len )
      return rc
      
   
   def __send_file_chunks( self ):
      """
      Call repeatedly in the ifttransmit main loop to send chunks.
      
      This will be called once prepare_transmit and possibly send_job have been called
      
      Return an event to be handled by ifttransmit
      """
      
      if self.ready_to_send == False or self.transmit_state != TRANSMIT_STATE_CHUNKS:
         return (0,E_BAD_STATE)
      
      # can we do anything?
      if self.ift_job == None and self.ready_to_send == True:
         iftlog.log(5, self.name + ": No job to process!  Use my assign_job() method and resume me")
         self.ready_to_send = False
         return (0,E_BAD_STATE)
      
   
      chunk = None
      chunk_id = -1
      rc = 0
      
      chunk, chunk_id, chunk_path, remote_chunk_path = self.__next_chunk()
      
      try:
         rc = self.send_one_chunk( chunk, chunk_id, chunk_path, remote_chunk_path )
      except Exception, inst:
         iftlog.exception( self.name + ": could not send data", inst )
         self.close_connection( TRANSMIT_STATE_FAILURE )
         
         t = time.time()
         iftstats.log_chunk( self.ift_job, self.name, False, t, t, 0 )
         
         return (PROTO_MSG_ERROR_FATAL, E_NO_DATA)
      
      # note:  rc < 0 indicates error, rc == 0 indicates the last chunk was sent, rc > 0 indicates more data to be sent
      if rc < 0:
         if chunk != None:
            iftlog.log( 5, self.name + ": protocol error sending chunk " + str(chunk_id) + " of file " + chunk_path + ", error = " + str(rc))
         else: 
            iftlog.log( 5, self.name + ": protocol error sending file " + str(self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_NAME )) + ", error=" + str(rc))
         
         return (PROTO_MSG_ERROR, rc)
      
      elif rc == 0:
         if chunk != None:
            iftlog.log( 1, self.name + ": sender indicates that " + str(chunk_id) + " of file " + chunk_path + " was the last chunk")
         else:
            iftlog.log( 1, self.name + ": sender indicates that it has sent file " + str(self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_NAME )) + " successfully")
         
         self.close_connection( TRANSMIT_STATE_SUCCESS )
         return (PROTO_MSG_END, None)
      
      else:
         return (0,0)
   
   
   def send_finished(self, status):
      """
      Indicate that we are done receiving
      """
      self.send_finish = True
      self.send_status = status
      
   
   def clean(self):
      """
      When we clean up, release the reference to the file we acquired
      """
      
      self.proto_clean()
      iftcore.ifttransmit.transmitter.clean(self)
      self.ift_job = None
      self.send_finish = False
      self.send_status = 0
   
   
   
   def proto_clean(self):
      """
      Protocol-specific cleanup
      """
      
      return 
   
   
   def kill(self, args):
      """
      Kill myself
      """
      
      return
   
   
   def set_state( self, newstate ):
      """
      Forcibly set my ifttransmit state
      """
      
      self.state = newstate

