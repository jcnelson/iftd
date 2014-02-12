import iftcore
from iftcore.consts import *

import os
import sys
import thread
import time
import math
import copy
import types
import hashlib
import shutil
import threading

from collections import deque

from iftdata import *

import iftlog
import iftfile
import iftutil
import iftloader
import iftstats


class receiver( iftcore.ifttransmit.transmitter ):
   """
   Base class for a data receiver (needed by iftproto).
   NOTE:  Pay attention to the output of get_recv_attrs()--you may have to catch exceptions.
   
   
   It defines the internal variable ift_job that describes the file transfer job
   """

   
   def __init__(self):
      iftcore.ifttransmit.transmitter.__init__(self)
      self.ift_job = None
      self.iftfile_ref = None
      self.connect_args = None
      self.ready_to_receive = False    # can't transmit unless this is true
      self.start_time = -1    # transmission timing
      self.end_time = -1
      self.recv_finish = False    # not done yet
      self.recv_status = 0        # not done yet
      self.has_whole_file = False    # set to true if we suddently get the whole file
      
      # protocol name
      name = "iftreceiver"
      
      self.set_handler( PROTO_MSG_START, self.on_start )
      self.set_handler( PROTO_MSG_END, self.on_end )
      self.set_handler( PROTO_MSG_ERROR, self.on_error )
      self.set_handler( PROTO_MSG_ERROR_FATAL, self.on_fatal_error )
      self.set_handler( PROTO_MSG_TERM, self.on_term )
      self.set_handler( PROTO_MSG_USER, self.set_state )
      self.set_default_behavior( self.__receive_data )
      self.set_chunking_mode( PROTO_NO_CHUNKING )
      
      
   def get_connection_args(self):
      return self.connect_args
   
   
   def set_connection_attrs( self, connect_args ):
      """
      Assign our connection attributes.
      Return 0 if they are valid.  Nonzero if not.
      """
      rc = self.validate_attrs( connect_args, self.get_connection_args() )
      if rc == 0:
         self.connect_args = connect_args
         return 0
      return rc
   
   
   def on_start( self, job, connect_args=None, connect_timeout=0.01 ):
      """
      When we start, we should await a sender
      Args are connection properties as a dict
      """
      
      if connect_args == None:
         # use our given connect args if none were given
         connect_args = self.connect_args
      
      # check connection args
      rc = self.set_connection_attrs( connect_args )
      if rc != 0:
         return rc
      
      self.transmit_state = TRANSMIT_STATE_DEAD
      self.ift_job = None
      self.iftfile_ref = None
      
      # give the job
      try:
         rc = self.validate_attrs( job.attrs, self.get_recv_attrs() )
         if rc < 0:
            return rc
         
         status = self.recv_job( job )
         
         if status != 0:
            iftlog.log(5, self.name + ": recv_job rc=" + str(status))
            return status
         
         self.ift_job = job
         
      except Exception, inst:
         iftlog.exception( self.name + ": could not receive job", inst )
         return E_UNHANDLED_EXCEPTION
      
      # attempt to connect
      try:
         rc = self.__await_sender( connect_args, connect_timeout )
         if rc < 0:
            iftlog.log(5, self.name + ".on_start: await_sender rc=" + str(rc))
            self.transmit_state = TRANSMIT_STATE_FAILURE
            iftcore.ifttransmit.transmitter.on_end( self, connect_args )
            return rc
         else:
            rc = self.open_connection( job )
            if rc < 0:
               iftlog.log(5, self.name + ".on_start: open_connection rc = " + str(rc))
               self.transmit_state = TRANSMIT_STATE_FAILURE
               iftcore.ifttransmit.transmitter.on_end( self, connect_args )
               return rc
            
            self.connect_args = connect_args
            
            # we're connected!
            self.transmit_state = TRANSMIT_STATE_CHUNKS
      
            iftcore.ifttransmit.transmitter.on_start( self, connect_args )
            
            return 0
      except Exception, inst:
         iftlog.exception( "iftreceiver.on_start failed!", inst)
         self.transmit_state = TRANSMIT_STATE_FAILURE
         iftcore.ifttransmit.transmitter.on_end( self, connect_args )
         return E_UNHANDLED_EXCEPTION
      
   
   def on_end( self, args ):
      """
      Override on_end() so as to end transmission
      """
      #print self.name + ": ended"
      self.__end_receive()
      iftcore.ifttransmit.transmitter.on_end( self, args )
      self.clean()
   
   
   def on_term(self, args):
      """
      Override on_term() to shutdown the protocol
      """
      
      if self.ready_to_receive == True:
         self.__end_receive()
      self.kill(args)
   
   
   def get_recv_attrs(self):
      """
      Get a list of file attribute keys that the protocol needs for receiving a file
      @Return
         The list of keys
      """
      return None
      
   
   
   def __await_sender(self, connection_attrs, timeout=0.0 ):
      """
      Wait for a sender to connect.  Take no more than the given timeout (in seconds) to complete.
      This method should only block if the receiver is active (otherwise it will not be expected
      to block, and blocking operations may cause the process to freeze).
      
      Return 0 on success, negative on error
      """
      
      self.connect_args = connection_attrs
      return self.await_sender( connection_attrs, timeout )
   
   
   
   def await_sender(self, connection_attrs, timeout=0.0 ):
      """
      Wait for a sender to connect.  Take no more than the given timeout (in seconds) to complete.
      This method should only block if the receiver is active (otherwise it will not be expected
      to block, and blocking operations may cause the process to freeze).
      
      Return 0 on success, negative on error
      """
      
      return 0
   
   
   def prepare_receive( self, job ):
      """
      Prepare to converse with the sender.
      Return 0 on success, negative on error
      """
      
      return 0
      
   
   def __prepare_receive( self, job ):
      """
      The *real* prepare_receive
      """
      
      if self.ready_to_receive == False:
         rc = self.prepare_receive( job )
         if rc == 0:
            self.ready_to_receive = True
            return 0
         else:
            self.ready_to_receive = False
            return rc
      
      return E_BAD_STATE   # should only be called once
      
   
   def end_receive( self ):
      """
      Stop receiving.
      """
      
      return 0
   
   
   def __end_receive( self ):
      """
      The *real* end_receive
      """
      rc = self.end_receive()
      if rc == 0:
         self.ready_to_receive = False
   
   
   
   def recv_job( self, job ):
      """
      Load job data from the given job.
      This MUST be an idempotent operation
      """
      return 0   
   
   
   def open_connection( self, job ):
      """
      Get the job and validate receive attributes.
      If a job is given, then don't receive the job; just open the connection and use the given job.
      """
      
      # prepare to receive
      recv_status = 0
      try:
         recv_status = self.__prepare_receive( job )
      except Exception, inst:
         iftlog.exception( self.name + ": could not prepare to receive", inst)
         return E_UNHANDLED_EXCEPTION
      
      if recv_status < 0:
         return recv_status
      
      self.start_time = time.time()

      # open the file
      self.iftfile_ref = self.ift_job.get_attr( iftfile.JOB_ATTR_IFTFILE )
      if self.iftfile_ref == None:
         self.close_connection( TRANSMIT_STATE_FAILURE )
         return E_FILE_NOT_FOUND
      
      return 0
   
   
   
   def close_connection( self, final_state ):
      """
      Close up a connection.
      Invalidate any locks or references we have acquired.
      """
      self.__end_receive()
      
      if self.iftfile_ref != None:
         #print self.name + ".close_connection: releasing my iftfile"
         self.iftfile_ref.unreserve_all( self )
         self.iftfile_ref = None
      
      self.transmit_state = final_state
      self.ready_to_receive = False
      self.end_time = time.time()
      
      iftlog.log(5, self.name + ": Transmission took " + str(self.end_time - self.start_time) + " ticks" )
      
   
   
   
   def __receive_chunk_data( self ):
      """
      If this protocol is to accumulate chunks as byte strings, then receive chunks of data.
      """
      
      if self.ift_job == None and (self.ready_to_receive == False or self.transmit_state != TRANSMIT_STATE_CHUNKS):
         return (0, E_BAD_STATE)     # nothing to do
      
      # try to receive a chunk
      chunk_table = None
      recv_rc = 0
      
      try:
         # if we can be assured that we'll get the chunks we want, go ahead and receive the next unreceived chunks
         
         recv_rc = 0
         chunk_table = None

         desired_chunk_ids = self.__next_chunks()
         if len(desired_chunk_ids) == 0:
            # they're all reserved...
            #print self.name + ": all reserved..."
            return (PROTO_MSG_NONE, E_TRY_AGAIN)

         # attempt to receive
         stime = time.time()
         recv_rc, chunk_table = self.__recv_chunks( self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_CHUNK_DIR ), desired_chunk_ids )
         etime = time.time()
         
         status = {}
         
         # if by some miracle we got the whole file at once, then check the file and be done with it.
         whole_file_path = chunk_table.get("whole_file")
         if whole_file_path != None:
            iftlog.log(3, self.name + ": got whole file back, saved in " + whole_file_path)
            shutil.move( whole_file_path, self.iftfile_ref.path )
            iftfile.apply_dir_permissions( self.iftfile_ref.path )
            self.iftfile_ref.mark_complete()
            return self.__recv_cleanup( TRANSMIT_STATE_SUCCESS )

         # validate the chunk table otherwise.
         elif chunk_table and len(chunk_table.keys()) > 0:     
         
            # verify chunk hashes if we need to.
            # record each chunk if that's what we got
            if self.ift_job.get_attr( iftfile.JOB_ATTR_CHUNK_HASHES ) != None:
               # verify each hash
               chunk_hashes = self.ift_job.get_attr( iftfile.JOB_ATTR_CHUNK_HASHES )
               for k in chunk_table.keys():
                  if len(chunk_hashes) > k:
                     m = hashlib.sha1()
                     m.update( chunk_table[k] )
                     if chunk_hashes[k] != m.hexdigest():
                        iftlog.log(5, self.name + ": chunk " + str(k) + "'s hash is incorrect!")
                        status[k] = False
                     else:
                        status[k] = True
         
         
            # log chunk data
            for k in chunk_table.keys():
               if status == {}:
                  if recv_rc == 0:
                     iftstats.log_chunk( self.ift_job, self.name, True, stime, etime, self.ift_job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE ) )    # no way to verify chunk correctness
                  else:
                     iftstats.log_chunk( self.ift_job, self.name, False, stime, etime, self.ift_job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE ) )
               
               else:
                  iftstats.log_chunk( self.ift_job, self.name, status[k], stime, etime, self.ift_job.get_attr( iftfile.JOB_ATTR_CHUNKSIZE ) )
               
            
            # if we had a non-zero RC, we should warn the user
            if recv_rc != 0:
               # got some data, but still encountered an error so we need to emit a warning
               # are we missing any chunks?
               not_received = []
               for recved_id in chunk_table.keys():
                  if desired_chunk_ids.count(recved_id) == 0:
                     not_received.append( recved_id )
               
               iftlog.log(5, "WARNING: " + self.name + " receive RC = " + str(recv_rc))
               if len(not_received) != 0:
                  iftlog.log(5, "WARNING: " + self.name + " did not receive chunks " + str(not_received))

            # store chunks
            for chunk_id in chunk_table.keys():
               msg, rc = self.__store_chunk( chunk_table[chunk_id], chunk_id )
               if rc != E_DUPLICATE and (msg == PROTO_MSG_ERROR or msg == PROTO_MSG_ERROR_FATAL):
                  if msg == PROTO_MSG_ERROR_FATAL:
                     self.__recv_cleanup( TRANSMIT_STATE_FAILURE )
                  return (msg, rc)
                  
            # are we done?
            if self.iftfile_ref.is_complete():
               return self.__recv_cleanup( TRANSMIT_STATE_SUCCESS )

            
            # are we finished receiving, as indicated by the protocol?
            if self.recv_finish:
               return self.__recv_cleanup( self.recv_status )
               
            return (0, 0)     # get moar!
            
         
         elif recv_rc != 0:
            # negative RC and no data given back
            if recv_rc == E_EOF:
               # nothing left for us to receive--save the file (if we handle file I/O) and exit
               return self.__recv_cleanup( TRANSMIT_STATE_SUCCESS )
            
            else:
               # are we finished receiving, as indicated by the protocol?
               if self.recv_finish:
                  return self.__recv_cleanup( self.recv_status )

               self.__recv_cleanup( TRANSMIT_STATE_FAILURE )
               return (PROTO_MSG_ERROR_FATAL, recv_rc)

         
         else:
            # recv_rc == 0 and no chunks given
            # are we finished receiving, as indicated by the protocol?
            if self.recv_finish:
               return self.__recv_cleanup( self.recv_status )

            return (0, 0)     # just try again...
        
            
      except Exception, inst:
         iftlog.exception( self.name + ": could not receive chunk", inst )
         self.close_connection( TRANSMIT_STATE_FAILURE )
         
         t = time.time()
         iftstats.log_chunk( self.ift_job, self.name, False, t, t, 0 )
         
         self.__recv_cleanup( TRANSMIT_STATE_FAILURE )
         return (PROTO_MSG_ERROR_FATAL, recv_rc)

      
   
   def __receive_file_data( self ):
      """
      If this protocol is to accumulate files straight to disk, then receive files.
      """
      
      if self.ift_job == None and (self.ready_to_receive == False or self.transmit_state != TRANSMIT_STATE_CHUNKS):
         return (0, E_BAD_STATE)     # nothing to do

      
      # attempt to get the files
      files_list = None
      recv_rc = 0
      
      try:
         # if we can be assured that we'll get the chunks we want, go ahead and receive the next unreceived chunks
         
         recv_rc = 0
         chunk_table = None

         file_names = self.__next_files()
         if len(file_names) == 0:
            # they're all reserved...
            #print self.name + ": all reserved..."
            return (PROTO_MSG_NONE, E_TRY_AGAIN)


         # attempt to receive the files
         stime = time.time()
         recv_rc, files_list = self.__recv_files( file_names, self.ift_job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ) )
         etime = time.time()
         
         # did we get the whole file back?
         if len(files_list) > 0 and files_list[0][0] == -1:
            whole_file_path = files_list[0][1]
            iftlog.log(3, self.name + ": got whole file back, saved in " + whole_file_path)
            try:
               shutil.move( whole_file_path, self.iftfile_ref.path )
               iftfile.apply_dir_permissions( self.iftfile_ref.path )
               self.iftfile_ref.mark_complete()
               return self.__recv_cleanup( TRANSMIT_STATE_SUCCESS )
            except Exception, inst:
               if self.iftfile_ref.path == None:      # someone beat us...
                  iftlog.log(3, self.name + ": file is already received")
                  os.remove( whole_file_path )
               else:
                  iftlog.exception( self.name + ": exception while receiving whole file", inst )
               
               return self.__recv_cleanup( TRANSMIT_STATE_FAILURE )
            
            
         # did we only get one file (i.e. no chunking even from IFTD)?
         elif len(files_list) > 0:
            if recv_rc != 0:
               iftlog.log(5, self.name + ".__receive_file_data: WARNING! got rc=" + str(recv_rc) )
            
            if self.ift_job.get_attr( iftfile.JOB_ATTR_REMOTE_IFTD ) == False and self.get_chunking_mode() == PROTO_NO_CHUNKING:
               # got the file directly, so we can bypass the iftfile instance and move the file in place directly
               fname = files_list[0][1]
               try:
                  shutil.move( fname, self.iftfile_ref.path )
                  iftfile.apply_dir_permissions( self.iftfile_ref.path )
                  self.__recv_cleanup( TRANSMIT_STATE_SUCCESS )
                  return (PROTO_MSG_END, 0)
               except Exception, inst:
                  iftlog.exception(self.name + ".__receive_file_data: could not receive file " + str(fname))
                  return (PROTO_MSG_ERROR_FATAL, E_UNHANDLED_EXCEPTION)
                     
         
            else:
               # got one or more chunks, so add them to the file we're reconstructing
               for f in files_list:
                  try:
                     fname = self.ift_job.get_attr( iftfile.JOB_ATTR_DEST_CHUNK_DIR ) + "/" + str(f[0])
                     fd = open(fname, "r")
                     chunk = fd.read()
                     fd.close()
                     msg, rc = self.__store_chunk( chunk, f[0] )
                     if msg != 0 or rc != 0:
                        return (msg, rc)
                     
                        
                     # are we finished receiving, as indicated by the protocol?
                     if self.recv_finish:
                        return self.__recv_cleanup( self.recv_status )
                     
                     return (0,0)
               
                  except Exception, inst:
                     iftlog.exception( self.name + ".__receive_file_data: could not store chunk " + str(f))
                     return (PROTO_MSG_ERROR_FATAL, E_UNHANDLED_EXCEPTION)
                  
               
            
         else:
               
            # are we finished receiving, as indicated by the protocol?
            if self.recv_finish:
               return self.__recv_cleanup( self.recv_status )
   
            # no file data given
            if recv_rc != 0:
               iftlog.log(5, self.name + ".__receive_file_data: no data received, rc=" + str(recv_rc))
               return (PROTO_MSG_ERROR_FATAL, recv_rc)
            
            else:
               return (0, 0)     # just keep going...
   
      except Exception, inst:
         iftlog.exception(self.name + ".__receive_file_data(): exception while receiving " + str(self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) ), inst )
         return (PROTO_MSG_ERROR_FATAL, E_UNHANDLED_EXCEPTION)
   
      
   def __receive_data( self ):
      """
      Carry out the conversation with the sender to receive a file.
      Return (message, args) on success, or (PROTO_MSG_ERROR, error code) on error
      """

      if self.ift_job == None and (self.ready_to_receive == False or self.transmit_state != TRANSMIT_STATE_CHUNKS):
         return (0, E_BAD_STATE)     # nothing to do


      # can we do chunking as bytestrings?
      if self.get_chunking_mode() != PROTO_NO_CHUNKING:
         self.set_default_behavior( self.__receive_chunk_data )      # use bytestring chunk-specific method from now on
         return self.__receive_chunk_data()
         
      # otherwise, we'll be receiving files
      else:
         self.set_default_behavior( self.__receive_file_data )       # use file-specific method from now on
         return self.__receive_file_data()
   
   
   
   
   def __store_chunk( self, chunk, chunk_id ):
      """
      Store a chunk into the file
      """
      
      iftlog.log(1, self.name + ": chunk " + str(chunk_id) + ": received " + str(len(chunk)) + " bytes")
      
      if self.iftfile_ref == None:
         # the whole file was saved, so we should die
         return (PROTO_MSG_ERROR_FATAL, E_TERMINATED)
 
      
      # we have the chunk NOW, so overwride someone else's reservation if we have to
      rc = self.iftfile_ref.lock_chunk( self, chunk_id, True, 1.0 )
      if rc < 0:
         if rc == E_DUPLICATE:
            # someone beat us to it
            iftlog.log(5, self.name + ": will not write already-received chunk " + str(chunk_id))
            return (PROTO_MSG_NONE, rc)
         
         else:
            iftlog.log( 5, self.name + ": could not lock chunk " + str(chunk_id) + " for writing, rc=" + str(rc))
            return (PROTO_MSG_ERROR_FATAL, rc)
      
      # we have exclusive access to the chunk, so write the data
      rc = self.iftfile_ref.set_chunk( chunk, chunk_id, self.ift_job.get_attr( iftfile.JOB_ATTR_TRUNICATE ), self.ift_job.get_attr( iftfile.JOB_ATTR_STRICT_CHUNKSIZE ) )
      rc2 = self.iftfile_ref.unlock_chunk( self, chunk_id )
      
      if rc < 0:
         iftlog.log( 5, self.name + ": could not write chunk " + str(chunk_id) + ", rc=" + str(rc))
         
         # someone's spoofing us
         self.close_connection( TRANSMIT_STATE_FAILURE )
         return (PROTO_MSG_ERROR_FATAL, rc)
      
      if rc2 < 0:
         iftlog.log( 5, self.name + ": could not unlock chunk " + str(chunk_id) + ", rc = " + str(rc))
         return (PROTO_MSG_ERROR_FATAL, rc)
      
      iftlog.log(1, self.name + ": saved chunk " + str(chunk_id))
      
      return (0,0)
      
   
   
   
   
   def __recv_cleanup(self, default_status ):
      """
      Validate and close the file
      """
      if self.iftfile_ref != None:
         iftlog.log(3, self.name + ": done receiving chunks for " + str(self.iftfile_ref.path))
      else:
         iftlog.log(3, self.name + ": done receiving")
      
      if default_status == TRANSMIT_STATE_SUCCESS:
         iftlog.log(3, self.name + ": Successful exit in receiving " + str(self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_NAME )) + " to " + str(self.ift_job.get_attr(iftfile.JOB_ATTR_DEST_NAME)) )
         self.close_connection( TRANSMIT_STATE_SUCCESS )
         return (PROTO_MSG_END, None)
      
      else:
         iftlog.log(3, self.name + ": receiver indicates unsuccessful transmission")
         self.close_connection( default_status )
         return (PROTO_MSG_ERROR_FATAL, E_FAILURE)
      
      
      
   def add_chunk( self, chunk_id, chunk_str ):
      """
      Record that we have indeed received a chunk.
      """
      if self.__recv_chunks_dict != None:
         self.__recv_chunks_dict[ chunk_id ] = chunk_str
      
   
   def add_file( self, chunk_id, file_path ):
      """
      Record that we have indeed received a file.
      """
      if self.__recv_files_list != None:
         self.__recv_files_list.append( [chunk_id, file_path] )
      
   
   def whole_file( self, file_path ):
      """
      Sometimes, the receiver will get the entire file back at once.
      Call this method if so.
      """
      if self.__recv_chunks_dict != None:
         self.__recv_chunks_dict["whole_file"] = file_path
      
      if self.__recv_files_list != None:
         self.__recv_files_list = [[-1, file_path]] + self.__recv_files_list
      
      self.has_whole_file = True
      
   
   def __recv_chunks( self, remote_chunk_dir, desired_chunks ):
      """
      The "real" recv_chunks method, which will track the chunks received
      through the add_chunk method called in all subclasses.
      """
      self.__recv_chunks_dict = {}     # dictionary to store received data
      self.__recv_files_list = None
      rc = self.recv_chunks( remote_chunk_dir, desired_chunks )
      ret_chunks_dict = self.__recv_chunks_dict
      self.__recv_chunks_dict = None
      return (rc, ret_chunks_dict)
   
   
   def __recv_files( self, remote_file_paths, local_file_dir ):
      """
      The "real" recv_file method, which will track the file(s) received
      through the add_file method called in all subclasses.
      """
      self.__recv_chunks_dict = None
      self.__recv_files_list = []   # list of remote file paths
      rc = self.recv_files( remote_file_paths, local_file_dir )
      ret_recv_files = self.__recv_files_list
      self.__recv_files_list = None
      return (rc, ret_recv_files)
      
   
   def recv_chunks( self, remote_chunk_dir, desired_chunks ):
      """
      Receive one or more chunks.
      Used only if there is a remote IFTD or if the protocol can natively handle chunks.
      
      @arg remote_chunk_dir
         Location on the remote disk where the remote chunks can be found.
         
      @arg desired_chunks
         Array of chunk IDs that correspond to the chunks it should attempt to receive.
      
      @return
         0 on success, negative on failure
      """
      return E_NOT_IMPLEMENTED

   
   def recv_files( self, remote_file_paths, local_file_dir ):
      """
      Receive one or more files from a remote host.
      
      @arg remote_file_paths
         Array of paths of the files to receive.  In implementation, this should trump the
         job attribute JOB_ATTR_SRC_NAME field, since remote_file_path may refer
         to a chunk on disk to receive.
      
      @arg local_file_dir
         Path to a directory on localhost where the files should be stored to.  In implementation,
         this should trump the job attribute JOB_ATTR_DEST_NAME, since the local_file_dir
         may refer to the chunk directory and remote_file_paths may be chunks.
      
      @return
         0 on success, negative on failure
      """
      
      return E_NOT_IMPLEMENTED
   
   
   def recv_finished(self, status):
      """
      Indicate that we are done receiving
      """
      self.recv_finish = True
      self.recv_status = status
      
      
   """
   Protocol-specific cleanup
   """
   def proto_clean(self):
      return
   
   
   def clean(self):
      """
      transmitter cleanup
      """
      self.proto_clean()
      iftcore.ifttransmit.transmitter.clean(self)
      self.ift_job = None
      self.recv_finish = False
      self.has_whole_file = False
      self.recv_status = 0
      self.state = PROTO_STATE_DEAD
      if self.iftfile_ref != None:
         #print "releasing my iftfile"
         self.iftfile_ref.unreserve_all( self )
   

   def unreceived_chunk_ids(self):
      """
      Which chunks are unreceived?
      """
      if self.iftfile_ref == None:
         return None
      
      rc = self.iftfile_ref.get_unwritten_chunks()
      if rc < 0:
         iftlog.log(5, self.name + ": could not determine pending file pieces (rc = " + str(rc) + ")")
         return None
  
      if len(rc) > 0: 
         return [rc[0]]
      return None
   
   
   def __next_chunks(self):
      """
      What chunks do we want to receive next?
      """
      if self.ift_job.get_attr( iftfile.JOB_ATTR_REMOTE_IFTD ) or self.get_chunking_mode() != PROTO_NO_CHUNKING:
         unreceived = self.unreceived_chunk_ids()
         if unreceived == None:
            return []

         if self.get_chunking_mode() != PROTO_NONDETERMINISTIC_CHUNKING:
            # reserve these in advance
            urc = []
            for i in unreceived:
               rc = self.iftfile_ref.reserve_chunk( self, i, self.ift_job.get_attr( iftfile.JOB_ATTR_CHUNK_TIMEOUT ) )
               if rc == E_COMPLETE:
                  # we're done!
                  self.recv_finish( TRANSMIT_STATE_SUCCESS )
                  return []
                  
               elif rc != 0:
                  iftlog.log(1, self.name + ": WARNING: could not reserve chunk " + str(i) + " for writing in " + str(self.iftfile_ref))
               else:
                  iftlog.log(1, self.name + ": reserved chunk " + str(i) + " for writing in " + str(self.iftfile_ref))
                  urc.append( i )
   
            unreceived = urc
         
      else:
         unreceived = [-1]
      
      return unreceived


   def __next_files(self):
      """
      What files do we want to receive next?
      """
      unreceived = None
      
      if self.ift_job.get_attr( iftfile.JOB_ATTR_REMOTE_IFTD ) or self.get_chunking_mode() != PROTO_NO_CHUNKING:
         unreceived = self.unreceived_chunk_ids()
         if unreceived == None:
            return []   
      

         tld = self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_CHUNK_DIR )
         if self.get_chunking_mode() != PROTO_NONDETERMINISTIC_CHUNKING:
            # reserve these in advance
            urc = []
            for i in unreceived:
               rc = self.iftfile_ref.reserve_chunk( self, i, self.ift_job.get_attr( iftfile.JOB_ATTR_CHUNK_TIMEOUT ) )
               if rc == E_COMPLETE:
                  # we're done!
                  self.recv_finished( TRANSMIT_STATE_SUCCESS )
                  return []
               elif rc != 0:
                  iftlog.log(1, self.name + ": WARNING: could not reserve chunk " + str(i) + " for writing in " + str(self.iftfile_ref))
               else:
                  iftlog.log(1, self.name + ": reserved chunk " + str(i) + " for writing in " + str(self.iftfile_ref))
                  urc.append( [i, tld + "/" + str(i)] )
   
            unreceived = urc
         
      else:
         unreceived = [[-1, os.path.abspath( self.ift_job.get_attr( iftfile.JOB_ATTR_SRC_NAME ) )]]
      
      return unreceived
   
   
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
   
