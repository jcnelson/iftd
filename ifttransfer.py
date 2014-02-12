#!/usr/bin/python2.5

"""
ifttransfer.py
Copyright (c) 2010 Jude Nelson

This package provides the core data transmission code that
gets invoked by the iftapi package.
"""


import os
import sys
import getopt
import time
import copy
from collections import deque

import iftfile
import iftlog
import iftstats
import iftutil
from iftdata import *
import iftloader
import iftcore
from iftcore.consts import *

import threading
import thread

import protocols

import traceback


class ReceiverData:

   """
   Representative of an ongoing data receiving process
   """
   
   job = None
   protos = None
   file = None
   connect_timeout = None
   transfer_timeout = None
   start_time = None
   remote_iftd = False
   negotiated = False      # set to true once the content negotiation completes
   
   def __init__(self, user_job, proto_insts, iftfile_ref, remote_iftd, connect_timeout, transfer_timeout, start_time):
      self.job = user_job
      self.protos = proto_insts
      self.file = iftfile_ref
      self.connect_timeout = connect_timeout
      self.transfer_timeout = transfer_timeout
      self.start_time = start_time
      self.remote_iftd = remote_iftd
      self.negotiated = False
   
   
   def update(self, user_job, proto_insts, remote_iftd ):
      # merge in job attributes
      for key in user_job.attrs.keys():
         self.job.set_attr( key, user_job.get_attr(key) )
         
      # merge in protocols
      self.protos += proto_insts
      
      # we may have detected one....
      self.remote_iftd = remote_iftd
   
   
   def finish_negotiation(self):
      self.negotiated = True
      


class SenderData:
   
   """
   Representation of sender data during negotiation
   """   
   chunk_data = None
   chunk_timeout = None
   connect_attrs = None
   job_attrs = None
   
   def __init__(self, chunk_data, chunk_timeout, connect_attrs, job_attrs ):
      self.chunk_data = chunk_data
      self.chunk_timeout = chunk_timeout
      self.connect_attrs = connect_attrs
      self.job_attrs = job_attrs
      
      

class TRANSFER_CORE:

   """
   Manager of active transmissions
   """

   __active_transmission_lock = threading.BoundedSemaphore(1)
   __active_transmissions = {}      # map xmit_id to ReceiverData
   
   
   __active_sending_lock = threading.BoundedSemaphore(1)
   __active_senders = {}      # map xmit_id to SenderData
   
   __sender_ack_buff = deque()   # thread-safe buffer of received xmit IDs from the receiver, from which the sender expect acknowledgement
   __sender_ack_rc = {}          # map receiver xmit_ids to the receiver RCs as they arrive.



   def add_receiver_ack( self, xmit_id ):
      self.__sender_ack_buff.append( xmit_id )
      
   
   def remove_receiver_ack( self, xmit_id ):
      try:
         self.__sender_ack_buff.remove( xmit_id )
      except:
         pass
         
   
   def sender_valid_xmit_id( self, xmit_id ):
      """
      Given by a remote receiver, is this a valid xmit_id?
      """
      return self.__active_senders.has_key( xmit_id )
      
   
   def get_connection_attrs( self, xmit_id ):
      """
      Look up saved connection attributes (given to the sender from the receiver)
      """
      try:
         sd = self.__active_senders.get( xmit_id )
         if sd:
            return sd.connect_attrs
         
         return None
      except:
         return None
      
   
   
   def get_job_attrs( self, xmit_id ):
      """
      Look up saved job attributes (given to the sender from the receiver)
      """
      try:
         sd = self.__active_senders.get( xmit_id )
         if sd:
            return sd.job_attrs
         
         return None
      except:
         return None
         
         
   def await_receiver_ack( self, xmit_id, timeout ):
      """
      Wait for the receiver to acknowledge that all chunks have been transferred.
      """
      
      timeout = timeout + time.time()
      
      rc = 0
      # now wait until timeout seconds have passed until the receiver acknowledges us
      while xmit_id in self.__sender_ack_buff and time.time() < timeout:
         time.sleep(0.01)
         continue
      
      if time.time() > timeout:
         rc = E_TIMEOUT

      else:
         sender_rc = self.__sender_ack_rc.get( xmit_id )      
         if sender_rc != None:
            rc = sender_rc      
            del self.__sender_ack_rc[xmit_id]

      return rc
   
   
   def do_receiver_ack( self, xmit_id, rc ):
      """
      Remove the given xmit_id from the sender ack buff.  This should be called
      by a remote receiver so a local sender (waiting for an ack) knows the receiver's
      return code.
      """
      
      self.__sender_ack_rc[ xmit_id ] = rc
      
      try:
         self.__sender_ack_buff.remove( xmit_id )
      except:
         pass
      
      return 
      

   def begin_ift_send( self, xmit_id, user_job, chunk_data, chunk_timeout, connect_attrs ):
      """
      Begin to send data.  This method will buffer up the job, chunk data, chunk timeout, and xmit_id of
      a pending transfer, to which more data can be added later by the run_ift_send_* methods.
      """
      
      if self.__active_transmissions.get( xmit_id ) != None:
         return E_DUPLICATE      # should never happen
      
      # new record otherwise
      else:
         self.__active_sending_lock.acquire()
         # did someone make a new record while we were blocking?  If so, then just update
         if self.__active_senders.has_key( xmit_id ):
            self.__active_sending_lock.release()
            return E_DUPLICATE
          
         # otherwise, new!
         else:
            # begin to gather stats
            iftstats.begin_transfer( user_job, sender=True )
            self.__active_senders[ xmit_id ] = SenderData( chunk_data, chunk_timeout, connect_attrs, user_job.attrs )
            
         self.__active_sending_lock.release()
         
      return 0
      
      
      
   def run_ift_send_passive( self, xmit_id, user_job, connected_protos, timeout=0.0):
      """
      Begin to send data, provided that there is an iftd instance
      on both the sender and receiver.
      
      This only uses passive protocols, which do not move data but rather make it available.
         
      @arg xmit_id
         Key to SenderData instance with chunk data
         
      @arg user_job
         an iftjob instance
      
      @arg connected_protos
         list of connected protocol instances to use, put in order by most preferable first.
         
      @arg timeout
         ???
      
      """
      
      print "run_ift_send_passive: connected: " + str(connected_protos) 
      
      if timeout == None:
         timeout = 0.0
         
      max_attempts = 3
      if user_job.get_attr( iftfile.JOB_ATTR_MAX_ATTEMPTS ) != None:
         max_attempts = user_job.get_attr( iftfile.JOB_ATTR_MAX_ATTEMPTS )
      
      if len(connected_protos) == 0:
         iftlog.log( 5, "run_ift_send_passive: no protocols started up!")
         return E_NO_CONNECT
      
      sender_data = self.__active_senders[ xmit_id ]
      if sender_data == None:
         return E_NO_DATA
      
      # get the chunks
      chunk_data = sender_data.chunk_data
      
      curr_proto = 0    # start with the best protocol
      cycles = 0        # how many times have we looped through the protocols?
      
      # start sending via passive protocols
      for (chunk, chunk_id, local_chunk_path, remote_chunk_path) in chunk_data:
         #print "send (" + str(chunk_id) + ", " + str(chunk) + ") from " + str(local_chunk_path) + " to " + str(remote_chunk_path)
         try:
            # all passive protocols send
            for p in connected_protos:
               if not p.isactive():
                  rc = p.send_one_chunk( chunk, chunk_id, local_chunk_path, remote_chunk_path )
                  if rc < 0:
                     iftlog.log(5, "run_ift_send_passive: sending chunk with passive protocol " + str(p.name) + " failed with rc=" + str(rc))
                     continue
               
            
         except Exception, inst:
            iftlog.exception( "run_ift_send_passive: could not send chunk with protocol " + str(curr_proto) + " of " + str(len(connected_protos)) + " (" + str([p.name for p in connected_protos]) + ")", inst)
            try:
               connected_protos[curr_proto].clean()
            except:
               pass
            
            connected_protos.remove( connected_protos[curr_proto] )
            continue
         
      
      # all chunks sent!
      for proto in connected_protos:
         proto.clean()
      
      rc = TRANSMIT_STATE_SUCCESS
      return rc
      
   
   
   def run_ift_send_active( self, xmit_id, user_job, connected_protos, timeout=0.0, has_best_proto = False):
      """
      Begin to send data, provided that there is an iftd instance
      on both the sender and receiver.
      
      This only runs active senders, which actually move the data.  Consequently, the
      receiver must acknowledge the sender.  This method will wait until timeout seconds
      have passed, or the acknowledgement has been received.
         
      @arg user_job
         an iftjob instance
      
      @arg chunk_data
         list of (chunk, chunk id, local chunk path, remote chunk path), some of which may be null
      
      @arg connected_protos
         list of connected protocol instances to use, put in order by most preferable first.
         
      @arg timeout
         how long we should wait for receiver acknowledgement
      
      """
      
      print "run_ift_send_active: connected: " + str(connected_protos) 
      if timeout == None:
         timeout = 0.0
         
      max_attempts = 3
      if user_job.get_attr( iftfile.JOB_ATTR_MAX_ATTEMPTS ) != None:
         max_attempts = user_job.get_attr( iftfile.JOB_ATTR_MAX_ATTEMPTS )
      
      if len(connected_protos) == 0:
         iftlog.log( 5, "run_ift_send_active: no protocols started up!")
         return E_NO_CONNECT
      
      
      
      sender_data = self.__active_senders[ xmit_id ]
      if sender_data == None:
         return E_NO_DATA
      
      # get the chunks
      chunk_data = sender_data.chunk_data
      
      curr_proto = 0    # start with the best protocol
      cycles = 0        # how many times have we looped through the protocols? (only if we have a best protocol)
      fails = 0         # how many failures have we had? (only used if we don't have a best protocol)
      max_rc = 0
      
      # start sending.  start up protocols as needed.
      # TODO: round-robin between protocols if there is no favorite
      print len(chunk_data)
      for (chunk, chunk_id, local_chunk_path, remote_chunk_path) in chunk_data:
         #print "send (" + str(chunk_id) + ", " + str(chunk) + ") from " + str(local_chunk_path) + " to " + str(remote_chunk_path)
         if len(connected_protos) == 0:
            break
            
         try:
            
            # if there is no "best" proto, then just cycle through them all
            if not has_best_proto:
               curr_proto = (curr_proto + 1) % len(connected_protos)
            
            rc = connected_protos[curr_proto].send_one_chunk( chunk, chunk_id, local_chunk_path, remote_chunk_path )
            
            if rc < 0:
               max_rc = abs(rc)
               old_proto_name = connected_protos[curr_proto].name
               
               if has_best_proto:
                  # only change protocols if we have a best protocol (otherwise we're already cycling through them)
                  curr_proto = (curr_proto + 1) % len(connected_protos)
               
                  iftlog.log(5, "run_ift_send_active: sending chunk with " + old_proto_name + " failed with rc=" + str(rc) + ", switching to " + connected_protos[curr_proto].name )
               
                  if curr_proto == 0:
                     cycles += 1
                  
                  if cycles >= max_attempts:
                     # stop after three full rounds
                     iftlog.log(5, "run_ift_send_active: attempted and failed with all available protocols " + str(max_attempts) + " times already; it must be impossible to send")
                     max_rc = TRANSMIT_STATE_FAILURE
                     for proto in connected_protos:
                        proto.clean()
                     
                     break
                  
               else:
                  # no best protocol, so we're failed if we have a negative RC occurrence more than 3x the number of chunks
                  fails += 1
                  if fails > max_attempts * len(chunk_data):
                     iftlog.log(5, "run_ift_send_active: attempted and failed with all available protocols " + str(max_attempts) + " each; it must be impossible to send")
                     max_rc = TRANSMIT_STATE_FAILURE
                     for proto in connected_protos:
                        proto.clean()
                     
                     break
            
         except Exception, inst:
            iftlog.exception( "run_ift_send_active: could not send chunk with protocol " + str(curr_proto) + " of " + str(len(connected_protos)) + " (" + str([p.name for p in connected_protos]) + ")", inst)
            connected_protos[curr_proto].clean()
            print "removing..."
            connected_protos.remove( connected_protos[curr_proto] )
            continue
      
      # all chunks sent!
      for proto in connected_protos:
         proto.clean()
      
      # get receiver acknowledgement
      receiver_rc = self.await_receiver_ack( xmit_id, timeout )
      iftlog.log(1, "run_ift_send_active: ACK is " + str(max_rc))
      
      return (max_rc, receiver_rc)



   def finish_negotiation( self, xmit_id ):
      """
      Call this once the content negotiation completes
      """
      xmit = self.__active_transmissions.get(xmit_id)
      if xmit != None:
         xmit.finish_negotiation()


   def begin_ift_recv( self, xmit_id, user_job, proto_insts, remote_iftd, niceness=-1, connect_timeout=1.0, transfer_timeout=3600.0, threaded=True ):
      """
      Start running some protocols and store the data assocated with the active transmission.
      
      @arg xmit_id
         Identifier for this transmission
         
      @arg user_job
         Job describing the transfer

      @arg proto_insts
         Initialized protocol instances that are ready to run

      @arg remote_iftd
         True/False value if there is/isn't a remote IFTD

      @arg niceness
         Delay value between calls of the protocol's default behavior function.  -1 means no delay.

      @arg connect_timeout
         Maximum amount of time a protocol is allowed to take to connect to the remote host

      @arg transfer_timeout
         Maximum amount of time this transfer is allowed to take
      """
       
      if not transfer_timeout:
         transfer_timeout = 3600.0

      if len(proto_insts) == 0:
         rc = E_NO_CONNECT
         return rc

      last_recv = {}    # proto_name ==> (last recorded data sent, time of last data transmission)
      for proto in proto_insts:
         last_recv[proto.name] = (-1, -1)

      # we will do stats gathering
      iftstats.begin_transfer( user_job, receiver=True )

      # wait until one protocol registers successful transmission
      rc = -1

      #myself = hash(sets.ImmutableSet(user_job.attrs))      # unique owner ID
      #myself = self
      #iftfile_ref = iftfile.acquire_iftfile_recv( myself, user_job.get_attr( iftfile.JOB_ATTR_DEST_NAME ), user_job.attrs )
      #if iftfile_ref == None:
      #   iftlog.log("run_ift_recv: could not initialize chunk reservation system for " + str(user_job.get_attr( iftfile.JOB_ATTR_DEST_NAME )))
      #   rc = NO_CONNECT
      #   return rc

      start_time = time.time()
       
      for proto in proto_insts:
         # start threads to receive concurrently
         proto.post_msg( PROTO_MSG_USER, PROTO_STATE_RUNNING )       # switch to running state
         #proto.run(niceness)
         thread.start_new_thread( proto.run, (niceness,) )
      
      
      rc = 0
      
      # and we're running!
      # are we updating an existing record?
      if self.__active_transmissions.get( xmit_id ) != None:
         # update!
         self.__active_transmission_lock.acquire()
         self.__active_transmissions[ xmit_id ].update( user_job, proto_insts, remote_iftd )
         self.__active_transmission_lock.release()
      
      # new record otherwise
      else:
         new = False
         self.__active_transmission_lock.acquire()
         # did someone make a new record while we were blocking?  If so, then just update
         xmit = self.__active_transmissions.get( xmit_id )
         if xmit != None:
            self.__active_transmissions[ xmit_id ].update( user_job, proto_insts, remote_iftd )
         
         # otherwise, new!
         else:
            # begin to gather stats
            iftstats.begin_transfer( user_job, receiver=True )
            new = True
            self.__active_transmissions[ xmit_id ] = ReceiverData( user_job, proto_insts, user_job.get_attr( iftfile.JOB_ATTR_IFTFILE ), remote_iftd, connect_timeout, transfer_timeout, time.time() )
            
         self.__active_transmission_lock.release()
         
      return rc


   def run_ift_recv(self, xmit_id, sender_xmlrpc ):
      """
      Receive data with the given connected protocols, with the given job and connection attributes.

      @arg xmit_id
         Identifier for this transmission
         
      @arg sender_xmlrpc
         xmlrpclib.Server instance that can talk to the sender
         
      @return
         0 on success, negative on error
      """
 
      active_xmit = self.__active_transmissions.get( xmit_id )
      if active_xmit == None:
         try:
            sender_xmlrpc.ack_sender( xmit_id, E_NO_DATA )
         except Exception, inst:
            iftlog.exception("run_ift_recv: WARNING: failed to ACK " + str(xmit_id), inst)
            pass
         
         iftlog.log(5, "run_ift_recv: ERROR: no active transmission matching " + str(xmit_id))
         return TRANSMIT_STATE_FAILURE    # we're done
      
      transfer_rc = 0
      transfer_timeout = active_xmit.transfer_timeout
      start_time = active_xmit.start_time
      negotiated = False
      remote_iftd = active_xmit.remote_iftd
      
      last_bw_check = time.time()
      
      while time.time() - start_time < transfer_timeout and transfer_rc == 0:
         active_count = 0
         data_xmit = 0

         # look up our transmission details         
         active_xmit = self.__active_transmissions.get( xmit_id )
         if active_xmit == None:
            try:
               sender_xmlrpc.ack_sender( xmit_id, E_NO_DATA )
            except Exception, inst:
               iftlog.exception("run_ift_recv: WARNING: failed to ACK " + str(xmit_id), inst)
               pass
            
            iftlog.log(5, "run_ift_recv: ERROR: no active transmission matching " + str(xmit_id))
            return TRANSMIT_STATE_FAILURE    # we're done
         
         transfer_rc = 0
         transfer_timeout = active_xmit.transfer_timeout
         proto_insts = active_xmit.protos
         user_job = active_xmit.job
         remote_iftd = active_xmit.remote_iftd
         connect_timeout = active_xmit.connect_timeout
         iftfile_ref = active_xmit.file
         negotiated = active_xmit.negotiated

         for proto in proto_insts:
            
            if proto.get_transmit_state() == TRANSMIT_STATE_SUCCESS:
               # someone succeeded
               transfer_rc = TRANSMIT_STATE_SUCCESS
               active_count += 1    # don't bail if no one is active
               break
            
            elif proto.get_transmit_state() not in (TRANSMIT_STATE_SUCCESS, TRANSMIT_STATE_FAILURE) and proto.get_transmit_state() != TRANSMIT_STATE_DEAD:
               # still running
               active_count += 1
               data_cnt, time_cnt = iftstats.proto_performance( user_job, proto.name )
               
               if time.time() - last_bw_check > connect_timeout and data_cnt:
                  
                  last_bw_check = time.time()
                  data_xmit += data_cnt
                  
                  # is there a bandwidth lower threshold for this protocol?
                  bandwidth_threshold = None
                  if proto.get_connection_attrs() != None and proto.get_connection_attrs().get( iftfile.JOB_ATTR_MIN_BANDWIDTH ) != None:
                     bandwidth_threshold = proto.get_connection_attrs().get( iftfile.JOB_ATTR_MIN_BANDWIDTH )
                  
                  if bandwidth_threshold == None and user_job.get_attr( iftfile.JOB_ATTR_MIN_BANDWIDTH ) != None:
                     bandwidth_threshold = user_job.get_attr( iftfile.JOB_ATTR_MIN_BANDWIDTH )
                  
                  if bandwidth_threshold:
                     
                     # make sure to account for the case where we're waiting for the protocol to receive something (e.g. we execute this loop multiple times between receives, so account for the time)
                     if last_recv[proto.name][0] != -1:
                        last_recv[proto.name][0] = data_xmit      # data sent since last check
                        last_recv[proto.name][1] = time.time()    # time of last transmission
                     
                     else:
                        if last_recv[proto.name][0] != data_xmit:
                           # new data has been sent
                           last_recv[proto.name][0] = data_xmit
                           last_recv[proto.name][1] = time.time()    # record (approximate) time of last transmission
                        
                        else:
                           time_cnt += time.time() - last_recv[proto.name][1]      # no new data sent, so increase receive time
                        
                     
                     # what's its bandwidth?
                     if (float(data_cnt) / time_cnt) < bandwidth_threshold:
                        # this protocol is too slow; kill it
                        iftlog.log(5, "run_ift_recv: ERROR: protocol " + proto.name + " receives at " + str(bandwidth) + " bytes/sec, less than given threshold " + str(bandwidth_threshold) + " bytes/sec")
                        proto.post_msg( PROTO_MSG_END, None )
                  
               
            
         
         if user_job.get_attr( iftfile.JOB_ATTR_FILE_MAX_SIZE ) != None and user_job.get_attr( iftfile.JOB_ATTR_FILE_MAX_SIZE ) < data_xmit:
            # we've received too much data, so we've failed
            transfer_rc = TRANSMIT_STATE_FAILURE
            iftlog.log(5, "run_ift_recv: ERROR: received " + str(data_xmit) + " bytes, but expected no more than " + str(user_job.get_attr( iftfile.JOB_ATTR_FILE_MAX_SIZE )) + " bytes")
            break
         
         
         if iftfile_ref.is_complete():
            # got everything, so we can break
            transfer_rc = TRANSMIT_STATE_SUCCESS
            break
            
         if active_count == 0 and transfer_rc == 0 and negotiated:
            transfer_rc = TRANSMIT_STATE_FAILURE    # out of options
            iftlog.log(5, "run_ift_recv: ERROR: no active protocols remaining and none have received data")
            break
         
            
         # yield
         time.sleep(0.000001)

      if time.time() - start_time >= transfer_timeout:
         # transfer took too long
         iftlog.log(5, "run_ift_recv: ERROR: transfer has taken longer than " + str(transfer_timeout) + " seconds." )
         transfer_rc = TRANSMIT_STATE_FAILURE
         

      
      # signal all protocols to end transmission
      for proto in active_xmit.protos:
         proto.post_msg( PROTO_MSG_END, None )

      if not iftfile_ref.is_complete() and iftfile_ref.known_size:
         transfer_rc = TRANSMIT_STATE_FAILURE
         iftlog.log(5, "run_ift_recv: did not completely download " + str(iftfile_ref.path))
         iftlog.log(1, "run_ift_recv: still missing " + str(iftfile_ref.get_unwritten_chunks()))

      elif transfer_rc != TRANSMIT_STATE_FAILURE:
         my_hash = user_job.get_attr( iftfile.JOB_ATTR_FILE_HASH )
         if my_hash != None and my_hash != iftfile.JOB_ATTR_OPTIONAL:
            file_hash = iftfile_ref.calc_hash()
            if my_hash == file_hash:
               iftlog.log(5, "run_ift_recv: file hash is correct")
               iftfile.apply_dir_permissions( iftfile_ref.path )
               transfer_rc = TRANSMIT_STATE_SUCCESS
            else:
               iftlog.log(5, "run_ift_recv: expected file hash " + str(my_hash) + ", but got " + str(file_hash))
               transfer_rc = TRANSMIT_STATE_FAILURE

      
      
      # acknowledge the sender with our status
      try:
         if remote_iftd:
            sender_xmlrpc.ack_sender( xmit_id, transfer_rc )
            
      except Exception, inst:
         iftlog.exception("run_ift_recv: failed to ACK " + str(xmit_id), inst)
         pass
      
      # invalidate this reference 
      self.cleanup_recv( xmit_id )
      
      if transfer_rc == TRANSMIT_STATE_SUCCESS:
         iftstats.end_transfer( active_xmit.job, True )
      else:
         iftstats.end_transfer( active_xmit.job, False )

      iftlog.log(5, "run_ift_recv: transmission activity has finished")
      
      return transfer_rc   


   def cleanup_recv( self, xmit_id ):
      """
      Erase data for a receiver transmission
      """
      self.__active_transmission_lock.acquire()
      self.__active_transmissions[xmit_id] = None
      self.__active_transmission_lock.release()
      




TransferCore = TRANSFER_CORE()


