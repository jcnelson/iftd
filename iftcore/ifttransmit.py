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

import iftlog
import iftfile
import iftutil
import iftloader
import iftstats

import iftcore
from iftcore.consts import *

"""
Basic class that encapsulates message passing capabilities for transmitting entities.
The run() method runs in a separate thread.
"""
class transmitter:
   
   def __init__(self):
      self.msg_queue = deque()
      self.check = 0
      self.message_table = {}
      self.state = PROTO_STATE_DEAD
      self.transmit_state = TRANSMIT_STATE_DEAD
      self.chunking_mode = PROTO_NO_CHUNKING
      self.active = False  # not active by default
      self.last_error = 0  # last error
      self.setup_attrs = None
      self.default_run = None
      self.name = "<unknown>"
      
   
   def get_transmit_state(self):
      return self.transmit_state
   
   def isactive(self):
      """
      Are we active?  Do we initiate the transmission?
      """
      return self.active
   
   
   def setactive(self, flag):
      """
      Set active.  Must be called by a subclass
      """
      
      self.active = flag
   
   def get_chunking_mode( self ):
      return self.chunking_mode
   
   def set_chunking_mode( self, mode ):
      """
      Set the chunking mode.  Possible modes are PROTO_DETERMINISTIC_CHUNKING, PROTO_NONDETERMINISTIC_CHUNKING, PROTO_NO_CHUNKING.
      """
      
      if mode == PROTO_DETERMINISTIC_CHUNKING or mode == PROTO_NONDETERMINISTIC_CHUNKING or mode == PROTO_NO_CHUNKING:
         self.chunking_mode = mode
   
   
   def set_handler( self, msg_type, msg_func ):
      """
      Map a message type to a handler
      
      @arg msg_type
         integer type
      
      @arg msg_func
         function that takes a dictionary as an argument
      """
      self.message_table[ msg_type ] = msg_func
      
   
   def set_default_behavior( self, default_func ):
      """
      Set the default behavior when not handling messages.
      The default_func should take no arguments and should return a 2-tuple
      with the first element being a message to be interpreted by the message handler
      and with the second element being a dictionary of arguments that go with the message.
      """
      self.default_run = default_func
   
   
    
   def validate_attrs(self, given_attrs, needed_args):
      """
      Determine whether or not the given attributes
      are valid and sufficient.
      
      @arg given_attrs
         Dictionary of setup attributes to be passed to ifttransmit.setup()
      
      @arg needed_args
         Dictionary of needed arguments
         
      @return
         0 if valid, nonzero if not
      """
      
      rc = 0
      if given_attrs != None and needed_args != None:
         for arg in needed_args:
            if given_attrs.has_key(arg) == False:
               iftlog.log(5, self.name + ".validate_attrs: argument " + arg + " not supplied")
               rc = E_NO_DATA
      
      elif needed_args != None:
         if iftfile.JOB_ATTR_OPTIONAL in needed_args:
            needed_args.remove( iftfile.JOB_ATTR_OPTIONAL )
         if PROTO_USE_DEPRICATED in needed_args:
            needed_args.remove( PROTO_USE_DEPRICATED )
         if len(needed_args) != 0:
            iftlog.log(5, self.name + ".validate_attrs: need " + str(needed_args) + ", but None given")
            rc = E_NO_DATA

      return rc
   
   
   
   
   def get_setup_attrs(self):
      """
      Get attributes that we need to know to set up
      """
      return []
   
   
   def get_connect_attrs(self):
      """
      Get attributes that we need to know about the connection
      """
      return []
   
   
   
   def get_all_attrs(self):
      """
      Get all attributes this protocol recognizes
      """
      return []
   
   
   def setup(self, setup_attrs ):
      """
      This method will be invoked to perform one-time initialization of the protocol.
      Anything that has to be done exactly once before file transfers
      can begin must occur here.
      
      @arg setup_attrs
         This is a protocol-specific dictionary of additional arguments
         specific to the setup.
         
      @return
         Returns 0 if initialization succeeded; returns negative on error
      """
      return 0
   
   def do_setup( self, setup_attrs ):
      """
      This is the "real" setup() method
      """
      self.setup_attrs = setup_attrs
      return self.setup( setup_attrs )

   
   def shutdown( self, shutdown_args=None ):
      """
      Perform total protocol shutdown
      
      @arg shutdown_args
         Optional dictionary of arguments specific to the shutdown procedure
         
      @return
         0 on success; negative on error
      """
      # terminate ourselves by default
      self.post_msg( PROTO_MSG_TERM, shutdown_args )
      self.setup_attrs = None
      return 0
   
   
   
   def on_start( self, args ):
      """
      Default behavior of what to do upon thread startup
      """
      self.state = PROTO_STATE_RUNNING
   
   
   def on_end( self, args ):
      """
      Default behavior of what to do upon ending
      """
      self.state = PROTO_STATE_DEAD
   
   
   def on_term( self, args ):
      """
      Default behavior for termination
      """
      self.state = PROTO_STATE_TERM
   
   def on_error( self, error_code ):
      iftlog.log(5, "Received error (code " + str(error_code) + ")")
   
   def on_fatal_error( self, error_code ):
      """
      Default behavior of what to do upon error
      """
      iftlog.log( 5, "Received fatal error (code " + str(error_code) + "), shutting down")
      self.shutdown( None )
   
   
   def clean( self ):
      """
      Clean up from a terminated transmission
      """
      self.msg_queue = deque()
      self.check = 0
      self.state = PROTO_STATE_DEAD
      self.transmit_state = TRANSMIT_STATE_DEAD
   
   
   def message_handler( self, msg_type, msg_params ):
      """
      Handle an event from the protocol (or iftd itself)
      
      @arg msg_type
         The type of message (integer)
         
      @arg msg_params
         Optional parameters associated with this message
         
      """
      try:
         if self.message_table[ msg_type ] != None:
            rc = self.message_table[ msg_type ]( msg_params )
            if is_error( rc ):
               self.on_error( rc )
         else:
            iftlog.log(3, "ifttransmit: unhandled message " + str(msg_type) + " ignored")
            
      except Exception, inst:
         iftlog.log(5, "ifttransmit: Caught unhandled exception: " + str(inst))
         if self.message_table.has_key( msg_type ):
            iftlog.log(5, "   Handler: " + str(self.message_table[msg_type]))
         else:
            iftlog.log(5, "   Handler: None")
         iftlog.log(5, "   Message ID:   " + str(msg_type))
         iftlog.log(5, "   Message args: " + str(msg_params))
         self.transmit_state = TRANSMIT_STATE_FAILURE
         self.on_error( E_UNHANDLED_EXCEPTION )
      
      return
   
   
   def post_msg( self, msg_type, msg_params ):
      """
      Post a message to this ifttransmit object.
      
      @arg msg_type
         An integer
      
      @arg msg_params
         A dictionary of optional parameters
         
      """
      item = (msg_type, msg_params)
      self.msg_queue.append( item )
   
   
   def run(self, timeslice ):
      """
      While running, get messages from the encapsulating iftproto instance and handle them.
      @arg timeslice
         How often to sleep between iterations
      @return
         last message received
      """
      if self.state == PROTO_STATE_DEAD:
         self.state = PROTO_STATE_SUSPENDED
      end_args = None
      shutdown_args = None
      last_msg = 0
      #print self.name + ": initial state = " + str(self.state)
      while self.state != PROTO_STATE_TERM:
         try:
            if self.default_run != None and self.state == PROTO_STATE_RUNNING:
               default_msg = None
               default_args = None
               
               try:
                  default_msg, default_args  = self.default_run()
               except Exception, inst:
                  iftlog.exception( self.name + ": Default behavior exception", inst)
                  self.transmit_state = TRANSMIT_STATE_FAILURE
                  break

               #print self.name + ": msg = " + str(default_msg) + ", args = " + str(default_args)  
             
               if default_msg == PROTO_MSG_TERM:
                  self.last_msg = PROTO_MSG_TERM
                  iftlog.log(3, self.name + ": Got PROTO_MSG_TERM, dying...")
                  break    # default run method says it's time to die
               
               elif default_msg == PROTO_MSG_END:
                  iftlog.log(3, self.name + ": Got PROTO_MSG_END, cleaning up...")
                  end_args = default_args
                  last_msg = PROTO_MSG_END
                  break
               
               elif default_msg == PROTO_MSG_ERROR_FATAL:
                  self.last_error = default_args
                  iftlog.log(3, self.name + ": Got irrecoverable error " + str(self.last_error) + ", dying...")
                  self.last_msg = PROTO_MSG_ERROR_FATAL
                  break
               
               elif default_msg == PROTO_MSG_ERROR:
                  self.last_msg = PROTO_MSG_ERROR
                  self.last_error = default_args
                  iftlog.log(3, self.name + ": Got recoverable error " + str(self.last_error))
               
               if default_msg != None and default_msg != PROTO_MSG_NONE:
                  self.message_handler( default_msg, default_args )
                  if timeslice > 0:
                     time.sleep( timeslice )
            
            msg = None
            try:
               msg = self.msg_queue.popleft()
              
            except Exception, inst:
               pass
            
            if msg != None:
               if msg[0] == PROTO_MSG_TERM:
                  iftlog.log(3, self.name + ": Got PROTO_MSG_TERM, dying...")
                  shutdown_args = msg[1]
                  last_msg = PROTO_MSG_TERM
                  break    # time to die
               elif msg[0] == PROTO_MSG_END:
                  iftlog.log(3, self.name + ": Got PROTO_MSG_END, cleaning up...")
                  last_msg = PROTO_MSG_END
                  break
               else:
                  self.message_handler( msg[0], msg[1] )

            if timeslice > 0:
               time.sleep( timeslice )

            
         except Exception, inst:
            iftlog.exception( self.name + ": Event handler exception", inst )
            self.transmit_state = TRANSMIT_STATE_FAILURE
            break
     
         continue

      try:
         self.state = PROTO_STATE_ENDED
         self.on_end( end_args )
      except Exception, inst:
         iftlog.exception( self.name + ": could not clean up after myself", inst)
      
      # shutdown if terminated
      if last_msg == PROTO_MSG_TERM:
         try:
            self.state = PROTO_STATE_DEAD
            self.on_term( shutdown_args )
         except Exception, inst:
            iftlog.exception( self.name + ": terminated but could not properly shutdown", inst)

      return last_msg

