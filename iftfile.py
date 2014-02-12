#!/usr/bin/env python

"""
iftfile.py
Copyright (c) 2009 Jude Nelson

This package defines file attributes, properties, and categories
"""

import iftlog

from iftdata import *

import iftutil

import math
import os
import hashlib
import threading
import thread
import time
import stat

from Queue import Queue

"""
Names of job attributes.  These names are known to protocols, IFTD, applications, and the user.
"""
JOB_ATTR_FILE_TYPE       = "JOB_ATTR_FILE_TYPE"    # MIME type of file 
JOB_ATTR_FILE_SIZE       = "JOB_ATTR_FILE_SIZE"    # total size of the file (in bytes; if given, the next two attrs are ignored)
JOB_ATTR_FILE_MIN_SIZE   = "JOB_ATTR_FILE_MIN_SIZE"   # minimum allowable file size
JOB_ATTR_FILE_MAX_SIZE   = "JOB_ATTR_FILE_MAX_SIZE"   # maximum allowable file size
JOB_ATTR_CHUNKSIZE       = "JOB_ATTR_CHUNKSIZE"    # chunk size to use for transmission
JOB_ATTR_NUM_CHUNKS      = "JOB_ATTR_NUM_CHUNKS"   # how many chunks are there?
JOB_ATTR_FILE_HASH       = "JOB_ATTR_FILE_HASH"    # SHA-1 hash of the file
JOB_ATTR_SRC_NAME        = "JOB_ATTR_SRC_NAME"     # name of the file on the sender
JOB_ATTR_DEST_NAME       = "JOB_ATTR_DEST_NAME"    # name of the file on the receiver
JOB_ATTR_SRC_HOST        = "JOB_ATTR_SRC_HOST"     # name of the sender host
JOB_ATTR_DEST_HOST       = "JOB_ATTR_DEST_HOST"    # name of the receiver host
JOB_ATTR_PROTOS          = "JOB_ATTR_PROTOS"       # partial ordering of protocols to try, given as a list of strings from list_protocols()
JOB_ATTR_IFTFILE         = "JOB_ATTR_IFTFILE"      # reference to the iftfile that can be used to access the file described.  INTERNAL USE ONLY.
JOB_ATTR_TRUNICATE       = "JOB_ATTR_TRUNICATE"    # require that chunks written do not exceed the transmission chunk size
JOB_ATTR_STRICT_CHUNKSIZE= "JOB_ATTR_STRICT_CHUNKSIZE"  # require that chunks written be exactly the given size
JOB_ATTR_GIVEN_CHUNKS    = "JOB_ATTR_GIVEN_CHUNKS" # if True, then the protocol will be given the chunks on the fly. (INTERNAL USE ONLY by senders)
                                                   # If set, then there should be a Queue instance in the job metadata, mapped by this attribute key.
                                                   # Each queued element should be (chunk data, chunk id, path to the chunk), with None in place of any of those if they are not known
JOB_ATTR_CHUNK_HASHES    = "JOB_ATTR_CHUNK_HASHES" # If given, this is an in-order list of all chunk hashes (INTERNAL USE ONLY by receivers)
JOB_ATTR_TRANSFER_TIMEOUT= "JOB_ATTR_TRANSFER_TIMEOUT"      # if not null, this is the maximum amount of time that can be spent transferring this file
JOB_ATTR_SRC_CHUNK_DIR   = "JOB_ATTR_SRC_CHUNK_DIR"         # if two IFTD instances are communicating, this is the chunk directory on the source host (INTERNAL USE ONLY)
JOB_ATTR_DEST_CHUNK_DIR  = "JOB_ATTR_DEST_CHUNK_DIR"        # if two IFTD instances are communicating, this is the chunk directory on the destination host (INTERNAL USE ONLY)

JOB_ATTR_DO_CHUNKING     = "JOB_ATTR_DO_CHUNKING"           # True by default; if false, then the file is symlinked into the src chunk dir as a single chunk (chunk 0)
JOB_ATTR_MIN_BANDWIDTH   = "JOB_ATTR_MIN_BANDWIDTH"         # (connection attribute) minimum transmission bandwidth
JOB_ATTR_MAX_ATTEMPTS    = "JOB_ATTR_MAX_ATTEMPTS"          # maximum number of times to cycle through the protocols in the event of a failure
JOB_ATTR_CHUNK_TIMEOUT   = "JOB_ATTR_CHUNK_TIMEOUT"         # how long do we expect a chunk to take to be downloaded?

JOB_ATTR_REMOTE_IFTD    = "JOB_ATTR_REMOTE_IFTD"   # if true, there is known to be a remote iftd present

"""
Sentinel value to indicate that the field is optional
"""
JOB_ATTR_OPTIONAL       = "JOB_ATTR_OPTIONAL"

"""
Default values for some file attributes
"""
DEFAULT_FILE_CHUNKSIZE  = 65536
DEFAULT_CACHE_FRESHNESS = 60*60*24     # one day


class iftjob:
   """
   File transfer job description.
   This class describes a transmission job to iftd and provides
   methods for reading and writing the file data itself.
   
   This class provides the means for reading and writing chunks to
   and from the source and destination files in the job.
   """

   attrs = {}  # for invariable transfer
   stats = {}  # for statistical information
   meta = {}   # for job metadata
   
   
   def __init__(self, file_attrs):
      """
      Creates an iftfile from a dictionary of attributes (keyed with the above),
      and sets file status.
      
      @arg file_attrs
         Dictionary of file attributes with keys
      
      """
      
      self.attrs = file_attrs
      
      # set some default attributes
      if self.get_attr( JOB_ATTR_FILE_AGE ) == None:
         self.set_attr( JOB_ATTR_FILE_AGE, (-1, -1) )
      
      if self.get_attr( JOB_ATTR_FILE_OWNERS ) == None:
         self.set_attr( JOB_ATTR_FILE_OWNERS, [None] )
      
      #if self.get_attr( JOB_ATTR_FILE_TYPE ) == None:
      #   self.set_attr( JOB_ATTR_FILE_TYPE, "application/octet-stream" )
      
      if self.get_attr( JOB_ATTR_TRUNICATE ) == None:
         self.set_attr( JOB_ATTR_TRUNICATE, True )
         
      if self.get_attr( JOB_ATTR_STRICT_CHUNKSIZE ) == None:
         self.set_attr( JOB_ATTR_STRICT_CHUNKSIZE, False )
         
      if self.get_attr( JOB_ATTR_CHUNKSIZE ) == None:
         self.set_attr( JOB_ATTR_CHUNKSIZE, DEFAULT_FILE_CHUNKSIZE )
      
      if self.get_attr( JOB_ATTR_FILE_MIN_SIZE ) == None:
         self.set_attr( JOB_ATTR_FILE_MIN_SIZE, 0 )
      
      if self.get_attr( JOB_ATTR_FILE_MAX_SIZE ) == None:
         self.set_attr( JOB_ATTR_FILE_MAX_SIZE, self.get_attr( JOB_ATTR_FILE_SIZE ) )
      
      if self.get_attr( JOB_ATTR_TRANSFER_TIMEOUT ) == None:
         self.set_attr( JOB_ATTR_TRANSFER_TIMEOUT, 3600 )      # wait at most 1 hour for a data transfer
      
      if self.get_attr( JOB_ATTR_DO_CHUNKING ) == None:
         self.set_attr( JOB_ATTR_DO_CHUNKING, True )
      
      if self.get_attr( JOB_ATTR_REMOTE_IFTD ) == None:
         self.set_attr( JOB_ATTR_REMOTE_IFTD, True )
      
      if self.get_attr( JOB_ATTR_CHUNK_TIMEOUT ) == None:
         self.set_attr( JOB_ATTR_CHUNK_TIMEOUT, 1.0 )
       
      if self.get_attr( JOB_ATTR_DEST_CHUNK_DIR ) == None:
         self.set_attr( JOB_ATTR_DEST_CHUNK_DIR, get_chunks_dir( self.attrs.get( JOB_ATTR_SRC_NAME ), self.attrs.get( JOB_ATTR_FILE_HASH ) ) )
         
   
   def get_attr( self, attr ):
      """
      Get the names of the files this job describes
      """
      return self.attrs.get( attr )

   @staticmethod
   def get_attrs_copy( attrs ):
      """
      Get attributes that are safe to copy (e.g. no thread locks)
      """
      ret = {}
      for k in attrs.keys():
         try:
            ret[k] = copy.deepcopy(attrs[k])
         except:
            pass
         
      return ret
      
   
   def get_meta( self, attr ):
      """
      Get a meta-attribute.
      """
      return self.meta.get( attr )
   
   
   def defined( self, key_list ):
      """
      Given a list of keys, determine whether or not this iftfd has defined
      values for them all.
      """
      
      for key in key_list:
         if self.attrs.has_key( key ) == False:
            return False
      
      return True
   
   def supply_attr( self, attr, value ):
      """
      Set an attribute if there currently is none.
      """
      if self.attrs.get(attr) == None:
         self.attrs[attr] = value
   
   def set_attr( self, attr, value ):
      """
      Set an attribute
      """
      
      self.attrs[ attr ] = value

   def set_meta( self, attr, value ):
      """
      Set metadatum
      """
      self.meta[ attr ] = value

   def get_stat( self, attr ):
      """
      Get a stat
      """
      
      if self.stats.has_key( attr ) == False:
         return None
      
      return self.stats[ attr ]
   
   
   def set_stat( self, attr, value ):
      """
      Log a statistic
      """
      
      self.stats[ attr ] = value
      
      

# I/O modes (exclusive!)
MODE_READ = 1
MODE_WRITE = 2
   
class iftfile:
   """
   A wrapper around Python's File class that allows iftd to 
   read and write uniquely-identified chunks from and to the file.
   
   Due to the chunked nature of files, a file can either be opened
   in READ mode or WRITE mode, but not both.
   """
   
   __num_chunks = 0        # how many chunks are there?
   __chunks_size = 0       # how big is a chunk supposed to be? (used for reading)
   __chunk_mask = []       # False for unwritten, True for written--bit array of chunks
   __chunk_locks = []      # array of Semaphores for accessing a particular chunk
   __chunk_reservations = []  # array of times in the future when a chunk will become available
   __chunk_owners = []     # array of references to protocols reserving chunks   

   __read_lock = None      # lock to acquire to read the next chunk (used only in READ mode)
   __next_chunk = 0        # ID of chunk that was just read
   
   __mode = 0              # what mode we're open in (see above)
   __error = 0             # last error we encountered
   path = None             # path to file
   
   __bytes_written = 0     # total number of bytes written
   __bytes_max = -1        # maximum number of bytes allowed (optional)
   __open = False
  
   __hash = None           # hash to verify 
   __expand_lock = threading.BoundedSemaphore(1)   # lock to allow mutual exclusion in expanding the number of mutices when the file size is unknown
   
   # do we know the size?
   known_size = True
   
   # is the file actually complete?
   marked_complete = False
   
   def __init__( self, file_path ):
      self.path = file_path
      self.__read_lock = threading.BoundedSemaphore(1)
      self.__next_chunk = 0
      self.__mode = 0
   
   
   def last_error( self ):
      """
      Get the last error that occurred
      """
      return self.__error
   
   
   
   def fopen(self, file_attrs, mode ):
      """
      "Open" the file given in the constructor.  That is, set up the file metadata so we can write to different pieces of it concurrently.
      Return 0 on success; nonzero on error
      """
      if self.__open:
         return E_ALREADY_OPEN
      
      self.marked_complete = False
      
      # fill in a chunk size if none was given
      self.__chunk_size = DEFAULT_FILE_CHUNKSIZE
      
      if file_attrs.has_key(JOB_ATTR_CHUNKSIZE) == True:
         self.__chunk_size = file_attrs[JOB_ATTR_CHUNKSIZE]
      
      if file_attrs.has_key(JOB_ATTR_FILE_MAX_SIZE) == True:
         self.__bytes_max = file_attrs[JOB_ATTR_FILE_MAX_SIZE]
      else:
         self.__bytes_max = None
         
      if file_attrs.has_key(JOB_ATTR_FILE_HASH) == True:
         self.__hash = file_attrs[JOB_ATTR_FILE_HASH]

      self.known_size = True
      if (file_attrs.has_key(JOB_ATTR_FILE_SIZE) == False or file_attrs[JOB_ATTR_FILE_SIZE] == JOB_ATTR_OPTIONAL) and mode == MODE_WRITE:
         self.known_size = False
     
      elif file_attrs.get(JOB_ATTR_FILE_SIZE) != None and file_attrs.get(JOB_ATTR_FILE_SIZE) < 0:
         self.known_size = False

      elif file_attrs.get(JOB_ATTR_FILE_SIZE) == None:
         self.known_size = False

      if self.known_size and self.__bytes_max == None:
         self.__bytes_max = file_attrs.get( JOB_ATTR_FILE_SIZE )
         
      self.__mode = mode
      self.__chunk_id = 0
      
      try:
         if mode == MODE_READ:
            if not os.path.exists( self.path ):
               return E_FILE_NOT_FOUND
         
            iftlog.log(1, "iftfile: opened " + self.path + " for READING")
            self.__open = True
            return 0
         
         elif mode == MODE_WRITE:
            if os.path.exists( self.path ):
               # problem--will overwrite
               iftlog.log(1, "iftfile: WARNING: will overwrite " + self.path)
               try:
                  os.remove( self.path )
               except:
                  iftlog.exception("iftfile: could not remove " + self.path)
                  return E_DUPLICATE
            
            # prepare to receive chunks
            if self.known_size == True:
               # we know how big the file is
               self.__num_chunks = int( math.ceil( float( file_attrs[JOB_ATTR_FILE_SIZE] ) / float(self.__chunk_size) ) )
               self.__chunk_mask = [False] * self.__num_chunks
               self.__chunk_locks = [threading.BoundedSemaphore(1)] * self.__num_chunks
               self.__chunk_reservations = [0] * self.__num_chunks
               self.__chunk_owners = [None] * self.__num_chunks
               
            else:
               self.__chunk_mask = []
               # we don't know how big the file will be!
               self.__num_chunks = -1     # sentinel
               if file_attrs.has_key( JOB_ATTR_NUM_CHUNKS ):
                  self.__num_chunks = file_attrs[ JOB_ATTR_NUM_CHUNKS ]
                  self.__chunk_mask = [False] * self.__num_chunks
                  self.__chunk_locks = [threading.BoundedSemaphore(1)] * self.__num_chunks
                  self.__chunk_reservations = [0] * self.__num_chunks
                  self.__chunk_owners = [None] * self.__num_chunks
            
            iftlog.log(1, "iftfile: opened " + self.path + " for WRITING, expecting " + str(self.__num_chunks) + " chunks")
            self.__open = True
            return 0
            
         
         else:
            # bad mode
            self.__error = E_BAD_MODE
            return E_BAD_MODE
      
      except Exception, inst:
         iftlog.exception("iftfile: could not open " + self.path, inst)
         print inst
         self.__error = E_IOERROR
         return E_IOERROR
      
   
   def fclose(self):
      """
      Close this file and reset.  Do so atomically in case another thread tries to modify the file.
      """
      self.__expand_lock.acquire()
      self.__open = False
      rc = 0
      self.__num_chunks = 0
      self.__next_chunk = 0
      self.__chunk_mask = []
      self.__chunk_locks = []
      self.__chunk_reservations = []
      self.__chunk_owners = []
      #self.cleanup_chunks()
      self.path = None
      self.__expand_lock.release()
      return rc
   
   
   def fpurge(self):
      """
      Erase all data we've retained.  Do so atomically.
      """
      self.__expand_lock.acquire()
      self.__open = False
      try:
         os.remove( self.path)
      except Exception, inst:
         iftlog.exception("iftfile: could not fpurge " + self.path, inst)
      
      self.__num_chunks = 0
      self.__next_chunk = 0
      self.__mode = 0
      self.__chunk_mask = []
      self.__chunk_locks = []
      self.__chunk_reservations = []
      self.__chunk_owners = []
      #self.cleanup_chunks()
      self.path = None
      self.__expand_lock.release()
      

   def is_open(self):
      """
      Is the file open?
      """
      return self.__open
   
   
   def mark_complete(self):
      """
      We have all the data, even if we didn't get it via conventional means
      """
      self.marked_complete = True
   
   
   def is_complete(self):
      """
      Do we have all of the chunks?
      """
      
      if self.marked_complete:
         return True
         
      if self.known_size == False:
         return False   # no way to tell!
      
      if self.__mode == MODE_READ or self.__mode == 0:
         return True
      
      for chunk in self.__chunk_mask:
         if chunk == False:
            return False
      
      return True
   
   def is_valid_chunk(self, chunk_id ):
      if chunk_id >= 0 and (self.known_size and chunk_id < self.__num_chunks):
         return True
      return False
   
   def read_next_chunk( self ):
      """
      Read the next chunk of this file
      Call fopen() before calling this.
      Return a chunk ID and a chunk with length 0 or more (0 indicates EOF)
      Return negative and None if the file isn't open
      """
      
      if self.__mode != MODE_READ:
         # can't read if we aren't in READ mode
         self.__error = E_BAD_MODE
         return (None, E_BAD_MODE)
      
      if not os.path.exists(self.path):
         self.__error = E_IOERROR
         return (None, E_IOERROR)
      
      self.__read_lock.acquire()
      
      fd = None
      bytes = None
      
      try:
         fd = open(self.path, "r")
         fd.seek( self.__next_chunk * self.__chunk_size )
         bytes = fd.read( self.__chunk_size )
         fd.close()
      except Exception, inst:
         iftlog.exception( "iftfile: could not open " + self.path, inst)
         self.__error = E_IOERROR
         self.__read_lock.release()
         return (None, E_IOERROR)
      
      if bytes == None:
         self.__read_lock.release()
         self.__error = E_NO_DATA
         return (None, E_NO_DATA)
      
      # next chunk
      this_id = self.__next_chunk
      self.__next_chunk = self.__next_chunk + 1
      self.__read_lock.release()
      
      return (bytes, this_id)
      
   
   
   def make_chunks( self ):
      """
      Take the file and break it up into chunks in iftd's chunk directory.
      This will not modify the original file.
      Call only once fopen() is called.
      Front-end for iftfile.make_chunks.
      Available only if the file is open for READing
      @return
         The return value of iftfile.make_chunks
      """
      if self.__mode != MODE_READ:
         self.__error = E_BAD_MODE
         return E_BAD_MODE
      
      return make_chunks( self.path, self.__chunk_size )
      
   
   def cleanup_chunks( self ):
      """
      Clean up from chunking.
      Does nothing if never chunked.
      """
      #cleanup_chunks( self.path )
      pass
      
   

   def __grow_metadata( self, chunk_id ):
      """
      Atomically allocate more chunk masks, locks, and reservations.
      """
      self.__expand_lock.acquire()
      
      # if we were closed, do nothing
      if self.__open == False:
         iftlog.log(5, "iftfile.__grow_metadata(): file is no longer open, so doing nothing")
         self.__expand_lock.release()
         return
      
      num_mutexes = len(self.__chunk_locks)
      if num_mutexes < chunk_id + 1:
         self.__chunk_locks = self.__chunk_locks + [threading.BoundedSemaphore(1)] * (chunk_id + 1 - num_mutexes)

      num_reservations = len(self.__chunk_reservations)
      if num_reservations < chunk_id + 1:
         self.__chunk_reservations = self.__chunk_reservations + [0] * (chunk_id + 1 - num_reservations)
      
      num_owners = len(self.__chunk_owners)
      if num_owners < chunk_id + 1:
         self.__chunk_owners = self.__chunk_owners + [None] * (chunk_id + 1 - num_owners)

      self.__expand_lock.release()


   def is_chunk_available( self, chunk_id ):
      """
      Is a chunk available for writing?
      """
      if chunk_id < 0 or self.__open == False or self.marked_complete:
         return False
      
      else:
         if self.known_size:
            if chunk_id >= len(self.__chunk_mask):
               return False
            else:
               return self.__chunk_mask[ chunk_id ]
            
         else:
            if self.__bytes_max >= 0 and chunk_id * self.__chunk_size < self.__bytes_max:
               return True
            else:
               return False
        
     
   
   def is_chunk_reserved( self, owner, chunk_id ):
      """
      Is a chunk reserved by a given owner?
      """
      if not self.marked_complete and self.__chunk_reservations[ chunk_id ] != 0 and self.__chunk_owners[ chunk_id ] == owner:
         return True
      return False


   def reserve_chunk( self, owner, chunk_id, t ):
      """
      Reserve a chunk for a period of time.
      """
      try:
         # if the file is closed by another thread, we might catch an exception
         
         # sanity check
         if self.marked_complete:
            self.__error = E_COMPLETE
            return E_COMPLETE
            
         if self.__mode != MODE_WRITE:
            self.__error = E_BAD_MODE
            return E_BAD_MODE
         
         if self.known_size == True:
            if chunk_id < 0 or chunk_id >= self.__num_chunks:
               self.__error = E_INVAL
               return E_INVAL
            
         else:
            if chunk_id < 0:
               self.__error = E_INVAL
               return E_INVAL
            
            # positive chunk id--do we need to add more locks?
            self.__grow_metadata( chunk_id )
         
         # someone else had better not own this
         if self.__chunk_reservations[chunk_id] >= time.time() and self.__chunk_owners[ chunk_id ] != None:
            return E_TRY_AGAIN
         
         # can't reserve if someone else locked the chunk
         if not self.__chunk_locks[ chunk_id ].acquire(False):
            return E_TRY_AGAIN
         
         # reserve the chunk
         self.__chunk_reservations[ chunk_id ] = time.time() + t
         self.__chunk_owners[ chunk_id ] = owner
         
         self.__chunk_locks[ chunk_id ].release()
      except Exception, inst:
         # only happens if another thread closed the file
         iftlog.exception("iftfile.reserve_chunk: could not resrve chunk " + str(chunk_id) + " for " + str(owner), inst)
         return E_BAD_STATE
      
      return 0
         

   def unreserve_all( self, owner ):
      """
      Unreserve every chunk owned by this owner
      """
      try:
         if self.__mode == MODE_WRITE:
            for i in xrange(0, len(self.__chunk_locks)):
               if self.__chunk_owners[i] == owner:
                  self.__chunk_locks[i].acquire()
                  self.__chunk_owners[i] = None
                  self.__chunk_reservations[i] = 0
                  self.__chunk_locks[i].release() 
               
            
         
      except Exception, inst:
         pass     # closed under us
         
 
   def lock_chunk( self, owner, chunk_id, override=False, t=1.0 ):
      """
      Lock a chunk for writing.
      No other threads can access it.
      Only valid for MODE_WRITE
      Blocks, and returns
      """
      
      try:
         # sanity check
         if self.marked_complete:
            self.__error = E_COMPLETE
            return E_COMPLETE
         
         if self.__mode != MODE_WRITE:
            self.__error = E_BAD_MODE
            return E_BAD_MODE
         
         # if we know how many chunks there are, then lock it as usual
         if self.known_size == True:
            if chunk_id < 0 or chunk_id >= self.__num_chunks:
               self.__error = E_INVAL
               return E_INVAL
         else:
            # make a new entry if we need to and lock it
            if chunk_id < 0:
               self.__error = E_INVAL
               return E_INVAL
        
            self.__grow_metadata( chunk_id )
         
         # can't lock the chunk if it already has data
         if self.__chunk_mask[ chunk_id ]:
            return E_DUPLICATE
         
         self.__chunk_locks[ chunk_id ].acquire()
         
         # we're takin' over
         if override:
            self.__chunk_reservations[ chunk_id ] = time.time() + t
            self.__chunk_owners[ chunk_id ] = owner
         
         # sanity check again (in case this was called after fclose())
         if self.__mode != MODE_WRITE:
            self.__error = E_BAD_MODE
            self.__chunk_locks[ chunk_id ].release()
            return E_BAD_MODE
         
         # when we return, this thread holds the lock
         return 0
      except Exception, inst:
         if self.__open == False:
            iftlog.log("iftfile.lock_chunk: file is no longer open")
            
         # should only happen if the file gets closed by another thread
         iftlog.exception("iftfile.lock_chunk: could not lock chunk " + str(chunk_id))
         return E_BAD_STATE
         
   
   
   def unlock_chunk( self, owner, chunk_id ):
      """
      Unlock a chunk.
      Only valid for MODE_WRITE
      Return 0 on success; negative on failure
      """
      
      try:
         # sanity check
         if self.__mode != MODE_WRITE:
            self.__error = E_BAD_MODE
            return E_BAD_MODE
            
         if chunk_id < 0 or chunk_id >= self.__num_chunks:
            self.__error = E_INVAL
            return E_OVERFLOW
         
         if self.__chunk_owners[ chunk_id ] == owner:
            self.__chunk_reservations[ chunk_id ] = 0
            self.__chunk_owners[ chunk_id ] = None
            self.__chunk_locks[ chunk_id ].release()
            return 0

         return E_INVAL
      except Exception, inst:
         if self.__open == False:
            iftlog.log("iftfile.unlock_chunk: file is no longer open")
            
         iftlog.exception("iftfile.unlock_chunk: could not unlock " + str(chunk_id))
         return E_BAD_STATE
   
   
   
   def mark_chunk( self, chunk_id, value ):
      """
      Mark a chunk as either set (value == True) or unset (value == False)
      Return 0 on success, nonzero otherwise
      """
      
      try:
         if self.marked_complete:
            self.__error = E_COMPLETE
            return E_COMPLETE
         
         if self.__mode != MODE_WRITE:
            # can't modify chunk mask if we aren't in WRITE mode
            self.__error = E_BAD_MODE
            return E_BAD_MODE
         
         # can we write?
         # don't overwrite what's already there
         if chunk_id < self.__num_chunks and chunk_id >= 0 and self.__chunk_mask[ chunk_id ] == True:
            self.__error = E_BAD_STATE
            return E_BAD_STATE
            
         if self.known_size == True:
            if chunk_id < 0 or chunk_id >= self.__num_chunks:
               self.__error = E_INVAL
               return E_INVAL
         else:
            if self.__bytes_max > 0 and self.__bytes_written + self.__chunk_size > self.__bytes_max:
               # problem--this would expand the file bigger than we have capped it at
               iftlog.log(3, "iftfile: attempted to write " + str(self.__chunk_size) + " more bytes beyond required maximum of " + str(self.__bytes_max) + ", will not mark chunk " + str(chunk_id) )
               return E_OVERFLOW
            
            num_chunks = len(self.__chunk_mask)
            if num_chunks < chunk_id + 1:
               self.__chunk_mask = self.__chunk_mask + [False] * (chunk_id + 1 - num_chunks)
            
            self.__num_chunks = len( self.__chunk_mask )
         
         self.__chunk_mask[ chunk_id ] = True
         return 0
      except Exception, inst:
         if self.__open == False:
            iftlog.log(5, "iftfile.mark_chunk: file is not open")
         
         iftlog.exception( "iftfile.mark_chunk: could not mark chunk " + str(chunk_id) + " as " + str(value))
         return E_BAD_STATE
      
      
   
   def set_chunk( self, chunk, chunk_id, trunicate=True, strict=False ):
      """
      Receive a chunk to be written.
      No trunication will happen unless trunicate = True
      Chunk does not need to be exactly the right size unless strict = True
      CALL ONLY FOR LOCKED CHUNKS
      Return 0 on success
      Return E_INVAL if there is already a chunk for this id, or if the id is out of range
      Return E_BAD_MODE if we're in READ mode
      Return E_OVERFLOW if the upper bound of the known file size has been reached
      Return E_UNDERFLOW if the chunk was too small and strict == True
      """
      
      try:
         
         if self.marked_complete:
            self.__error = E_COMPLETE
            return E_COMPLETE
         
         if self.__mode != MODE_WRITE:
            # can't write if we aren't in WRITE mode
            self.__error = E_BAD_MODE
            return E_BAD_MODE
         
         if self.__bytes_max > 0 and self.__bytes_written + len(chunk) > self.__bytes_max:
            # problem--got too much data
            iftlog.log(3, "iftfile: attempted to write " + str(len(chunk)) + " more bytes beyond required maximum of " + str(self.__bytes_max) + ", will not write chunk " + str(chunk_id))
            return E_OVERFLOW
         
         if trunicate and len(chunk) > self.__chunk_size:
            iftlog.log(3, "iftfile: got chunk of length " + str(len(chunk)) + ", trunicating to " + str(self.__chunk_size))
            chunk = chunk[0 : self.__chunk_size-1]
         
         if len(chunk) < self.__chunk_size and strict:
            return E_UNDERFLOW
         
         
         # write to disk
         try:
            fd = open(self.path, "ab")
            fd.seek( self.__chunk_size * chunk_id )
            fd.write( chunk )
            fd.close()
         except Exception, inst:
            iftlog.exception("iftfile.set_chunk: could not write to " + self.path, inst)
            return E_IOERROR
   
         # do not receive this chunk again
         rc = self.mark_chunk( chunk_id, True )
         if rc != 0:
            iftlog.log(3, "iftfile: will not write chunk " + str(chunk_id))
            return rc
         
         return 0
      except Exception, inst:
         if self.__open == False:
            iftlog.log("iftfile.set_chunk: file is no longer open")
         
         iftlog.exception("iftfile.set_chunk: could not set chunk " + str(chunk_id))
         return E_BAD_STATE
   
   
   
   def get_mode(self):
      """
      Get the mode we opened in
      """
      return self.__mode

   
   def calc_hash(self):
      """
      Calculate our SHA-1 hash from all of our received chunks.
      Return a string of the hex digest
      """
      try:
         m = hashlib.sha1()
         read_fd = open( self.path, "r")
         buff = read_fd.read()
         m.update( buff )
         read_fd.close() 
         
         return m.hexdigest()
      except Exception, inst:
         iftlog.exception("iftfile.calc_hash", inst)
         return ""
   
   
   def get_unwritten_chunks(self):
      """
      Get a list of the chunks that have not yet been received.
      Note: this list will possibly be outdated as soon as it is returned!
      """
      try:
         # sanity check
         if self.__mode != MODE_WRITE:
            self.__error = E_BAD_MODE
            return E_BAD_MODE
         
         index = 0
         ret = []
         for i in xrange(0, len(self.__chunk_mask)):
            if self.__chunk_reservations[i] >= time.time():
               continue

            try:
               chunk = self.__chunk_mask[i]
               if chunk == False:
                  ret.append( i )
            except:
               break
         
         if len(ret) == 0 and self.known_size == False:
            # if we don't know the size, add the chunks required to reach __bytes_max
            ret.append( len(self.__chunk_mask) )
         
         return ret
      except Exception, inst:
         iftlog.exception("iftfile.get_unwritten_chunks", inst)
         return []
   

def get_hash( filename ):
   """
   Given a filename, get its SHA1 hash
   """
   m = hashlib.sha1()
   try:
      file_handle = open(filename)
      while True:
         chunk = file_handle.read( DEFAULT_FILE_CHUNKSIZE )
         if len(chunk) == 0:
            break
         m.update( chunk )
      
      return m.hexdigest()
   except:
      return E_FILE_NOT_FOUND

   

"""
Location of the directory to store file chunks
"""
__file_chunks_dir = "/tmp/iftd/files/"


def startup( file_chunks_dir = "/tmp/iftd/files/" ):
   """
   Start up the file I/O system
   """
   global __file_chunks_dir
   
   # set up the chunk directory
   rc = os.popen("mkdir -p " + file_chunks_dir ).close()
   if rc != 0 and rc != None:
      iftlog.log(5, "Could not start up iftfile I/O; could not create " + file_chunks_dir + " (rc = " + str(rc) + ")")
      return E_IOERROR
   
   __file_chunks_dir = file_chunks_dir
   if __file_chunks_dir[-1] != "/":
      __file_chunks_dir += "/"
   
   return 0
   


def shutdown():
   """
   Shut down the file writer
   """
   
   os.popen("rm -rf " + __file_chunks_dir )
   iftlog.log(3, "iftfile: shutdown complete")
   

def get_chunks_dir( filename=None, filehash=None, remote_iftd=None ):
   """
   Get the top-level directory where iftd stores temporary file chunks
   
   @arg filename
      The name of the file from which to get the chunks path.
      If not given, this method will return the top-level chunks path.
      
   @arg filehash
      This is the sha-1 hash of the file.
      If not given, this method will return the top-level chunks path.
      
   @arg remote_iftd
      This is a boolean to indicate if there is a remote iftd.
      If there is not, then there is no designated chunks directory.
   """
   if filename and filehash and remote_iftd != False:
      if os.path.basename( filename ) != '':
         filename = os.path.basename( filename )
      
      return os.path.join(__file_chunks_dir, filename) + "." + filehash
   else:
      return __file_chunks_dir


def make_chunks_dir( filename, filehash ):
   """
   Make a directory from the filename and filehash to store incoming chunks into.
   """
   if filename[0] == "/":
      filename = filename[1:]
   
   file_dir = os.path.basename( filename ) + "." + str(filehash)
      
   if os.path.exists( os.path.join(__file_chunks_dir, file_dir) ):
      iftlog.log(3, "WARNING: " + os.path.join(__file_chunks_dir, file_dir) + " exists!")
      try:
         os.popen("rm -rf " + os.path.join(__file_chunks_dir, file_dir) + "/*").close()
      except:
         pass
         
      return 0    # already done!
   
   chunk_dir = get_chunks_dir( filename, filehash )
   try:
      rc = os.popen("mkdir -p " + chunk_dir ).close()
      if rc != None:
         iftlog.log(5, "iftfile: could not make chunk directory " + chunk_dir )
         return E_IOERROR
      
      return 0
   except Exception, inst:
      iftlog.exception( "iftfile: could not make chunk directory " + chunk_dir )
      return E_IOERROR


def cleanup_chunks_dir( filename, filehash ):
   """
   Clean up a chunk directory, given the filename and file hash
   """
   if filename == None or filehash == None:
      return 0
   
   iftlog.log(1, "cleaning up chunks for " + str(filename))
   chunk_dir = get_chunks_dir( filename, filehash )
   
   os.popen("rm -rf " + chunk_dir + "/*").close()
   os.popen("rmdir " + chunk_dir).close()
   return 0
   
   
def get_job_path( filename ):
   """
   If a protocol wants to write the job to "send" it (a.k.a. make it available to the receiver), it should
   get the path to where to do so with this method.
   """
   if filename == None:
      filename = ""
   return get_chunks_dir() + "/" + filename
   

def get_filesize( path ):
   try:
      return os.stat( path ).st_size
   except:
      return -1


def make_chunks( filename, chunksize ):
   """
   Split the given file into chunks and store them in the chunks directory.
   Returns (0, file_hash, chunk_hashes, chunk_paths) on success; (nonzero, None, None, None) on error.
   """
   # sanity check
   if not os.path.exists(filename):
      iftlog.log(3, "Skipping " + filename + " since it cannot be found")
      return (E_IOERROR, None, None, None)
   
   if not (stat.S_IWUSR & os.stat( filename ).st_mode):
      iftlog.log(3, "Skipping " + filename + " since I do not have read permission")
      return (E_IOERROR, None, None, None)
   
   # get file hash
   file_hash = get_hash( filename )
   
   first_char = ""
   if filename[0] == "/":
      filename = filename[1:]
      first_char = "/"
      
   file_dir = os.path.basename(filename) + "." + str(file_hash)
   
   # does the directory exist?
   if os.path.exists( __file_chunks_dir + file_dir ):
      iftlog.log(3, "WARNING: " + __file_chunks_dir + file_dir + " exists!  Removing...")
      rc = os.popen("rm -rf " + __file_chunks_dir + file_dir ).close()
      if rc != None:
         iftlog.log(5, "ERROR: could not make chunks for " + filename + "; " + __file_chunks_dir + file_dir + " could not be removed!")
         return (E_IOERROR, None, None, None)
      
   
   
   # make the directory and populate it with chunks
   rc = os.popen("mkdir -p " + __file_chunks_dir + file_dir ).close()
   if rc != None:
      iftlog.log(5, "ERROR: could not make chunk directory " + __file_chunks_dir + file_dir )
      return (E_IOERROR, None, None, None)
   
   # open the file
   fd = None
   try:
      fd = open(first_char + filename, "rb")
      if not fd:
         iftlog.log(5, "ERROR: could not open " + filename + " for reading!")
         cleanup_chunks( filename, file_hash )
         return (E_IOERROR, None, None, None)
   except Exception, inst:
      iftlog.exception("ERROR: could not open " + filename + " for reading!", inst)
      cleanup_chunks( filename, file_hash )
      return (E_UNHANDLED_EXCEPTION, None, None, None)
   
   # write out chunks
   chunk_id = 0
   chunk_hashes = []
   chunk_paths = []
   while True:
      chunk = fd.read( chunksize )
      chunk_name = str(chunk_id)
      chunk_fd = open( __file_chunks_dir + file_dir + "/" + chunk_name, "wb" )
      chunk_fd.write( chunk )
      chunk_fd.close()
      chunk_id += 1
      
      m = hashlib.sha1()
      m.update( chunk )
      chunk_hash = m.hexdigest()
      chunk_hashes.append( chunk_hash )
      
      chunk_paths.append( __file_chunks_dir + file_dir + "/" + chunk_name )
      
      if len(chunk) != chunksize:
         # last chunk; EOF reached
         break
   
   fd.close()
   
   iftlog.log(1, "Broke " + filename + " into " + str(chunk_id) + " chunks in " + __file_chunks_dir + file_dir + "/" )
   return (0, file_hash, chunk_hashes, chunk_paths)



def get_chunks( filename, chunksize ):
   """
   Determine the chunks and sha-1 hashes of the would-be chunks of a file.
   """
   # sanity check
   if not os.path.exists(filename):
      iftlog.log(3, "Skipping " + filename + " since it cannot be found")
      return (E_IOERROR, None)
   
   if not (stat.S_IWUSR & os.stat( filename ).st_mode):
      iftlog.log(3, "Skipping " + filename + " since I do not have read permission")
      return (E_IOERROR, None)
   
   chunks = []
   chunk_hashes = []
   fd = open( filename, "rb" )
   while True:
      chunk = fd.read( chunksize )
      
      m = hashlib.sha1()
      m.update( chunk )
      chunk_hash = m.hexdigest()
      chunk_hashes.append( chunk_hash )
      chunks.append( chunk )
      
      if len(chunk) != chunksize:
         # last chunk; EOF reached
         break
   
   fd.close()
   return (0, chunks, chunk_hashes)
   


def cleanup_chunks( filename, filehash ):
   """
   Clean up after chunking
   """
   os.popen("rm -rf " + __file_chunks_dir + "/" + filename + "." + str(filehash)).close()
   


def apply_dir_permissions( filepath ):
   """
   Given a directory and a file path, change the file's permissions, group, and ownership to match
   the directory.  This is needed since IFTD often runs under permissions
   different from those of the applications that invoke it to transfer files.
   """
   
   filepath_dir = os.path.dirname( filepath )
   
   filepath_dir_stats = os.stat( filepath_dir )
   
   dir_permissions = filepath_dir_stats.st_mode
   dir_owner_id = filepath_dir_stats.st_uid
   dir_group_id = filepath_dir_stats.st_gid
   
   os.chmod( filepath, dir_permissions )
   os.chown( filepath, dir_owner_id, dir_group_id )

   return 0
   

"""
We need to treat iftfile instances like singletons so we can 
have multiple senders and receivers working on a file without
duplication.

Mapping:
   fileName --> [iftfileInstance, [owners]]
"""
__iftfile_recv_table = {}
__iftfile_recv_table_lock = threading.BoundedSemaphore(1)


def acquire_iftfile_recv( owner, file_name, file_attrs=None ):
   """
   Get a reference to a file that is being received.
   TODO: make this thread-safe
   """
   if __iftfile_recv_table.has_key( file_name ) == True and __iftfile_recv_table[file_name] != None:
   
      __iftfile_recv_table_lock.acquire()
      if __iftfile_recv_table.has_key(file_name) == False:
         __iftfile_recv_table_lock.release()
         return None
         
      if __iftfile_recv_table[ file_name ][1] == None:
         __iftfile_recv_table[ file_name ][1] = [owner]
         
      elif owner not in __iftfile_recv_table[ file_name ][1]:
         # new owner
         __iftfile_recv_table[ file_name ][1].append( owner )

      __iftfile_recv_table_lock.release()
      return __iftfile_recv_table[ file_name ][0]
      
   else:
      __iftfile_recv_table_lock.acquire()
      # did someone insert underneath us?
      if __iftfile_recv_table.has_key( file_name ) and __iftfile_recv_table[file_name] != None:
         __iftfile_recv_table_lock.release()
         return __iftfile_recv_table[file_name][0]
         
      # try to open the file
      fd = iftfile( file_name )
      rc = fd.fopen( file_attrs, MODE_WRITE )
      if rc < 0:
         __iftfile_recv_table_lock.release()
         return None

      #print "first owner " + str(owner) + " for " + file_name      
      __iftfile_recv_table[ file_name ] = [fd, [owner]]
      __iftfile_recv_table_lock.release()
      return fd


def release_iftfile_recv( owner, file_name ):
   """
   Release a reference to a file that is being received
   """
   
   __iftfile_recv_table_lock.acquire()
   
   if __iftfile_recv_table.has_key( file_name ) == True and __iftfile_recv_table[file_name] != None:
      # no owners
      if __iftfile_recv_table[ file_name ][1] == None:
         #print "no owner"
         __iftfile_recv_table_lock.release()
         return E_INVAL
      
      # can't release if we never owned this file
      if owner not in __iftfile_recv_table[ file_name ][1]:
         #print "ERROR: " + str(owner) + " is not an owner of " + file_name
         __iftfile_recv_table_lock.release()
         return E_INVAL
      
      else:   
         # remove this owner
         __iftfile_recv_table[ file_name ][1].remove(owner)
         
         #print str(owner) + " no longer owns " + file_name
      
      if len(__iftfile_recv_table[ file_name ][1]) == 0:
         
         rc = __iftfile_recv_table[ file_name ][0].fclose()
         __iftfile_recv_table[ file_name ] = None
         del __iftfile_recv_table[ file_name ]
         __iftfile_recv_table_lock.release()
         #print file_name + " out of references, so unallocated"
         return rc

      #print str(owner) + " end of the line on " + file_name      
      __iftfile_recv_table_lock.release()
      return 0
   else:
      # bad name
      #print "bad name"
      __iftfile_recv_table_lock.release()
      return E_INVAL


def invalidate_iftfile_recv( file_name ):
   """
   Completely erase all references for an iftfile
   """
   if __iftfile_recv_table.has_key( file_name ) == True:
      __iftfile_recv_table_lock.acquire()
      
      # ensure that it is still there
      if __iftfile_recv_table.has_key( file_name ) == False:
         __iftfile_recv_table_lock.release()
         return 0
      
      rc = __iftfile_recv_table[ file_name ][0].fclose()
      del __iftfile_recv_table[ file_name ]
      __iftfile_recv_table_lock.release()
      return rc
   
   return 0
