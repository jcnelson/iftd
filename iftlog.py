#!/usr/bin/env python

"""
iftlog.py
Copyright (c) 2009 Jude Nelson

iftd loggin facility.
"""

import threading
import thread
from time import strftime
import traceback
import os
import stat
import time

LOGMODE_PRINT = "LOGMODE_PRINT"
LOGMODE_FILE = "LOGMODE_FILE"
LOGMODE_RAM = "LOGMODE_RAM"


__output_disp = LOGMODE_PRINT    # what to do with output
__output_thresh = 0             # minimum threshold

__default_output_fname = "/var/log/iftd.log"
__output_fname = __default_output_fname               # output file name (defualt)
__output_maxlines = 10000                             # maximum lines per log (give or take)
__logswap_sem = threading.BoundedSemaphore(1)         # don't swap logs concurrently
__logno = 0                                           # how many times have we renamed the log
__loglinecnt = 0                                      # current log line count
__log_ram = ""                                        # if LOGMODE_RAM is active, this is the string in RAM holding it

LOG_MIN_THRESHOLD = 0
LOG_MAX_THRESHOLD = 10

def log( level, msg, mode=None ):
   global __output_fname
   global __output_disp
   global __output_maxlines
   global __loglinecnt
   
   output_str = "[iftd " + strftime("%H:%M:%S") + "] " + str(msg)
   #output_str = "[" + str(time.time()) + "] " + str(msg)

   output_disp = mode
   
   if output_disp == None:
      output_disp = __output_disp
   
   if output_disp == LOGMODE_RAM and level >= __output_thresh:
      __log_ram += output_str + "\n"
   
   elif output_disp == LOGMODE_PRINT and level >= __output_thresh:
      print output_str
   
   elif output_disp == LOGMODE_FILE and level >= __output_thresh:
      
      fd = open(__output_fname, "a+")
      fd.write( output_str + "\n" )
      fd.close()  # want output immediately
      
      # TODO: do we really want thread safety?  Or will this suffice?
      __loglinecnt += 1
      if __loglinecnt >= __output_maxlines:
         rc = swap_logs()
         if rc != 0:
            # couldn't swap logs; defer to printing
            set_logging_mode( LOGMODE_PRINT )


def exception( msg, inst, mode=None ):
   log( 5, msg + " (" + str(type(inst)) + ", \'" + str(inst) + "\')", mode )
   print ""
   traceback.print_exc()
   print ""


def set_verbosity( threshold ):
   __output_thresh = threshold
   

def swap_logs():
   # useful only if in LOGMODE_FILE
   global __output_disp
   global __logswap_sem
   global __output_fname
   global __logno
   global __loglinecnt
   
   if __output_disp == LOGMODE_FILE:
      rc = __logswap_sem.acquire(blocking=False)
      if not rc:
         return 0  # someone else is doing this
      
      newname = __output_fname + "." + str(__logno)
      os.rename( __output_fname, newname )
      __logno += 1
      __loglinecnt = 0
      
      __logswap_sem.release()
      
      
      rc = os.popen( "rm -f " + newname + "; gzip " + newname ).close()
   return 0
   


def set_logging_mode( new_mode, **kwargs ):
   global __output_maxlines
   global __output_disp
   global __output_fname
   global __default_output_fname
   
   if new_mode == LOGMODE_FILE:
      kwargs.setdefault("maxlines", 10000)
      kwargs.setdefault("filename", __default_output_fname)
      fname = kwargs.get("filename")
      maxlines = kwargs.get("maxlines")
      
      if os.path.exists( fname ) and not (stat.S_IWUSR & os.stat( fname ).st_mode):
         log( 5, "set_logging_mode: could not open " + fname + " for writing!", LOGMODE_PRINT)
         return E_INVAL
      
      if not os.path.exists( fname ) and not (stat.S_IWUSR & os.stat( os.path.dirname(fname) ).st_mode):
         log( 5, "set_logging_mode: could not open " + fname + " for writing!", LOGMODE_PRINT)
         return E_INVAL
      
      __output_maxlines = maxlines
      __output_fname = fname
   
   __output_disp = new_mode
   return 0
      

def dump_log():
   global __log_ram
   
   if __output_disp != LOGMODE_RAM:
      return      # nothing to do
   
   print __log_ram
   __log_ram = ""
   
