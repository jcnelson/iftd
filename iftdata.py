#!/usr/bin/env python

"""
iftdata.py
Copyright (c) 2009 Jude Nelson

Global data used by the program and methods to manipulate them.
"""

import copy
import iftlog



"""
Common error messages
"""

IFTD_OK           = 0         # no error to report
IFTD_NO_ACTIVE    = 1         # for senders, this means that no active protocols were used (so it's impossible to know if the transmission was successful)
                              # TODO: fix this!  Have the receiver ACK the sender in this situation!
E_NO_VALUE        = -1        # a value is not defined for a given key
E_EOF             = -2        # we couldn't receive everything
E_NO_CONNECT      = -3        # we couldn't connect or send correctly
E_NO_DATA         = -4        # we couldn't read/receive data correctly
E_FORK_FAIL       = -5        # we couldn't start a thread
E_DUPLICATE       = -6        # can't have more than one of this
E_FILE_NOT_FOUND  = -101      # could not open a file
E_INVAL           = -102      # data is invalid (either the given data or the structure's internal data)
E_IOERROR         = -103      # I/O error
E_ALREADY_OPEN    = -104      # a file is already open
E_OVERFLOW        = -105      # the buffer was too big
E_UNDERFLOW       = -106      # the buffer was too small
E_BAD_MODE        = -107      # open in the wrong mode
E_LOCKED          = -108      # resource could not be accessed since it is in use
E_UNAVAIL         = -109      # resource not available
E_BAD_STATE       = -110      # we're in the wrong state to do this
E_WRITE           = -111      # could not write data
E_COMPLETE        = -112      # received everything, but tried to receive more
E_TERMINATED      = -200      # request could not be carried out since the thread isn't running
E_UNHANDLED_EXCEPTION = -300  # caught unhandled exception
E_FAILURE         = -400      # transfer has failed
E_CORRUPT         = -500      # data is corrupt (e.g. bad hash, etc)
E_TRY_AGAIN       = -1000     # the attempt didn't work this time, but it might work in a subsequent call
E_TIMEOUT         = -1001     # too much time was taken for the operation
E_NOT_IMPLEMENTED = -10000    # no implementation exists

"""
Is a return code an error?
"""
def is_error( rc ):
   for attr in dir():
      if attr.count("E_") != 0:
         if rc == eval(attr):
            return True
   
   return False

"""
Port on which to bind for app/user communication
"""
USER_PORT = 4000
RPC_DIR = "RPC2"

"""
Lockfile
"""
LOCKFILE_PATH = "/tmp/iftd.pid"

"""
File directory from which to serve data
"""
SEND_FILES_DIR = None #"/tmp/iftd-send"

"""
File directory to which to receive data
"""
RECV_FILES_DIR = None #"/tmp/iftd-recv"

"""
Data that can be re-used to reinstantiate iftproto instances.
Mapping:
   transmitter_name --> (setup_args, respawn)
"""
__reusable_data = {}

"""
Tuple indices to the above data
"""
REUSABLE_DATA_SETUP_ARGS = 0
REUSABLE_DATA_RESPAWN = 1

def send_dir( newname ):
   global SEND_FILES_DIR
   if newname == None or len(newname) == 0:
      iftlog.log(5, "iftdata: not changing send-files directory to None")
   else:
      SEND_FILES_DIR = newname


def recv_dir( newname ):
   global RECV_FILES_DIR
   if newname == None or len(newname) == 0:
      iftlog.log(5, "iftdata: not changing recv-files directory to None")
   else:
      RECV_FILES_DIR = newname

def get_reusable_data( proto_name, data_key, do_copy=True ):
   """
   Get a (deep) copy of a piece of reusable transmitter data, given the name of the receiver.
   
   @arg proto_name
      Name of the protocol
   
   @arg data_key
      either REUSABLE_DATA_SETUP_ARGS or REUSABLE_DATA_RESPAWN
      
   Return the data on success, or None on error.
   """
   
   if __reusable_data.has_key(proto_name) == False:
      return {}
   
   if __reusable_data[proto_name].has_key(data_key) == False:
      return {}
   
   try:
      if do_copy:
         return copy.deepcopy(__reusable_data[ proto_name ][ data_key ])
      else:
         return __reusable_data[ proto_name ][ data_key ]
   except Exception, inst:
      iftlog.exception( "Could not get re-instantiation data for transmitter " + proto_name, inst )
      return None



def save_reusable_data( proto_name, setup_args, respawn=True ):
   """
   Save reusable data for a given protocol.
   This will overwrite any previous data
   """
   
   iftlog.log(1, "Recording reusable data for transmitter " + proto_name )
   if __reusable_data.has_key(proto_name) == False:
      __reusable_data[proto_name] = {}
      
   __reusable_data[proto_name][REUSABLE_DATA_SETUP_ARGS] = (copy.deepcopy( setup_args ), respawn)
   
   return 0



def load_config( config_filename ):
   """
   Load the iftd configuration.
   
   @param config_filename
      Path to the config XML flie on disk
      
   @return
      (0, chunks path (or None), A dictionary mapping protocol names to their setup arguments) on success
      (Nonzero, chunks path (or None) None) on failure
   """
   
   try:
      import lxml
      import lxml.etree
      
      config = {}

      fd = None
      try:
         fd = open( config_filename, "rt" )
         if fd == None:
            iftlog.log(5, "Config file " + config_filename + " not found")
            return (E_FILE_NOT_FOUND, None, None)
      except Exception, inst:
         iftlog.log(5, "Config file " + config_filename + " could not be opened", inst)
         return (E_FILE_NOT_FOUND, None, None)
      
      parser = lxml.etree.XMLParser(remove_blank_text = True)
      for line in fd:
         parser.feed(line)
      
      root = parser.close()
      
      protocol_data = {}
      
      # sanity check...
      if root.tag != "iftd":
         iftlog.log(5, "Unrecognized tag " + str(root.tag))
         return (E_INVAL, chunks_path, None)
      
      for opt in root:
         if opt.tag == "protocol":
            # handle a protocol tree
            for proto in opt:
               proto_package = opt.get("name")
               proto_name = proto_package.split('.')[-1]
               
               class_name = ''   # fully qualified protocol class
               setup_args = {}   # setup args for this protocol
               
               if proto.tag == "sender":
                  class_name = proto_name + "_sender"
               
               elif proto.tag == "receiver":
                  class_name = proto_name + "_receiver"
               
               else:
                  iftlog.log(5, "2 Ignoring unrecognized tag " + str(proto.tag) + " in " + lxml.etree.tostring(proto))
                  continue
               
               for config_opt in proto:
                  # setup item?
                  if config_opt.tag == "setup":
                     config_type = config_opt.get("type")
                     if config_type == None:
                        iftlog.log(3, "WARNING: Could not determine type for " + config_opt.tag)
                        config_type = "str"
                     
                     config_dict = config_opt.attrib
                     
                     for key in config_dict:
                        if key == "type":
                           continue
                        
                        if config_type == "int":
                           setup_args[key] = int(config_dict[key])
                        if config_type == "bool":
                           setup_args[key] = bool(config_dict[key])
                        if config_type == "str":
                           setup_args[key] = str(config_dict[key])
                        if config_type == "float":
                           setup_args[key] = float(config_dict[key])
                        if config_type == "chr":
                           setup_args[key] = chr(config_dict[key])
                     
               # save this setup data
               if protocol_data.has_key(class_name) == False:
                  protocol_data[class_name] = [setup_args]
               else:
                  protocol_data[class_name].append( setup_args )
                  
               
            
         
         else:
           config[opt.tag] = {}
           for key in opt.attrib:
             config[opt.tag][key] = opt.attrib[key]
         
      
      
      return (0, config, protocol_data)
      
   except ImportError:
      iftlog.log(5, "python-lxml is not installed.  Configuration could not be loaded!")
      return (E_FILE_NOT_FOUND, None, None)

   except Exception, inst:
      iftlog.exception("Error loading configuration", inst)
      return (E_UNHANDLED_EXCEPTION, None, None)
