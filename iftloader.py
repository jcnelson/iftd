#!/usr/bin/env python

"""
iftloader.py
Copyright (c) 2009 Jude Nelson

This module contains methods loading and instantiating protocols, senders, and receivers dynamically.
"""


import iftlog
import protocols

from iftdata import *

"""
Where are the protocols?
"""
PROTOCOLS_DIR = "./protocols/"
PROTOCOLS_PACKAGE = "protocols"

"""
Protocol instances that have been loaded
"""
__loaded_proto_instances = {}

"""
Next handle for a loaded protocol
"""
__next_proto_handle = 1

"""
Store a protocol for future use and return a handle to it.
"""
def store_proto_instance( proto ):
   global __next_proto_handle
   handle = __next_proto_handle
   __next_proto_handle = __next_proto_handle + 1
   __loaded_proto_instances[ handle ] = proto
   return handle

"""
Delete a protocol instance
"""
def delete_proto_instance( handle ):
   if __loaded_proto_instances.has_key( handle ) == False:
      return E_NO_VALUE
   
   try:
      del __loaded_proto_instances[ handle ]
      return 0
   except Exception, inst:
      iftlog.exception( "iftloader: could not delete protocol instance", inst)
      return E_UNHANDLED_EXCEPTION

"""
Get a protocol instance that we previously had instantiated.
Return None if it doesn't exist
"""
def lookup_proto_instance( proto_handle ):
   try:
      inst =  __loaded_proto_instances[ proto_handle ]
      return inst
   except:
      iftlog.log(5, "iftloader: Invalid handle " + str(proto_handle) )
      return None


"""
Load a package dynamically

@arg name
   package name containing protocol

@return
   0 on success, -1 on failure
"""
def import_package( name ):
   try:
      __import__(name)
      iftlog.log(0, "iftloader: Loaded " + name)
      return 0
   except Exception, inst:
      iftlog.exception( "iftloader: could not load " + name, inst )
      return -1


"""
Create an instance of a class.

@arg name
   Name of the class (package and all)

@arg constructor_args
   Arguments to pass to the constructor as a list 
   
@return
   An instance on success, or None on failure
"""
def instantiate_class( name, constructor_args ):
   constructor_str = ""
   i = 0
   if constructor_args != None:
      for arg in constructor_args:
         constructor_str.append( ",constructor_args[" + str(i) + "]" )
         i = i + 1
   else:
      constructor_str = ' '
      
   try:
      
      # note: constructor_str is preceeded by a ','
      instance = eval( name + '(' + constructor_str[1:] + ')' )
      return instance
   except Exception, inst:
      iftlog.exception( "Could not instantiate class with \'" + name + '(' + constructor_str[1:] + ')\'', inst )
      return None

