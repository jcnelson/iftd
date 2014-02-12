#! /usr/bin/env python

"""
Stork Project (http://www.cs.arizona.edu/stork/)
Module: arizonatransfer
Description:   Provides file transferring from host and synchronizing two
               different directories.
               Uses IFTD as the transfer mechanism.

"""

"""arizonaconfig
   options=[["",     "--transfermethod",  "transfermethod", "append", "string", ["http", "ftp"], "program", "use this method to transfer files (default http, ftp)"],
            ["",     "--metatransfermethod", "metatransfermethod", "append", "string", ["http", "ftp"], "program", "use this method to transfer metadata (default http, ftp)"],
            ["",     "--transfertempdir", "transfertempdir", "store", "string", "/tmp/stork_trans_tmp", "PATH", "use this path to save transferd files temporary (default is /tmp/stork_trans_tmp)"],
            ["",     "--metafilecachetime", "metafilecachetime", "store", "int", 60, None, "seconds to cache metafile without re-requesting"],
            ["",     "--disablesignedmetafile", "disablesignedmetafile", "store_true", None, False, None, "disable signed metafile"],
            ["",     "--disabletransferhashcheck", "disabletransferhashcheck", "store_true", None, False, None, "disable transfer hash checking"]]
   includes=["$MAIN/transfer/*"]
"""

import sys
import os
import arizonareport
import arizonaconfig
import securerandom
import arizonageneral
import ravenlib.stats
import shutil
import traceback
import arizonacrypt
import storkpackage
import time
import signal
import fnmatch
import urllib2
import copy

sys.path.append("/usr/local/lib/python2.5/site-packages/iftd")
import iftapi
import iftfile
import iftlog
import iftproto

from stat import *

# metafile holds the names of files which need to sync
METAFILE_FN = "metafile"
SIGNED_METAFILE_FN = METAFILE_FN + ".signed"

# it holds what transfer method is imported
#global arizonafetch          # OBSOLETE
#arizonafetch = None          # OBSOLETE

# indicates importing status. If init_status is -1, then no transfer module has been imported.
init_status = -1
#glo_prioritylist = []         # OBSOLETE
#modules_failed_install = []   # OBSOLETE

# pass this in a file tuple when the size is unknown
SIZE_UNKNOWN = None





class TransferTimeOutExc(Exception):
    def __init__(self, value = "Timed Out"):
        self.value = value
    def __str__(self):
        return repr(self.value)





def TransferAlarmHandler(signum, frame):
   raise TransferTimeOutExc





glo_oldsignal = None
def __enable_timeout(seconds):
   """
      <Purpose>
          Enable the alarm signal
      <Arguments>
          seconds - number of seconds when alarm will go off
   """
   global glo_oldsignal
   glo_oldsignal = signal.signal(signal.SIGALRM, TransferAlarmHandler)
   signal.alarm(seconds)





def __disable_timeout():
   """
      <Purpose>
          disable the alarm signal
   """
   global glo_oldsignal
   signal.alarm(0)
   if glo_oldsignal!=None:
       signal.signal(signal.SIGALRM, glo_oldsignal)
       glo_oldsignal = None





def __compute_timeout(filelist):
   """
      <Purpose>
          compute a timeout for a file list.
      <Arguments>
          filelist - a list of file tuples (name, hash, size)
      <Returns>
          timeout in seconds
   """
   total_size = 0
   unknown_size = True

   for file in filelist:
      size = file.get('size', SIZE_UNKNOWN)
      if size != SIZE_UNKNOWN:
         unknown_size = False
         total_size = total_size + size

   if unknown_size:
      # if the size is unknown, return 60 minutes
      return 60*60
   else:
      # otherwise, return 10 minutes + 1 minute per megabyte
      return 60*10 + total_size / (1024*1024/60)





def reset_transfer_method():
   """
      <Purpose>
          reset the init_status variable. This will cause arizonatransfer to
          start transfering with the highest priority method again
   """
   global init_status
   init_status = -1




def default_hashfunc(filename):
   return arizonacrypt.get_fn_hash(filename, "sha1")




def initialize_transfer_method(method):
   #global arizonafetch
   #global glo_prioritylist
   global init_status
   #global modules_failed_install;
   
   init_status = 0
   
   """
   if method in modules_failed_install:
       arizonareport.send_error(2, "WARNING: method '" + method + "' previously tried to initialize and failed; skipping...") 
       arizonafetch = None
       return

   try:
      # import a certain transfer method
      # TODO possible security problem?   For example, method="nest as arizonafetch; hostile code...; #"
      exec("import transfer.arizonatransfer_" + method + " as arizonafetch")    

      # crazy and only way to use 'exec'   :)
      globals()['arizonafetch'] = locals()['arizonafetch']

      arizonareport.send_syslog(arizonareport.INFO, "\n" + arizonafetch.transfer_name() + " starting...")

      # initialize the transfer method
      try:
         arizonafetch.init_transfer_program()
      except:
         arizonareport.send_syslog(arizonareport.ERR, "getfiles(): Initializing : Initialization error.")
         arizonareport.send_error(2, "WARNING: Could not initialize " + method + " transfer...")
         arizonafetch = None
         return

      # init_status is set by index number so that it will indicate that something is imported
      init_status = glo_prioritylist.index(method)
   # if module name doesn't exist
   except ImportError, (errno):
      modules_failed_install.append(method)
      arizonareport.send_syslog(arizonareport.ERR, "getfiles(): Initializing : Import error(" + str(errno) + ")")
      arizonafetch = None
      arizonareport.send_error(2, "WARNING: Could not import " + method + " transfer: " + str(errno))
   except NameError, (errno):
      # BitTorrent 4.4.0-5.fc7 on python 2.5 is throwing this error
      # This is what my Fedora8 machine has installed
      modules_failed_install.append(method)
      arizonareport.send_syslog(arizonareport.ERR, "getfiles(): Initializing : Name error(" + str(errno) + ")")
      arizonafetch = None
      arizonareport.send_error(2, "WARNING: Could not import " + method + " transfer: " + str(errno))
   """


def getfiles(host, filelist, destdir, hashlist=[""], prog_indicator=0, createhashfile=False):
   """ stub for the old getfiles to convert parameters to getfiles1

       XXX this will go away soon

   """
   if hashlist == [""]:
      hashlist = None

   if hashlist != None and len(hashlist) != len(filelist):
      arizonareport.send_syslog(arizonareport.ERR, "getfiles(): The number of files given doesn't match the number of hashes given.")
      return (False, downloaded_files)

   tuples = []

   for (i,file) in enumerate(filelist):
      if hashlist:
         hash = hashlist[i]
      else:
         hash = None

      dict = {"filename": file}
      if hash:
         dict['hash'] = hash

      # XXX this will be going away
      dict['hashfuncs'] = [storkpackage.get_package_metadata_hash, default_hashfunc]

      tuples.append(dict)

   return getfiles1(host, tuples, destdir, prog_indicator, createhashfile)





def getfiles1(host, filelist, destdir, prog_indicator=0, createhashfile=False, ignoreHash=False, prioritylist=None):
   """
   <Purpose>
      This fetches files from given host, using prioritylist which holds
      transfer methods to fetch files.
      It tries to get files by one method, and if it fails it uses next
      possible method until it gets all files needed.

   <Arguments>
      host:
         'host' holds two things, a server name and download directory.
         For example, if you want to retrieve files from '/tmp/' directory
         in 'quadrus.cs.arizona.edu' server, the 'host' will be
         'quadrus.cs.arizona.edu/tmp'.
         'host' should be a string.

      filelist:
         'filelist' is a list of files which need to be retrieved.
         'filelist' should be a list of dictionaties of the format:
             {"filename": filename,
              "hash": hash,
              "size": size,
              "hashfuncs": list of hashfuncs to try}
             the hash and size parameters can be None if unavailable

      destdir:
         'destdir' is a destination directory where retrieved files will
         be placed. A user should have 'destdir' exist before retrieving
         files. 'destdir' should be a string.

      prog_indicator:
         DEPRICATED, unused.
         
      prioritylist:
         If it is not None, then this is a whitelist of protocols to use 
         for transferring the given files.  NOTE: the protocols here are
         base names (e.g. 'http', 'ftp', 'scp', etc., as opposed to
         'http_receiver', 'ftp_receiver', 'scp_receiver', etc.).

   <Exceptions>
      None.

   <Side Effects>
      Messes with SIGALRM
      Set init_status

   <Returns>
      True or False to indicate success, and a list of downloaded files
   """

   global init_status
   #global arizonafetch
   #global glo_prioritylist

   arizonareport.send_out(4, "[DEBUG] getfiles started")
   arizonareport.send_out(4, "host = " + str(host) + ", filelist = " + str(filelist))

   # downloaded files list
   downloaded_files = []

   # check if host is a string
   arizonageneral.check_type_simple(host, "host", str, "arizonatransfer.getfiles")

   # check if destdir is a string
   arizonageneral.check_type_simple(destdir, "destdir", str, "arizonatransfer.getfiles")

   # get username
   username = arizonageneral.getusername()

   # check that the destination directory exists
   if not os.path.isdir(destdir):
      arizonareport.send_syslog(arizonareport.ERR, "\ngetfiles(): The destination directory '" + destdir + "' does not exist...   Aborting...")
      # return false and empty list
      return (False, downloaded_files)

   if prioritylist == None:
      # transfer method list set by arizonaconfig
      prioritylist = arizonaconfig.get_option("transfermethod")

   # check the method list
   # if prioritylist is None, there's something wrong with configuration
   if prioritylist == None :
      arizonareport.send_syslog(arizonareport.ERR, "getfiles(): No transfer method was given.")
      return (False, downloaded_files)

   # create a temporary directory for the transfer
   arizonareport.send_out(4, "[DEBUG] getfiles creating temp dir")
   try:
      temp_dir = arizonaconfig.get_option("transfertempdir") + str(securerandom.SecureRandom().random())
   except TypeError:
      arizonareport.send_syslog(arizonareport.ERR, "getfiles(): No transfer temp dir is given.")
      return (False, downloaded_files)

   # in the case of destdir has '/' at the end
   # last '/' should go away to make result list(downloaded_files) match
   if len(destdir) > 1 and destdir.endswith('/'):
      destdir = destdir[:len(destdir) - 1]
   
   # if there are empty strings in the filelist, those will be taken away
   arizonareport.send_out(4, "[DEBUG] checking file list")
   filelist = __checkFileList(filelist)

   arizonareport.send_out(4, "[DEBUG] creating directories")
   for item in filelist:
      filename = item['filename']
      dirname = os.path.dirname(filename)
      if dirname != "":
         arizonageneral.makedirs_existok(os.path.join(temp_dir, dirname))
         arizonageneral.makedirs_existok(os.path.join(destdir, dirname))
   
   filenames = [item['filename'] for item in filelist]

   # keep the number of the list to compare how many files are downloaded at the end.
   numoflist = len(filelist)

   # if there is no file needing to be fetched
   if filelist == []:
      arizonareport.send_syslog(arizonareport.ERR, "getfiles(): No files needed to be downloaded.")
      return (False, downloaded_files)
  
   if not os.path.exists(temp_dir):
      arizonageneral.makedirs_existok(temp_dir)
   
   failures = 0
   for currFile in filelist:
      # attempt to get this file, with the data given
      common_attrs = {}
      
      # add attributes about this file that we know
      filename = currFile.get('filename',None)
      if filename == None or len(filename) == 0:
         # something's seriously wrong...
         continue
      
      if filename[0] != '/':
         filename = '/' + filename     # need to be an absolute path
         
      common_attrs[iftfile.JOB_ATTR_SRC_NAME] = "/" + str(host.split("/")[1]) + filename
      common_attrs[iftfile.JOB_ATTR_DEST_NAME] = temp_dir + filename
      common_attrs[iftfile.JOB_ATTR_FILE_SIZE] = currFile.get('size',None)
      common_attrs[iftfile.JOB_ATTR_FILE_HASH] = currFile.get('hash',None)
      common_attrs[iftfile.JOB_ATTR_SRC_HOST] = str(host.split("/")[0])
      common_attrs[iftfile.JOB_ATTR_DEST_HOST] = "localhost"
     
      job_attrs = copy.copy( common_attrs )
      job_attrs[iftfile.JOB_ATTR_REMOTE_IFTD] = False    # until we have a faster way of detecting it!
      connect_attrs = {}
      
      if prioritylist:
         job_attrs[iftfile.JOB_ATTR_PROTOS] = [proto + "_receiver" for proto in prioritylist]
         for proto in job_attrs[iftfile.JOB_ATTR_PROTOS]:
            connect_attrs[ proto ] = common_attrs

      __enable_timeout(__compute_timeout(filelist))
      
      # attempt to get it with iftd
      iftd_server = iftapi.make_XMLRPC_client() 
      try:
         # have iftd attempt to receive it
         rc = iftd_server.begin_ift( job_attrs, connect_attrs, False, True, 4001, "/RPC2", True, 3600 )
         print "XMLRPC call completed"
         if rc != iftproto.ifttransmit.TRANSMIT_STATE_SUCCESS:
            arizonareport.send_syslog( arizonareport.ERR, 'retrieve_files: File transfer RC=' + str(rc))
            print "XMLRPC call rc = " + str(rc)
            __disable_timeout()
            failures += 1
            continue
         else:
            print "got the file " + filename
            
      except Exception, inst:
         arizonareport.send_syslog( arizonareport.ERR, 'getfiles1(): XMLRPC error, could not receive file '  + filename )
         iftlog.exception("getfiles1(): XMLRPC error, could not receive file " + filename, inst)
         __disable_timeout()
         failures += 1
         continue
      
      
      """
      req = urllib2.Request("http://127.0.0.1:16387" + job_attrs.get( iftfile.JOB_ATTR_SRC_NAME ) )
      req.add_header( "Pragma", iftapi.pack_attrs( job_attrs, connect_attrs ) )
      resp = None
      try:
         # perform the request
         resp = urllib2.urlopen( req )
         if resp.code != 200:
            arizonareport.send_syslog(arizonareport.ERR, "getfiles(): HTTP " + str(resp.code) + " from IFTD for file " + filename)
            failures += 1
            __disable_timeout()
            continue
            
      except Exception, inst:
         arizonareport.send_out(3, "[" +username+"] "+str(element)+": error: "+ str( sys.exc_info()[0] ) )
         failures += 1
         __disable_timeout()
         continue
   
      """
      
      __disable_timeout()
      
      # write this file to the destination
      dest_filename = destdir + filename
      try:
         shutil.move( common_attrs[ iftfile.JOB_ATTR_DEST_NAME ], dest_filename )
         # success!
         downloaded_files.append( filename )
      except Exception, inst:
         arizonareport.send_out(3, "[" + username + "] " + str(element) + ": error: " + str(sys.exc_info()[0]) )
         failures += 1
         
   
   __close_transfer()
   shutil.rmtree(temp_dir)
   if failures == 0:
      return (True, downloaded_files)
   else:
      return (False, downloaded_files)
      




def sync_remote_dir(host, destdir, prog_indicator=0, metafile_signature_key=None, hashfuncs=[default_hashfunc], maskList=[]):
   """
   <Purpose>
      This synchronizes files between target directory in host and
      destination directory.

   <Arguments>
      host:
         'host' holds two things, a server name and target directory.
         For example, if you want to retrieve files from '/tmp/' directory
         in 'quadrus.cs.arizona.edu' server, the 'host' will be
         'quadrus.cs.arizona.edu/tmp'.  'host' should be a string.
         *** The host directory must contain a metafile ***

      destdir:
         'destdir' is a destination directory which will be synchronized.

      prog_indicator:
         If it is non-zero, this program will show a progress bar while
         downloading, with the given width. Default value is 0 (no
         indicator is shown).

      metafile_signature_key:
         The key that is expected to have signed the metafile for this repository.
         If None, then the metafile will not be required to be signed.

   <Exceptions>
      None.

   <Side Effects>
      None

   <Returns>
      A tuple: (result, grabbed_files, all_files)

      True or False to indicate success, a list of downloaded files, and a list
      of all files on the server.

      If the metafile_signature_key was provided but the signature is invalid
      or was not signed with this key, then no files will be downloaded and
      result will be False.
   """

   # check to see if we have an existing metafile that is within the cache
   # time limit. If we do, then do not bother to retrieve a new one.
   useCachedMetaFile = False
   metafile_path = os.path.join(destdir, METAFILE_FN)
   if os.path.exists(metafile_path):
      mtime = os.stat(metafile_path)[ST_MTIME]
      elapsed = abs(int(mtime - time.time()))
      if elapsed < arizonaconfig.get_option("metafilecachetime"):
         arizonareport.send_out(3, "Using cached metafile (" + str(elapsed) + ") seconds old")
         useCachedMetaFile = True

   if not useCachedMetaFile:
      # Fetch a metafile...
      # getfile will check that the validity of host, metafile, and destdir
      # if any of them is incorrect, return_value will be false
      if metafile_signature_key and (not arizonaconfig.get_option("disablesignedmetafile")):
         metafile_dict = {"filename": SIGNED_METAFILE_FN, "hashfuncs": hashfuncs}
         # when getting the metafile, only use http or ftp. enforce this by
         # passing a custom prioritylist to getfiles1.
         metaprioritylist = arizonaconfig.get_option("metatransfermethod")
         (return_value, file_list) = getfiles1(host, [metafile_dict], destdir, prog_indicator=prog_indicator, prioritylist=metaprioritylist)
         if not return_value:
            arizonareport.send_syslog(arizonareport.ERR, "sync_remote_dir(): Unable to retrieve " + SIGNED_METAFILE_FN + " from " + host)
            return (False, [], [])

         signedmetafile_path = os.path.join(destdir, SIGNED_METAFILE_FN)

         try:
             arizonareport.send_out(4, "rootsigcheck start")

             # verify signature in the sig file
             try:
                # SMB: ignoredisablexmlsigcheck is set to true, because we are
                # toggling this signature check with --disablesignedmetafile
                arizonacrypt.XML_validate_file(signedmetafile_path, None, publickey_string=metafile_signature_key, ignoredisablexmlsigcheck=True)
             except TypeError:
                arizonareport.send_out(1, "Invalid signature in " + SIGNED_METAFILE_FN + " from " + host)
                arizonareport.send_syslog(arizonareport.ERR, "sync_remote_dir(): Invalid signature in " + SIGNED_METAFILE_FN + " from " + host)
                return (False, [], [])

             arizonareport.send_out(4, "rootsigcheck stop")

             # extract the metafile
             try:
                metafile_tmp_fn = arizonacrypt.XML_retrieve_originalfile_from_signedfile(signedmetafile_path)
             except TypeError:
                arizonareport.send_out(1, "Unable to extract metafile from " + SIGNED_METAFILE_FN + " from " + host)
                arizonareport.send_syslog(arizonareport.ERR, "sync_remote_dir(): Unable to extract metafile from " + SIGNED_METAFILE_FN + " from " + host)
                return (False, [], [])

             f = file(metafile_tmp_fn, 'r')
             sig_file_contents = f.read()
             f.close

         except IOError, (errno, strerror):
             arizonareport.send_syslog(arizonareport.ERR, "sync_remote_dir(): I/O error(" + str(errno) + "): " + str(strerror))
             return (False, [], [])

         arizonareport.send_out(3, "[DEBUG] signed metafile validated from host " + host)

         shutil.copy(metafile_tmp_fn, metafile_path)

      else:
         arizonareport.send_out(2, "No metafile signature key, metafile signature not being checked for " + host)
         metafile_dict = {"filename": METAFILE_FN, "hashfuncs": hashfuncs}
         metaprioritylist = arizonaconfig.get_option("metatransfermethod")
         (return_value, file_list) = getfiles1(host, [metafile_dict], destdir, prog_indicator=prog_indicator, prioritylist=metaprioritylist)
         if not return_value:
            arizonareport.send_syslog(arizonareport.ERR, 'sync_remote_dir(): Error in retrieving metafile')
            return (False, [], [])

   (result, remote_list) = determine_remote_files(host, destdir, hashfuncs, maskList)
   if not result:
       return (result, [], [])

   fetch_list = []
   grabbed_files = []
   all_files = []
   for file in remote_list:
       if file['need_dl']:
           fetch_list.append(file)
       all_files.append(file['localfilename'])

   # if nothing needs to be downloaded
   if fetch_list == []:
      arizonareport.send_syslog(arizonareport.INFO, "\nNo files need to be fetched.")
   else :
      # get the files which needs to be downloaded
      #TODO pass the expected file sizes and limit the downloads by those sizes
      (return_value, grabbed_files) = getfiles1(host, fetch_list, destdir, prog_indicator, True)
      # fails to get files from host
      if not return_value and grabbed_files == []:
         arizonareport.send_syslog(arizonareport.ERR, "sync_remote_dir(): Failed to retrieve files.")
         return (False, grabbed_files, all_files)

   # if we retrieve every file needed
   if len(fetch_list) == len(grabbed_files):
      return (True, grabbed_files, all_files)
   # if we retrieve some of files
   else:
      arizonareport.send_syslog(arizonareport.ERR, "sync_remote_dir(): Failed to retrieve all files.")
      return (False, grabbed_files, all_files)




def determine_remote_files(name, destdir, hashfuncs=[default_hashfunc], maskList=[]):
   """
   <Purpose>
      Cracks open a metafile, determines the names of the files referenced
      from that metafile, and checks to make sure they are signed
      correctly.

      An unsigned metafile is assumed to exist at destdir/METAFILE_FN

   <Arguments>
      name:
         'name' of the remote thing we're synchronizing. The only purpose
         of this parameter is as text info for the user; It's suggested to
         use the same name as the 'host' parameter that is supplied to
         sync_remote_files(), but not absoletely necessary.

      destdir:
         'destdir' is a destination directory which will be synchronized.

   <Exceptions>
      None.

   <Side Effects>
      None

   <Returns>
      A tuple: (result, file_list)

      True or False to indicate success, a list of downloaded files, and a list
      of all files on the server.
   """

   metafile_path = os.path.join(destdir, METAFILE_FN)

   fetch_list = []

   if not os.path.exists(metafile_path):
       arizonareport.send_error(arizonareport.ERR, "determine_remote_files(): file " + str(metafile_path) + " does not exist")
       arizonareport.send_syslog(arizonareport.ERR, "determine_remote_files(): file " + str(metafile_path) + " does not exist")
       return (False, fetch_list)

   mtime = os.stat(metafile_path)[ST_MTIME]
   arizonareport.send_out(1, "Using metadata " + name + ", timestamp " + time.ctime(mtime))

   # Open the file we just retrieved
   arizonareport.send_out(4, "[DEBUG] opening " + metafile_path)
   try:
      dir_file = open(metafile_path)
   # if a file cannot be opened
   except IOError, (errno, strerror):
      arizonareport.send_error(arizonareport.ERR, "determine_remote_files(): I/O error(" + str(errno) + "): " + str(strerror))
      arizonareport.send_syslog(arizonareport.ERR, "determine_remote_files(): I/O error(" + str(errno) + "): " + str(strerror))
      return (False, fetch_list)

   # for each line in the metafile, check to make sure the local file is okay
   # each line has two string; the first one is filename, and second one is hash
   # go through every file and check if each file exist in the destdir
   # and the hash of files in the destdir match the hash from metafile
   # if it doesn't satisfy, then add the file to fetch_list to be retrieved
   for line in dir_file:
      # TWH: ignore blank lines
      if len(line.strip()) == 0:
         continue
      # Split the file's line into pieces
      line_dat = line.split()
      if len(line_dat) < 2:
         # invalid line in the meta file
         arizonareport.send_syslog(arizonareport.ERR, "sync_remote_dir(): The format of metafile is incorrect")
         return (False, fetch_list)

      # split a line into filename, filehash, and filesize
      filename = line_dat[0]
      expectedhash = line_dat[1].strip()
      if len(line_dat) >= 3:
          filesize = line_dat[2]
      else:
          filesize = None
      localfilename = os.path.join(destdir, filename)
      arizonareport.send_out(4, "[DEBUG] file: " + localfilename)
      arizonareport.send_out(4, "[DEBUG] expected hash: " + expectedhash)

      if maskList:
          matched = False
          for mask in maskList:
              if fnmatch.fnmatch(filename, mask):
                  matched = True
          if (not matched):
              continue
          
      status = None

      dict = {'filename': filename,
              'hash': expectedhash,
              'hashfuncs': hashfuncs,
              'need_dl': True,
              'localfilename': localfilename}

      # if this file has already been downloaded and checked, it will have
      # a filename.metahash file.. look for it
      if dict['need_dl']:
          if os.path.isfile(localfilename + ".metahash"):
             # open it and compare the hash
             f = open(localfilename + ".metahash")
             precomputedhash = f.read().strip()
             f.close()
             arizonareport.send_out(4, "[DEBUG] precomputed hash: " + precomputedhash)
             if precomputedhash == expectedhash:
                arizonareport.send_out(4, "[DEBUG] precomputed hash matched")
                # The hash matched so try the next file...
                dict["pre_hash_matched"] = True
                dict["need_dl"] = False
             else:
                dict["pre_hash_matched"] = False

      if dict['need_dl']:
          # if a file looking for is in the destination directory
          if os.path.isfile(localfilename):
             # and if it has the same hash
             # (we tell it to create a filename.metahash file for next time)
             actualhash = storkpackage.get_package_metadata_hash(localfilename, True)
             arizonareport.send_out(4, "[DEBUG] actual hash: " + actualhash)
             if actualhash == expectedhash:
                arizonareport.send_out(4, "[DEBUG] hash matched")
                # The hash matched so try the next file...
                dict["hash_matched"] = True
                dict["need_dl"] = False
             else:
                dict["hash_matched"] = False

      fetch_list.append(dict)

   return (True, fetch_list)






def __checkFileList(checklist):
   """
   <Purpose>
      This checks the given list and removes empty elements from the list.

   <Arguments>
      checklist:
         The list to be checked, should contain file tuples.

   <Exceptions>
      None.

   <Side Effects>
      None.

   <Returns>
      The list that empty elements are removed
   """

   checked_list = []
   for item in checklist:
      if (item != None) and ('filename' in item) and (item['filename']):
         checked_list.append(item)

   return checked_list





def __close_transfer() :
   """
   <Purpose>
      This closes the currently using transfer method
   
   <Arguments>
      None

   <Exceptions>
      None

   <Side Effects>
      set init_status as -1

   <Returns>
      None
   """

   global init_status  
   # if arizonafetch != None: 
   #    arizonafetch.close_transfer_program()
   init_status = -1




