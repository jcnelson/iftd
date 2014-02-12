#!/usr/bin/env python
##############################################################################
# TODO
# - have a list of allowable trackers; if torrent's announce url isnt in
#    one of those lists, dont add torrent to incoming dir
##############################################################################

from sys import *
from os.path import *
from sha import *
from BitTornado.bencode import *
import statvfs
import os
from shutil import *

import cookielib
import urllib2
from urllib import unquote_plus
import re
from common import *
import getopt

useSymlinks = False
#useSymlinks = True
## make a symlink from the torrent to the incoming dir?
## if false, copies torrent to incoming dir
cookieFile = os.path.join(os.environ["HOME"], ".btrss", "cookies.txt" )
forceDownload = False

print

# setup args
try:
    opts, args = getopt.getopt(argv[1:], 'f', ['force', 'cookie='])
except getopt.GetoptError:
    print '%s file1.torrent [--force/-f] [--cookie=path] fileN.torrent' % argv[0]
    exit(2)

for opt,arg in opts:
    if opt in ("-f", "--force"):
        forceDownload = True
    if opt == "--cookie":
        cookieFile =    os.path.expandvars(os.path.expanduser(arg))

if len(args) == 0:
    print '%s file1.torrent file2.torrent file3.torrent ...' % argv[0]
    exit(2)

## setup http stuffs
cj = cookielib.MozillaCookieJar()
if os.path.exists( cookieFile ):
    cj.load( cookieFile )
opener = urllib2.build_opener( urllib2.HTTPCookieProcessor( cj ) )
urllib2.install_opener( opener )
headers = {'User-Agent' : 'Mozilla/4.0 (compatible; python-based client; talk to liekomglol@gmail.com if this is problematic)'}
## end http stuffs

## get the filesize here so each torrent can take off what it needs
## in terms of disk; will give a more accurate picture of the available space
st = os.statvfs(os.getcwd())
totalSpace = st[statvfs.F_BLOCKS] * st[statvfs.F_FRSIZE]
#freeSpace = st[statvfs.F_BAVAIL] * st[statvfs.F_FRSIZE]
freeSpace = st[statvfs.F_BFREE] * st[statvfs.F_FRSIZE]
## take off 12%; dont let disk get above 88% full
freeSpace -= (totalSpace * PERCENT_KEEP_FREE)
    
for metainfo_name in args:
    if metainfo_name.startswith( 'http://' ) or metainfo_name.startswith( 'https://' ):
        # make an http request
        try:
            req = urllib2.Request(metainfo_name, None, headers)
            f = urllib2.urlopen(req)
        except urllib2.HttpError, e:
            print 'HTTP Error: %d' % e.code
            continue # skip this torrent
        except urllib2.URLError, e:
            print 'Network Error: %s' % e.reason.args[1]
            continue # skip this torrent
        if f.info().has_key('content-disposition'):
            filestr = escapeFilename(f.info()['content-disposition'].replace("inline; filename=\"",'').replace('"','')).replace('attachment__filename_','')
        else:
            filestr = basename(metainfo_name)
            pos = filestr.find('&')
            if pos > -1:
                eqpos = filestr.find('=',pos)+1 # drop =
                filestr = filestr[eqpos:]
            filestr = escapeFilename(unquote_plus(filestr))
        # write out to a local file
        of = open( filestr, "wb" )
        of.write( f.read() )
        of.close()
        metainfo_name = filestr

    # owners have to match
    if not isFileOwnerCurrentUser( metainfo_name ):
        print 'You do not own torrent \'%s\'' % metainfo_name
        continue

    metainfo_file = open(metainfo_name, 'rb')
    metainfo = bdecode(metainfo_file.read())
    metainfo_file.close()
    info = metainfo['info']
    info_hash = sha( bencode( info    ) ).hexdigest()

    # allowed tracker?
    if not isTrackerAllowed( metainfo['announce'] ):
        print 'Tracker for \'%s\' is not allowed.' % info['name']
        continue

    ## make sure we havent downloaded this already
    if checkDownloadStatus( info ) and not forceDownload:
        print "A torrent with the hash %s has already been downloaded." % info_hash
    else:    
        if info.has_key('length'):
            fileSize = info['length']
        else:
            fileSize = 0;
            for file in info['files']:
                fileSize += file['length']
    
        # filesize > disk free-10%?
        if fileSize > freeSpace:
            print "Sorry, not enough space for torrent %s!" % metainfo_name
        else:
            freeSpace -= fileSize
    
            # figure out what to name the torrent
            torrentName = ( os.path.join( INCOMING_TORRENT_DIR, info_hash ) + '.torrent' )
    
            if exists( torrentName ):
                print 'A torrent the signature %s is already being downloaded' % info_hash
            else:
                # log the torrent info
                if useSymlinks:
                    os.symlink( os.path.abspath(metainfo_name), torrentName )
                    os.chmod( metainfo_name, 0640 )
                else:
                    copy( metainfo_name, torrentName )
                    os.chmod( torrentName, 0640 )

                recordActiveTorrent( metainfo_name, info['name'], info_hash )
                print 'Will begin downloading %s shortly.' % metainfo_name 
