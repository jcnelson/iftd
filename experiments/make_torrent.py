#!/usr/bin/python

import sys
import os

file = sys.argv[1]
output = sys.argv[2]

import libtorrent as lt

fs = lt.file_storage()
lt.add_files( fs, file )

ct = lt.create_torrent( fs, 5000 )

#ct.add_tracker("http://tracker.openbittorrent.com/announce", 0)
#ct.add_tracker("udp://tracker.openbittorrent.com:80/announce", 0)
ct.add_tracker("http://tracker.publicbt.com/announce", 0)
ct.add_tracker("udp://tracker.publicbt.com:80/announce", 0)

ct.set_creator("iftd")

lt.set_piece_hashes( ct, os.path.dirname(file))

bt_str = lt.bencode( ct.generate() )

fd = open(output, "wb" )
fd.write( bt_str )
fd.close() 
