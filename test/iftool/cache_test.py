#!/usr/bin/python

import urllib2

proxy_handler = urllib2.ProxyHandler( {'http': 'http://localhost:31128'} )
opener = urllib2.build_opener( proxy_handler )
cached_file_fd = opener.open( "http://localhost:18090/tmp/output" )
cached_file_fd.close()

import sys
sys.exit(0)
