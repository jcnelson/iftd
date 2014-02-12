#!/usr/bin/python

import sys
import os
import time

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

class http11_request_handler( BaseHTTPRequestHandler ):

	def do_GET(self):
		print "GET " + self.path

		if os.path.exists( self.path ):
			self.send_response(200)
			self.send_header( 'Content-type', 'application/octet-stream' )

			self.send_header( 'Last-Modified', time.ctime( os.stat( self.path ).st_mtime) )
			self.end_headers()

			fd = open( self.path )
			lines = fd.readlines()
			fd.close()

			for line in lines:
				self.wfile.write( line )

			return
		else:
			print "Nope" 
			self.send_response(404)



http_server = HTTPServer( ('', 18090), http11_request_handler )
os.chdir( "/" )
http_server.serve_forever()

