#!/usr/bin/env python

import os
import sys
import BaseHTTPServer

FILE_CHUNK_SIZE = 10

class chunked_handler( BaseHTTPServer.BaseHTTPRequestHandler ):
	def do_GET(self):
		try:
			if not os.path.exists( self.path ):
				print self.path + " does not exist"
				self.send_response(404)
				return
			
			self.send_response(200)
			print self.headers.getheader('Range')
			self.send_header('Content-type:', 'text/plain')
			self.send_header('Transfer-encoding:', 'chunked')
			self.end_headers()

			fsize = os.stat( self.path ).st_size
			
			print 'write: ' + str(fsize) + ';'

			self.wfile.write( str(fsize) + ';')		# full file size

			fd = open( self.path, 'r' )

			data = fd.read( FILE_CHUNK_SIZE )

			print 'write: ' + str(data)
			self.wfile.write( str(data) )

			offset = FILE_CHUNK_SIZE
			while offset < fsize:
				data = fd.read( FILE_CHUNK_SIZE )

				print 'write: ' + str(len(data))
				print 'write: ' + str(data)

				self.wfile.write( str(data) )
				self.wfile.write( str(len(data)) )
				offset = offset + FILE_CHUNK_SIZE

			fd.close()

			return
		except Exception, inst:
			print "Exception: " + str(inst)
			send_response(404)
			return


# serve up files
server = BaseHTTPServer.HTTPServer( ('', 4000), chunked_handler )
server.serve_forever()

