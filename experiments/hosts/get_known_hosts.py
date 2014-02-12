#!/usr/bin/python

import sys
import os

hosts_file = sys.argv[1]
slice_name = hosts_file.split(".")[0]
hosts_file_fd = open( hosts_file, "r" )
expect_file = "/tmp/ssh.expect"

success_file = slice_name + ".success"
fail_file = slice_name + ".fail"

success_fd = open(success_file, "wt" )
fail_fd = open(fail_file, "wt" )

for host in hosts_file_fd:
   try:
      os.remove( expect_file )
   except:
      pass
   expect_fd = open( expect_file, "wt" )
   expect_fd.write( "spawn ssh " + slice_name + "@" + host + "\n" )
   expect_fd.write( "set timeout 60\n" )
   expect_fd.write( "expect {\n" )
   expect_fd.write( '   "Are you sure you want to continue connecting (yes/no)? " { send ' + r'"yes\r";' + ' exp_continue }\n' )
   expect_fd.write( r'   "\[' + slice_name + "@" + host.split('.')[0] + r' ~\]$" { send ' + r'"exit\r"' + '; exit 0 }\n' )
   expect_fd.write( '   "ssh: connect to host ' + host.strip("\n") + ' port 22: Connection timed out" { exit 1 }' )
   expect_fd.write( '}\n' )
   expect_fd.close()

   rc = os.popen("expect " + expect_file).close()

   if rc == None or rc == 0:
      success_fd.write( host )

   else:
      fail_fd.write( host )


success_fd.close()
fail_fd.close()
