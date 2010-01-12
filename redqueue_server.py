#!/usr/bin/python
####################################################################
#
# All of the deliverable code in REDQUEUE has been dedicated to the
# PUBLIC DOMAIN by the authors.
#
# Author: Zeng Ke  superisaac.ke at gmail dot com
#
####################################################################
import re, os, sys
import logging

from tornado import ioloop
import tornado.options
from tornado.options import define, options
from redqueue.server import Server
from redqueue import task

define('host', default="0.0.0.0", help="The binded ip host")
define('port', default=11211, type=int, help='The port to be listened')
define('jdir', default='journal', help='The directory to put journals')
define('reliable', default='yes', help='Store data to log files, options: (no, yes, sync)')
define('logfile', default='', help='Place where logging rows(info, debug, ...) are put.')

def main():
    tornado.options.parse_command_line()
    if options.logfile:
        logging.basicConfig(filename=options.logfile, level=logging.DEBUG)

    if not os.path.isdir(options.jdir):
        logging.error('Log directory %s does not exist.' % options.jdir)
        sys.exit(1)
    server = Server(options.jdir, options.reliable)
    server.start(options.host, options.port)
    task.run_all(server)
    ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()
