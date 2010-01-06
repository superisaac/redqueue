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
import socket
import logging
import time
import urllib
from collections import deque

from tornado import iostream
from tornado import ioloop
import tornado.options
from tornado.options import define, options

define('host', default="0.0.0.0", help="The binded ip host")
define('port', default=11211, type=int, help='The port to be listened')
define('logdir', default='log', help='the directory to put logs')

LOG_CAPACITY = 1024 * 1024 * 10  # 10 mega bytes for each chunk

#TODO: binary log
class Queue(object):
    def __init__(self, key):
        self.key = key
        self._queue = deque()
        self.log = None
        self.rotate_log()

    def rotate_log(self):
        if self.log:
            self.log.close()
        self.log = open(os.path.join(options.logdir,
                                     '%s-%d.log' % (urllib.quote_plus(self.key),
                                                    time.time())), 'ab')

    def enqueue(self, timeout, data):
        self._queue.appendleft((timeout, data))
        self.log.write('S %d %d\r\n%s\r\n' % (timeout,
                                              len(data), data))
        self.log.flush()

        if len(self._queue) > 128:
            logging.warn('queue size(%s) for key %s is too big' %
                         (len(self._queue), self.key))
            
    def dequeue(self):
        while True:
            try:
                timeout, data = self._queue.pop()
            except IndexError:
                return None
            self.log.write('G\r\n')
            self.log.flush()
            if (len(self._queue) == 0 and
                self.log.tell() >= LOG_CAPACITY):
                self.rotate_log()
            if timeout > 0 and timeout < time.time():
                continue
            return data

    def load_from_log(self, logpath):
        logfile = open(logpath, 'rb')
        while True:
            line = logfile.readline()
            if not line:
                break
            if line.startswith('G'):
                try:
                    self._queue.pop()
                except IndexError:
                    logging.error('Pop from empty stack')
            elif line.startswith('S'):
                t, timeout, lendata = line.split()
                data = logfile.read(int(lendata))
                logfile.read(2) # line break
                self._queue.appendleft((int(timeout),
                                        data))
            else:
                logging.error('Bad format for log file %s' % logpath)
        logfile.close()

class Protocol(object):
    def __init__(self, stream):
        self.stream = stream
        self.route = {
            'get': self.handle_get,
            'set': self.handle_set}
        self.wait_for_line()

    def wait_for_line(self):
        self.stream.read_until('\r\n', self.line_received)

    def line_received(self, line):
        args = line.split()
        data_required = self.route.get(args[0].lower(),
                                       self.handle_unknown)(*args[1:])
        if not data_required:
            self.wait_for_line()

    def handle_unknown(self, **args):
        self.stream.write("CLIENT_ERROR bad command line format\r\n")

    def handle_set(self, key, flags, exptime, bytes, *args):
        bytes = int(bytes)
        exptime = int(exptime)
        if exptime > 0:
            exptime = time.time() + exptime

        def on_set_data(data):
            data = data[:-2]
            self.server.get_queue(key).enqueue(exptime, data)
            self.stream.write('STORED\r\n')
            self.wait_for_line()
        self.stream.read_bytes(bytes + 2, on_set_data)
        return True

    def handle_get(self, key, *args):
        data = self.server.get_queue(key).dequeue()
        if data:
            self.stream.write('VALUE %s 0 %d\r\n%s\r\n' % (key, len(data), data))
        self.stream.write('END\r\n')

class Server(object):
    def __init__(self):
        self.queue_collection = {}

    def get_queue(self, key):
        if key not in self.queue_collection:
            self.queue_collection[key] = Queue(key)
        return self.queue_collection[key]

    def handle_accept(self, fd, events):
        conn, addr = self._sock.accept()
        p = Protocol(iostream.IOStream(conn))
        p.server = self

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        self._sock.bind((options.host, options.port))
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.listen(128)
        ioloop.IOLoop.instance().add_handler(self._sock.fileno(),
                                             self.handle_accept,
                                             ioloop.IOLoop.READ)
    def scan_logs(self):
        logging.info('Sanning logs ...')
        queue_fns = {}
        for fn in os.listdir(options.logdir):
            m = re.search(r'-(\d+).log$', fn)
            if m:
                key = fn[:m.start()]
                tm = int(m.group(1))
                if (key not in queue_fns or
                    tm > queue_fns[key]):
                    queue_fns[key] = tm

        for key, tm in queue_fns.iteritems():
            ukey = urllib.unquote_plus(key)
            logging.info('Restoring queue %s ...' % ukey)
            queue = Queue(ukey)
            queue.load_from_log(os.path.join(options.logdir,
                                             '%s-%d.log' % (key, tm)))
            self.queue_collection[ukey] = queue
        logging.info('Redqueue is ready to serve.')

if __name__ == '__main__':
    tornado.options.parse_command_line()
    if not os.path.isdir(options.logdir):
        logging.error('Log directory %s does not exits' % options.logdir)
        sys.exit(1)
    server = Server()
    server.scan_logs()
    server.start()
    ioloop.IOLoop.instance().start()
