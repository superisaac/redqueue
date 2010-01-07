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

LOG_CAPACITY = 1024 * 1024 # 1 mega bytes for each chunk

class Server(object):
    def __init__(self, logdir):
        self.queue_collection = {}
        self.logdir = logdir

    def get_queue(self, key, auto_create=True):
        if key not in self.queue_collection and auto_create:
            self.queue_collection[key] = Queue(key)
        return self.queue_collection.get(key)

    def handle_accept(self, fd, events):
        conn, addr = self._sock.accept()
        p = Protocol(iostream.IOStream(conn))

    def start(self, host, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(128)
        ioloop.IOLoop.instance().add_handler(self._sock.fileno(),
                                             self.handle_accept,
                                             ioloop.IOLoop.READ)
    def scan_logs(self):
        logging.info('Sanning logs ...')
        queue_fns = {}
        for fn in os.listdir(self.logdir):
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
            queue.load_from_log(os.path.join(self.logdir,
                                             '%s-%d.log' % (key, tm)))
            self.queue_collection[ukey] = queue
        logging.info('Redqueue is ready to serve.')

server = None
#TODO: binary log
class Queue(object):
    def __init__(self, key):
        self.key = key
        self._queue = deque()
        self.log = None
        self.rotate_log()
        self.borrowing = {}

    def addlog(self, w):
        os.write(self.log.fileno(), w)
        self.log.flush()
        os.fsync(self.log.fileno())
        
    def rotate_log(self):
        if self.log:
            self.log.close()
        fn = os.path.join(server.logdir,
                          '%s-%d.log' % (urllib.quote_plus(self.key),
                                         time.time()))
        self.log = open(fn, 'ab')

    def return_(self, prot_id):
        timeout, data = self.borrowing.pop(prot_id)        
        self._queue.appendleft((timeout, data))
        self.addlog('R %s\r\n' % prot_id)

        if len(self._queue) > 128:
            logging.warn('queue size(%s) for key %s is too big' %
                         (len(self._queue), self.key))


    def enqueue(self, timeout, data):
        self._queue.appendleft((timeout, data))

        self.addlog('S %d %d\r\n%s\r\n' % (timeout,
                                           len(data), data))
        if len(self._queue) > 128:
            logging.warn('queue size(%s) for key %s is too big' %
                         (len(self._queue), self.key))

    def use(self, prot_id):
        if prot_id in self.borrowing:
            self.addlog("U %s\r\n" % prot_id)
            del self.borrowing[prot_id]
        
    def dequeue(self, prot_id=None):
        while True:
            try:
                timeout, data = self._queue.pop()
            except IndexError:
                return None
            if prot_id is None:
                self.addlog('G\r\n')
            else:
                self.addlog('B %s\r\n' % prot_id)

            if (len(self._queue) == 0 and
                len(self.borrowing) == 0 and
                self.log.tell() >= LOG_CAPACITY):
                self.rotate_log()
            if timeout > 0 and timeout < time.time():
                continue
            if prot_id:
                assert prot_id not in self.borrowing
                self.borrowing[prot_id] = (timeout, data)
            return timeout, data

    def load_from_log(self, logpath):
        logfile = open(logpath, 'rb')
        while True:
            line = logfile.readline()
            if not line:
                break
            if line.startswith('B'):
                _, prot_id = line.split()
                try:
                    data = self._queue.pop()
                    self.borrowing[prot_id] = data
                except IndexError:
                    logging.error('Pop from empty stack')
            elif line.startswith('U'):
                _, prot_id = line.split()
                assert prot_id in self.borrowing
                del self.borrowing[prot_id]
            elif line.startswith('R'):
                _, prot_id = line.split()
                assert prot_id in self.borrowing
                t = self.borrowing.pop(prot_id)
                self._queue.appendleft(t)
            elif line.startswith('G'):
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

        for t in self.borrowing.itervalues():
            # The order is not important
            self._queue.append(t)
        self.borrowing = {}
        logfile.close()

class Protocol(object):
    def __init__(self, stream):
        self.protocol_id = str(id(self))
        self.stream = stream
        self.stream.set_close_callback(self._return_data)
        self.route = {
            'get': self.handle_get,
            'set': self.handle_set,
            'delete': self.handle_delete}
        self.wait_for_line()
        self.reservation = False
        self.reserved_key = None

    def set_reservation(self, value):
        orig_reservation = self.reservation
        self.reservation = value in ('1', 'true')
        if orig_reservation != self.reservation:
            logging.info('Set reservation to be %s' % self.reservation)
            self.use_key()
        
    def use_key(self):
        if self.reserved_key:
            server.get_queue(self.reserved_key).use(self.protocol_id)
            self.reserved_key = None

    def _return_data(self):
        if self.reserved_key:
            server.get_queue(self.reserved_key).return_(self.protocol_id)
            self.reserved_key = None

    def wait_for_line(self):
        self.stream.read_until('\r\n', self.line_received)

    def line_received(self, line):
        args = line.split()
        data_required = self.route.get(args[0].lower(),
                                       self.handle_unknown)(*args[1:])
        if not data_required:
            self.wait_for_line()

    def handle_unknown(self, *args):
        self.stream.write("CLIENT_ERROR bad command line format\r\n")

    def handle_set(self, key, flags, exptime, bytes, *args):
        bytes = int(bytes)
        exptime = int(exptime)
        if exptime > 0:
            exptime = time.time() + exptime

        def on_set_data(data):
            data = data[:-2]
            if key == 'config:reserv':
                self.set_reservation(data)
            else:
                server.get_queue(key).enqueue(exptime, data)

            self.stream.write('STORED\r\n')
            self.wait_for_line()

        self.stream.read_bytes(bytes + 2, on_set_data)
        return True

    def handle_get(self, key, *args):
        if self.reservation and self.reserved_key:
            self.stream.write('END\r\n')
            return
        
        prot_id = None
        if self.reservation:
            prot_id = self.protocol_id
        q = server.get_queue(key)
        t = q and q.dequeue(prot_id=prot_id) or None
        if t:
            timeout, data = t
            if self.reservation:
                self.reserved_key = key
            self.stream.write('VALUE %s 0 %d\r\n%s\r\n' % (key, len(data), data))
        self.stream.write('END\r\n')

    def handle_delete(self, key, *args):
        if key == self.reserved_key:
            self.use_key()
            self.stream.write('DELETED\r\n')
        else:
            self.stream.write('NOT_DELETED\r\n')

if __name__ == '__main__':
    tornado.options.parse_command_line()
    if not os.path.isdir(options.logdir):
        logging.error('Log directory %s does not exits' % options.logdir)
        sys.exit(1)
    server = Server(options.logdir)
    server.scan_logs()
    server.start(options.host, options.port)
    ioloop.IOLoop.instance().start()
