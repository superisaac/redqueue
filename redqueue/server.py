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

from tornado import iostream
from tornado import ioloop

from redqueue.queue import QueueFactory, Queue, ReliableQueue

class MemcacheServer(object):
    def __init__(self, logdir, reliable='no'):
        self.queue_factory = QueueFactory(logdir)
        if reliable in ('yes', 'sync'):
            self.queue_factory.queue_class = ReliableQueue
            if reliable == 'sync':
                ReliableQueue.addlog = ReliableQueue.addlog_sync
        else:
            self.queue_factory.queue_class = Queue

    def handle_accept(self, fd, events):
        conn, addr = self._sock.accept()
        p = MemcacheProtocol(iostream.IOStream(conn))
        p.server = self

    def start(self, host, port):
        self.queue_factory.scan_logs()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._sock.setblocking(0)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(128)
        ioloop.IOLoop.instance().add_handler(self._sock.fileno(),
                                             self.handle_accept,
                                             ioloop.IOLoop.READ)
class MemcacheProtocol(object):
    def __init__(self, stream):
        self.protocol_id = str(id(self))
        self.stream = stream
        self.stream.set_close_callback(self._return_data)
        self.route = {
            'get': self.handle_get,
            'gets': self.handle_gets,
            'set': self.handle_set,
            'delete': self.handle_delete}
        self.wait_for_line()
        self.reservation = False
        self.resved_keys = set()

    def set_reservation(self, value):
        orig_reservation = self.reservation
        self.reservation = value in ('1', 'true')
        if orig_reservation != self.reservation:
            logging.info('Set reservation to be %s' % self.reservation)
            self.use_key()

    def use_key(self, key=None):
        """ Mark all reserved keys or the specified key as used """
        if key is None:
            for k in self.resved_keys:
                self.server.queue_factory.get_queue(k).use(self.protocol_id)
            self.resved_keys = set()
        elif key in self.resved_keys:
            self.server.queue_factory.get_queue(key).use(self.protocol_id)
            self.resved_keys.remove(key)

    def _return_data(self):
        for key in self.resved_keys:
            self.server.queue_factory.get_queue(key).give_back(self.protocol_id)
        self.resved_keys = set()

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
                self.server.queue_factory.get_queue(key).give(exptime, data)
            self.stream.write('STORED\r\n')
            self.wait_for_line()
        self.stream.read_bytes(bytes + 2, on_set_data)
        return True

    def _get_data(self, key):
        if self.reservation and (key in self.resved_keys):
            return None
        prot_id = None
        if self.reservation:
            prot_id = self.protocol_id
        q = self.server.queue_factory.get_queue(key, auto_create=False)
        t = None
        if q:
            if self.reservation:
                t = q.reserve(prot_id=prot_id)
            else:
                t = q.take()
        if t:
            if self.reservation:
                self.resved_keys.add(key)
            return t[1] # t is a tuple of (timeout, data)

    def handle_get(self, *keys):
        for key in keys:
            data  = self._get_data(key)
            if data:
                self.stream.write('VALUE %s 0 %d\r\n%s\r\n' % (key, len(data), data))
        self.stream.write('END\r\n')
        
    def handle_gets(self, *keys):
        """ Gets here is like a poll(), return the first non-empty queue
        number, so that a client can wait several queues.
        """
        for key in keys:
            data = self._get_data(key)
            if data:
                self.stream.write('VALUE %s 0 %d\r\n%s\r\n' % (key, len(data), data))
                break
        self.stream.write('END\r\n')
                
    def handle_delete(self, key, *args):
        if key in self.resved_keys:
            self.use_key(key)
            self.stream.write('DELETED\r\n')
        else:
            self.stream.write('NOT_DELETED\r\n')

Server = MemcacheServer
