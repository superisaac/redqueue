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
import time
import urllib
from collections import deque

LOG_CAPACITY = 1024 * 1024 # 1 mega bytes for each chunk

#TODO: binary log
class Queue(object):
    def __init__(self, key):
        self.key = key
        self._queue = deque()
        self.log = None
        self.borrowing = {}

    def addlog(self, w):
        pass
    def rotate_log(self):
        pass

    def give_back(self, prot_id):
        """ Give the elememt borrowed by prot_id back for future calling"""
        timeout, data = self.borrowing.pop(prot_id)        
        self._queue.appendleft((timeout, data))
        self.addlog('R %s\r\n' % prot_id)

    def give(self, timeout, data):
        self._queue.appendleft((timeout, data))
        self.addlog('S %d %d\r\n%s\r\n' % (timeout,
                                           len(data), data))

    def use(self, prot_id):
        """ Mark the element borrowed by prot_id used
        """
        if prot_id in self.borrowing:
            self.addlog("U %s\r\n" % prot_id)
            del self.borrowing[prot_id]
        
    def reserve(self, prot_id):
        """ Reserve an element by prot_id and return it later, or the
        server will recycle it"""
        while True:
            try:
                timeout, data = self._queue.pop()
            except IndexError:
                return None
            self.addlog('B %s\r\n' % prot_id)

            self.rotate_log()
            if timeout > 0 and timeout < time.time():
                continue
            assert prot_id not in self.borrowing
            self.borrowing[prot_id] = (timeout, data)
            return timeout, data

    def take(self, prot_id):
        t = self.reserve(prot_id)
        if t is not None:
            self.use(prot_id)
        return t

    def load_from_log(self, logpath):
        logfile = open(logpath, 'rb')
        while True:
            line = logfile.readline()
            if not line:
                break
            if line.startswith('B'): # Borrow an item
                _, prot_id = line.split()
                try:
                    data = self._queue.pop()
                    self.borrowing[prot_id] = data
                except IndexError:
                    logging.error('Pop from empty stack')
            elif line.startswith('U'):  # Use an item
                _, prot_id = line.split()
                assert prot_id in self.borrowing
                del self.borrowing[prot_id]
            elif line.startswith('R'):  # Return an item
                _, prot_id = line.split()
                assert prot_id in self.borrowing
                t = self.borrowing.pop(prot_id)
                self._queue.appendleft(t)
            elif line.startswith('S'):
                t, timeout, lendata = line.split()
                data = logfile.read(int(lendata))
                logfile.read(2) # line break
                self._queue.appendleft((int(timeout),
                                        data))
            else:
                logging.error('Bad format for log file %s' % logpath)

        for t in self.borrowing.itervalues():
            self._queue.append(t)
        self.borrowing = {}
        logfile.close()

class ReliableQueue(Queue):
    def addlog(self, w):
        os.write(self.log.fileno(), w)
        self.log.flush()

    def addlog_sync(self, w):
        os.write(self.log.fileno(), w)
        self.log.flush()
        #os.fdatasync(self.log.fileno())
        os.fsync(self.log.fileno())
        
    def rotate_log(self):
        if self.log is None or (len(self._queue) == 0 and
                                len(self.borrowing) == 0 and
                                self.log.tell() >= LOG_CAPACITY):
            if self.log:
                self.log.close()
            fn = os.path.join(self.server.logdir,
                              '%s-%d.log' % (urllib.quote_plus(self.key),
                                             time.time()))
            logging.info('rotate log to %s' % fn)
            self.log = open(fn, 'ab')
        
class QueueFactory(object):
    queue_class = Queue
    def __init__(self, logdir):
        self.queue_collection = {}
        self.logdir = logdir

    def get_queue(self, key, auto_create=True):
        if key not in self.queue_collection and auto_create:
            q = self.queue_class(key)
            q.server = self
            q.rotate_log()
            self.queue_collection[key] = q            
        return self.queue_collection.get(key)

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
            queue = self.get_queue(ukey)
            queue.load_from_log(os.path.join(self.logdir,
                                             '%s-%d.log' % (key, tm)))
        logging.info('Redqueue is ready to serve.')
