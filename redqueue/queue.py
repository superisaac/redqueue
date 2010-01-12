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

JOURNAL_CAPACITY = 1 #024 * 1024 # 1 mega bytes for each chunk

#TODO: binary log
class Queue(object):
    def __init__(self, key):
        self.key = key
        self._queue = deque()
        self._jfile = None
        self._lent = {}

    def addjournal(self, w):
        pass
    def rotate_journal(self):
        pass

    def give_back(self, prot_id):
        """ Give the elememt borrowed by prot_id back for future calling"""
        timeout, data = self._lent.pop(prot_id)        
        self._queue.appendleft((timeout, data))
        self.addjournal('R %s\r\n' % prot_id)

    def give(self, timeout, data):
        self._queue.appendleft((timeout, data))
        self.addjournal('S %d %d\r\n%s\r\n' % (timeout,
                                           len(data), data))

    def use(self, prot_id):
        """ Mark the element borrowed by prot_id used
        """
        if prot_id in self._lent:
            self.addjournal("U %s\r\n" % prot_id)
            del self._lent[prot_id]
        self.rotate_journal()

    def reserve(self, prot_id):
        """ Reserve an element by prot_id and return it later, or the
        server will recycle it"""
        while True:
            try:
                timeout, data = self._queue.pop()
            except IndexError:
                return None
            self.addjournal('B %s\r\n' % prot_id)

            if timeout > 0 and timeout < time.time():
                continue
            assert prot_id not in self._lent
            self._lent[prot_id] = (timeout, data)
            return timeout, data

    def take(self, prot_id):
        t = self.reserve(prot_id)
        if t is not None:
            self.use(prot_id)
        return t

    def load_from_journal(self, jpath):
        jfile = open(jpath, 'rb')
        lent = {}
        while True:
            line = jfile.readline()
            if not line:
                break
            if line.startswith('B'): # Borrow an item
                _, prot_id = line.split()
                try:
                    data = self._queue.pop()
                    lent[prot_id] = data
                except IndexError:
                    logging.error('Pop from empty stack')
            elif line.startswith('U'):  # Use an item
                _, prot_id = line.split()
                assert prot_id in lent
                del lent[prot_id]
            elif line.startswith('R'):  # Return an item
                _, prot_id = line.split()
                assert prot_id in lent
                t = lent.pop(prot_id)
                self._queue.appendleft(t)
            elif line.startswith('S'):
                t, timeout, lendata = line.split()
                data = jfile.read(int(lendata))
                jfile.read(2) # line break
                self._queue.appendleft((int(timeout),
                                        data))
            else:
                journalging.error('Bad format for journal file %s' % jpath)

        for t in lent.itervalues():
            self._queue.appendleft(t)
        self._lent = {}
        jfile.close()

class ReliableQueue(Queue):
    def addjournal(self, w):
        os.write(self._jfile.fileno(), w)
        self._jfile.flush()

    def addjournal_sync(self, w):
        os.write(self._jfile.fileno(), w)
        self._jfile.flush()
        #os.fdatasync(self._jfile.fileno())
        os.fsync(self._jfile.fileno())
        
    def _journal_file_name(self):
        return os.path.join(self.server.jdir,
                            '%s.log' % urllib.quote_plus(self.key))
    def rotate_journal(self):
        if self._jfile is None:
            self._jfile = open(self._journal_file_name(), 'ab')
        elif (len(self._queue) == 0 and
              len(self._lent) == 0 and
              self._jfile.tell() >= JOURNAL_CAPACITY):
            self._jfile.close()
            curr_journal_fn = self._journal_file_name()
            journal_fn = '%s.%d' % (curr_journal_fn, time.time())
            os.rename(curr_journal_fn, journal_fn)
            logging.info('rotate journal to %s' % journal_fn)
            self._jfile = open(curr_journal_fn, 'ab')

class QueueFactory(object):
    queue_class = Queue
    def __init__(self, jdir):
        self.queue_collection = {}
        self.jdir = jdir

    def get_queue(self, key, auto_create=True):
        if key not in self.queue_collection and auto_create:
            q = self.queue_class(key)
            q.server = self
            q.rotate_journal()
            self.queue_collection[key] = q            
        return self.queue_collection.get(key)

    def scan_journals(self):
        logging.info('Sanning journals ...')
        for fn in os.listdir(self.jdir):
            m = re.search(r'\.log$', fn)
            if m:
                key = fn[:m.start()]
                ukey = urllib.unquote_plus(key)
                logging.info('Restoring queue %s ...' % ukey)
                queue = self.get_queue(ukey)
                queue.load_from_journal(os.path.join(self.jdir,
                                                 '%s.log' % key))
        logging.info('Redqueue is ready to serve.')
