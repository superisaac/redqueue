import random
import time
import logging
try:
    import json
except ImportError:
    import simplejson as json
    
from tornado import ioloop
from tornado.httpclient import AsyncHTTPClient

class Task(object):
    """ A task runner that periodically check a key to fetch and
    execute new tasks"""    
    key = None
    def __init__(self, server):
        self.server = server
        self.queue_factory = server.queue_factory
        self.prot_id = 'task:%s' % id(self)

    def watch(self):
        assert self.key, 'Task key must be set'
        self.server.watchers[self.key] = self

    def unwatch(self):
        watcher = self.server.watchers.get(self.key)
        if watcher == self:
            del self.server.watchers[self.key]

    def check(self):
        q = self.queue_factory.get_queue(self.key)
        t = q.take(self.prot_id)
        if t is not None:
            _, data = t
            if data:
                data = json.loads(data)
            try:
                self.on_data(data)
            except Exception, e:
                logging.error('Task(%s) error %s' % (self.__class__.__name__, e),
                              exc_info=True)

    def on_data(self, data):
        """ The callback when a task with data comes."""
        raise NotImplemented

class URLFetchTask(Task):
    """ Fetch an url, currently only HTTP scheme is supported.
    """
    key = 'task:url'
    def on_data(self, data):
        if isinstance(data, basestring):
            url = data
            delay = 0
        else: # data should be a dictionary
            url = data['url']
            delay = data.get('delay', 0)

        def handle_response(response):
            if response.error:
                logging.error('Error %s while fetch url %s' % (response.error,
                                                               url))
            else:
                logging.info('URL %s fetched.' % url)

        def fetch_url(url):
            logging.info('Fetching url %s' % url)
            http_client = AsyncHTTPClient()
            http_client.fetch(url, handle_response)

        if delay:
            ioloop.IOLoop.instance().add_timeout(time.time() + delay,
                                                 lambda: fetch_url(url))
        else:
            fetch_url(url)


runnable_tasks = [URLFetchTask]

def run_all(server):
    for task_cls in runnable_tasks:
        task = task_cls(server)
        task.watch()

