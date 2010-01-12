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
    key = ''
    def __init__(self, factory):
        self.timeout = 0.02
        self.queue_factory = factory
        self.prot_id = 'task:%s' % id(self)

    def check(self):
        q = self.queue_factory.get_queue(self.key)
        t = q.take(self.prot_id)
        if t is None:
            # time out increase with the null result
            self.timeout *= 1.5
            if self.timeout > 5:
                self.timeout = 5
        else:
            _, data = t
            if data:
                data = json.loads(data)
            try:
                self.on_data(data)
            except Exception, e:
                logging.error('Task(%s) error %s' % (self.__class__.__name__, e),
                              exc_info=True)
            self.timeout = 0.02
        ioloop.IOLoop.instance().add_timeout(time.time() + self.timeout,
                                             self.check)
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
        else: # data should be a dictionary
            url = data['url']
        
        def handle_response(response):
            if response.error:
                logging.error('Error %s while fetch url %s' % (response.error,
                                                               url))
            else:
                logging.info("URL %s fetched." % url)
        http_client = AsyncHTTPClient()
        http_client.fetch(url, handle_response)

runnable_tasks = [URLFetchTask]

def run_all(server):
    for task_cls in runnable_tasks:
        task = task_cls(server.queue_factory)
        ioloop.IOLoop.instance().add_timeout(time.time() + 2 * random.random(),
                                             task.check)
 
