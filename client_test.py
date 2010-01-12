#!/usr/bin/python
####################################################################
#
# All of the deliverable code in REDQUEUE has been dedicated to the
# PUBLIC DOMAIN by the authors.
#
# Author: Zeng Ke  superisaac.ke at gmail dot com
#
####################################################################
import sys, os
import time
import logging
import memcache
logging.basicConfig(stream=sys.stdout)
def get_mc():
    mc = memcache.Client(['127.0.0.1:12345'])
    return mc
mc = get_mc()

def take(key):
    v = mc.get(key)
    if v is not None:
        mc.delete(key)
    return v
    
def clean_queue(key):
    mc.delete(key)
    while True:
        if take(key) is None:
            break

def test_queue():
    #clean_queue('abc/def')
    mc.set('abc/def', 'I')
    mc.set('abc/def', 'really')
    mc.set('abc/def', 'love')
    mc.set('abc/def', 'it')
    assert(take('abc/def') == 'I')
    assert(take('abc/def') == 'really')
    assert(take('abc/def') == 'love')
    assert(take('abc/def') == 'it')
    assert(take('abc/def') is None)
    print 'test queue ok'

def test_timeout():
    clean_queue('abc/def')
    mc.set('abc/def', 'I')
    mc.set('abc/def', 'really', 3) # time out is 3 seconds
    mc.set('abc/def', 'love')
    mc.set('abc/def', 'it')

    time.sleep(5)
    assert(take('abc/def') == 'I')
    assert(take('abc/def') == 'love')
    assert(take('abc/def') == 'it')
    assert(take('abc/def') is None)
    print 'test queue timeout ok'

def test_reservation():
    clean_queue('abc')
    clean_queue('def')
    mc.set('abc', 'I')
    mc.set('abc', 'really')
    mc.set('config:reserv', 1)
    assert(mc.get('abc') == 'I')
    assert(mc.get('abc') is None)
    mc.delete('abc')
    assert(take('abc') == 'really')
    print 'test reservation ok'

def test_reservation_close():
    global mc
    clean_queue('abc')
    mc.set('abc', 'I')
    mc.set('abc', 'love')
    assert(mc.get('abc') == 'I')
    mc.disconnect_all()

    mc = get_mc()
    assert(take('abc') == 'love')
    assert(mc.get('abc') == 'I')
    print 'test reservation on close ok'

def test_server_error():
    """
    use send argument first
    % python client_test.py send
    I
    then kill server and restart server
    % python client_test.py
    love
    % python client_test.py
    I
    % python client_test.py
    love
    ...
    """
    if sys.argv[1:] == ['send']:
        mc.set('xyz', 'I')
        mc.set('xyz', 'love')
        print mc.get('xyz')
    else:
        print mc.get('xyz')

def test_get_multi():
    clean_queue('abc')
    clean_queue('def')
    clean_queue('ghi')
    clean_queue('jkl')
    
    mc.set('def', 'I')
    mc.set('abc', 'love')
    mc.set('ghi', 'it')
    assert(mc.get('def') == 'I')
    #print mc.get_multi(['abc', 'def', 'ghi', 'jkl'])
    assert(mc.get_multi(['abc', 'def', 'ghi', 'jkl']) ==
           {'abc': 'love', 'ghi': 'it'})
    print 'test get multi ok'

def test_delete_multi():
    clean_queue('abc')
    clean_queue('def')
    clean_queue('ghi')
    clean_queue('jkl')

    mc.set('def', 'I')
    mc.set('abc', 'love')
    assert(mc.get('def') == 'I')
    mc.delete_multi(['abc', 'def', 'ghi', 'jkl'])
    assert(mc.get_multi(['abc', 'def', 'ghi', 'jkl']) ==
           {'abc': 'love'})
    
def test_performance():
    for _ in xrange(100):
        for i in xrange(100):
            mc.set('perf', i)
        for i in xrange(100):
            take('perf')

if __name__ == '__main__':
    test_queue()
    test_timeout()
    test_reservation()
    test_reservation_close()
    test_get_multi()
    test_delete_multi()
    test_server_error()
    #test_performance()
    

