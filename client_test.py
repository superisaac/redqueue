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
import memcache
def get_mc():
    mc = memcache.Client(['127.0.0.1:12345'])
    return mc
mc = get_mc()

def clean_cache(key):
    while True:
        if mc.get(key) is None:
            break

def test_queue():
    clean_cache('abc/def')
    mc.set('abc/def', 'I')
    mc.set('abc/def', 'really')
    mc.set('abc/def', 'love')
    mc.set('abc/def', 'it')

    assert(mc.get('abc/def') == 'I')
    assert(mc.get('abc/def') == 'really')
    assert(mc.get('abc/def') == 'love')
    assert(mc.get('abc/def') == 'it')
    assert(mc.get('abc/def') is None)
    print 'test queue ok'

def test_timeout():
    clean_cache('abc/def')
    mc.set('abc/def', 'I')
    mc.set('abc/def', 'really', 3) # time out is 3 seconds
    mc.set('abc/def', 'love')
    mc.set('abc/def', 'it')

    time.sleep(5)
    assert(mc.get('abc/def') == 'I')
    assert(mc.get('abc/def') == 'love')
    assert(mc.get('abc/def') == 'it')
    assert(mc.get('abc/def') is None)
    print 'test queue timeout ok'

def test_reservation():
    clean_cache('abc')
    clean_cache('def')
    mc.set('abc', 'I')
    mc.set('abc', 'really')
    mc.set('config:reserv', 1)
    assert(mc.get('abc') == 'I')
    assert(mc.get('abc') is None)
    mc.delete('abc')
    mc.set('config:reserv', 0)
    assert(mc.get('abc') == 'really')
    print 'test reservation ok'

def test_reservation_close():
    global mc
    clean_cache('abc')
    mc.set('abc', 'I')
    mc.set('abc', 'love')
    mc.set('config:reserv', 1)
    assert(mc.get('abc') == 'I')
    mc.disconnect_all()

    mc = get_mc()
    assert(mc.get('abc') == 'love')
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
    None    
    """
    if sys.argv[1:] == ['send']:
        mc.set('xyz', 'I')
        mc.set('xyz', 'love')
        mc.set('config:reserv', 1)
        print mc.get('xyz')
    else:
        print mc.get('xyz')

def test_get_multi():
    clean_cache('abc')
    clean_cache('def')
    clean_cache('ghi')
    clean_cache('jkl')
    
    mc.set('def', 'I')
    mc.set('abc', 'love')
    mc.set('ghi', 'it')
    assert(mc.get('def') == 'I')
    assert(mc.get_multi(['abc', 'def', 'ghi', 'jkl']) ==
           {'abc': 'love', 'ghi': 'it'})
    print 'test get multi ok'

    
def test_performance():
    for _ in xrange(100):
        for i in xrange(100):
            mc.set('perf', i)
        for i in xrange(100):
            mc.get('perf')

if __name__ == '__main__':
    test_queue()
    test_timeout()
    test_reservation()
    test_reservation_close()
    test_get_multi()
    test_server_error()
    test_performance()
    

