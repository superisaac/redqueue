#!/usr/bin/python
####################################################################
#
# All of the deliverable code in REDQUEUE has been dedicated to the
# PUBLIC DOMAIN by the authors.
#
# Author: Zeng Ke  superisaac.ke at gmail dot com
#
####################################################################

import time
import memcache
mc = memcache.Client(['127.0.0.1:11211'])

def clean_cache():
    while True:
        if mc.get('abc/def') is None:
            break

def test_queue():
    clean_cache()
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
    clean_cache()
    mc.set('abc/def', 'I')
    mc.set('abc/def', 'really', 3) # time out is 3 seconds
    mc.set('abc/def', 'love')
    mc.set('abc/def', 'it')

    time.sleep(5)
    assert(mc.get('abc/def') == 'I')
    assert(mc.get('abc/def') == 'love')
    assert(mc.get('abc/def') == 'it')
    assert(mc.get('abc/def') is None)
    print 'test queue ok'

if __name__ == '__main__':
    test_queue()
    test_timeout()
        
    

