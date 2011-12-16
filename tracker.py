"""
    AEA Assets - tracker.py
    
    Pete Karl II - Oct 24, 2011
    
    The tracker is responsible for collecting tweets with links, which we'll
    run through embedly, if we haven't already.
    
    I've made username + password + embedly key into command line params because I don't
    want to deal with config files right now.
    
    1) change index format
    3) add 'reach' in the form of a list of {'username' : follower_count } dicts
    
"""

import sys
import pycurl
import urllib
import redis
from embedly import Embedly
import simplejson as json

"""
    Absorb & handle args for the script
"""
if len(sys.argv) is not 4:
    sys.exit("usage: $ python tracker.py twitter_username twitter_password embedly_key")


"""
    Handle nozzling, storage + Embedly
"""

def attach_nozzle(callback, args):
    conn = pycurl.Curl()
    data = urllib.urlencode(args)
    
    conn.setopt(pycurl.USERPWD, "%s:%s" % (sys.argv[1], sys.argv[2]))
    conn.setopt(pycurl.URL, "https://stream.twitter.com/1/statuses/filter.json")
    conn.setopt(pycurl.WRITEFUNCTION, callback)
    conn.setopt(pycurl.POSTFIELDS, data)

    try:
        conn.perform()
        print 'attached'
    except pycurl.error, error:
        errno, errstr = error
        print 'ERROR -> 0: ', errstr
    except Exception, err:
        sys.stderr.write('ERROR -> 1: %s\n' % str(err))

def hose(data):
    try:
        tweet = json.loads(data)
    except json.decoder.JSONDecodeError:
        return
        
    for url in tweet['entities']['urls']:
        
        if r.hexists('urls', url['expanded_url']):
            continue
        
        print e.preview(url['expanded_url'], raw=True).raw
        
        # r.sadd('url_index', "%s:%s" % (r.get('current_index'), url['expanded_url']))
        # r.hset('urls', url['expanded_url'], obj)
        # r.incr('current_index')

"""
    Initialize the app
"""

print 'Connecting to Redis... ',

# create Redis connection
r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.flushdb()
if not r.exists('current_index'):
    r.set('current_index', 0)

print 'connected'

print 'Creating Embedly client... ',
e = Embedly(sys.argv[3])
print 'created'

print 'Testing Embedly endpoint... ',
obj = e.preview('http://google.com/')
if obj.error:
    exit('embedly endpoint failed (http response %s)' % obj.error_code)
print 'passed'

print 'Attaching nozzle to Twitter firehose... '
attach_nozzle(hose, { 'track': 'instagr' })