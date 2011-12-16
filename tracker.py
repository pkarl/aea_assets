"""
    AEA Assets - tracker.py
    
    Pete Karl II - Oct 24, 2011
    
    The tracker is responsible for collecting tweets with links, which we'll
    run through embedly, if we haven't already.
    
    I've made username + password + embedly key into command line params because I don't
    want to deal with config files right now.

"""

import sys, pycurl, urllib, urllib2, redis
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

ACCEPTED_ARGS = ['maxwidth', 'maxheight', 'format']

def get_oembed(url, **kwargs):
    """
    Example Embedly oEmbed Function
    """
    api_url = 'http://api.embed.ly/1/oembed?'

    params = {'url': url , 'key': sys.argv[3] }

    for key, value in kwargs.items():
        if key not in ACCEPTED_ARGS:
            raise ValueError("Invalid Argument %s" % key)
        params[key] = value

    oembed_call = "%s%s" % (api_url, urllib.urlencode(params))

    return json.loads(urllib2.urlopen(oembed_call).read())

def attach_nozzle(api, callback, args):
    conn = pycurl.Curl()
    conn.setopt(pycurl.USERPWD, "%s:%s" % (sys.argv[1], sys.argv[2]))
    conn.setopt(pycurl.URL, "https://stream.twitter.com/1/statuses/%s.json" % api)
    
    conn.setopt(pycurl.WRITEFUNCTION, callback)

    data = urllib.urlencode(args)
    conn.setopt(pycurl.POSTFIELDS, data)

    conn.perform()

def hose(data):
    try:
        tweet = json.loads(data)
        
        for url in tweet['entities']['urls']:
            
            if r.hexists('urls', url['expanded_url']):
                continue
            try:
                obj = get_oembed(url['expanded_url'])
            except urllib2.HTTPError:
                continue
            
            r.sadd('url_index', "%s:%s" % (r.get('current_index'), url['expanded_url']))
            r.hset('urls', url['expanded_url'], obj)
            r.incr('current_index')
                
    except json.decoder.JSONDecodeError:
        pass


"""
    Initialize the app
"""

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.flushdb()
if not r.exists('current_index'):
    r.set('current_index', 0)
    
e = Embedly(sys.argv[3])

args = { 'track': 'imgur,500px,flic.kr,#photo,#pic,yfrog' }

attach_nozzle('filter', hose, args)