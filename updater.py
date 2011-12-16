"""
    This spits out urls + info
"""

import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)

print r.smembers('url_index')