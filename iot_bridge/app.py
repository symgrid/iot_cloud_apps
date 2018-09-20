
from __future__ import unicode_literals
import redis
import logging
from configparser import ConfigParser
from appmgr import AppMgr


logging.basicConfig(level=logging.DEBUG,
					format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
					datefmt='%a, %d %b %Y %H:%M:%S')

config = ConfigParser()
config.read('../config.ini')


redis_srv = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')

redis_sts = redis.Redis.from_url(redis_srv + "/9", decode_responses=True) # device status (online or offline)

mgr = AppMgr(config)
mgr.start()
mgr.join()