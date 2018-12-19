
from __future__ import unicode_literals
import sys
import redis
import logging
from configparser import ConfigParser
from appmgr import AppMgr


console_out = logging.StreamHandler(sys.stdout)
console_out.setLevel(logging.DEBUG)
console_err = logging.StreamHandler(sys.stderr)
console_err.setLevel(logging.ERROR)
logging_handlers = [console_out, console_err]
logging_format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
logging_datefmt = '%a, %d %b %Y %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, format=logging_format, datefmt=logging_datefmt, handlers=logging_handlers)


config = ConfigParser()
config.read('../config.ini')


redis_srv = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')

redis_sts = redis.Redis.from_url(redis_srv + "/9", decode_responses=True) # device status (online or offline)

mgr = AppMgr(config)
mgr.start()
mgr.join()