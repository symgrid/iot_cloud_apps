
from __future__ import unicode_literals
import re
import os
import json
import redis
import logging
from collections import deque
from configparser import ConfigParser
from tsdb.worker import Worker as TSDBWorker
from statistics.worker import Worker as StatisticsWorker


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S')

config = ConfigParser()
config.read('../config.ini')

redis_srv = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')
api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot.api.user"

redis_sts = redis.Redis.from_url(redis_srv + "/9") # device status (online or offline)
redis_cloud = redis.Redis.from_url(redis_srv + "/14") # Cloud configuration

def create_tsdb_worker(db):
	host = config.get('influxdb', 'host', fallback='127.0.0.1')
	port = config.getint('influxdb', 'port', fallback=8086)
	username = config.get('influxdb', 'username', fallback='root')
	password = config.get('influxdb', 'password', fallback='root')
	return TSDBWorker(datbase=db, host=host, port=port, username=username, password=password)