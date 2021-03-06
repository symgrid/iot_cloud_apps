
from __future__ import unicode_literals
import sys
import redis
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from configparser import ConfigParser
from tsdb.worker import Worker as TSDBWorker
from statistics.worker import Worker as StatisticsWorker
from utils import _dict
from utils.cloud_api import CloudApi as CloudApi


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
api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot.user_api"
auth_code = config.get('iot', 'auth_code', fallback='1234567890')


redis_sts = redis.Redis.from_url(redis_srv + "/9", decode_responses=True) # device status (online or offline)
redis_statistics = redis.Redis.from_url(redis_srv + "/15", decode_responses=True) # Cloud statistics result


cloud_api = CloudApi(api_srv, auth_code)
statistics_workers = {}
tsdb_worker = {}


def create_tsdb_worker(db):
	worker = tsdb_worker.get(db)
	if not worker:
		host = config.get('influxdb', 'host', fallback='127.0.0.1')
		port = config.getint('influxdb', 'port', fallback=8086)
		username = config.get('influxdb', 'username', fallback='root')
		password = config.get('influxdb', 'password', fallback='root')
		worker = TSDBWorker(database=db + '.statistics', host=host, port=port, username=username, password=password)
		worker.start()
		tsdb_worker[db] = worker
	return worker


def create_tsdb_client(db):
	host = config.get('influxdb', 'host', fallback='127.0.0.1')
	port = config.getint('influxdb', 'port', fallback=8086)
	username = config.get('influxdb', 'username', fallback='root')
	password = config.get('influxdb', 'password', fallback='root')
	from tsdb.client import Client
	client = Client(host, port, username, password, db)
	client.connect()
	return client


def create_statistics_worker(company):
	worker = statistics_workers.get(company)
	if not worker:
		worker = StatisticsWorker()
		worker.start()
		statistics_workers[company] = worker
	return worker


def run_statistics_tasks():
	logging.debug("run_statistics_tasks")
	cloud_statistics = []
	companines = cloud_api.list_companies()
	if not companines:
		logging.warning("Companies is None")
		return

	for comp in companines:
		comp = _dict(comp)
		if comp.enable is not None and (comp.enable is True or int(comp.enable) != 0):
			cloud_statistics.append(comp)

	for value in cloud_statistics:
		logging.debug("run_statistics_tasks for company %s", value.company)
		worker = create_statistics_worker(value.company)
		tsdb = create_tsdb_worker(value.database)
		# worker.create_dss_task(tsdb, redis_sts, api_srv, val.company, val.auth_code)
		worker.create_dts_task(redis_statistics, api_srv, value.company, value.auth_code)
		worker.create_dets_task(tsdb, create_tsdb_client(value.database), api_srv, value.company, value.auth_code)
		worker.create_des_task(create_tsdb_client(value.database), redis_statistics, api_srv, value.company, value.auth_code)
		worker.create_dscs_task(tsdb, create_tsdb_client(value.database), api_srv, value.company, value.auth_code)


run_statistics_tasks()

scheduler = BlockingScheduler()

five_min = CronTrigger(minute='0/5')
scheduler.add_job(run_statistics_tasks, five_min, name="IOT Run Statistics Tasks")

scheduler.start()

