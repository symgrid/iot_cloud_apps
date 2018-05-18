import threading
import queue
import requests
import json
import logging
import datetime
import time
from utils import _dict


class TaskBase:
	def run(self, woker):
		logging.debug("TaskBase")

	def create_post_session(self, auth_code):
		session = requests.session()
		#session.auth = (username, passwd)
		session.headers['AuthorizationCode'] = auth_code
		session.headers['Content-Type'] = 'application/json'
		session.headers['Accept'] = 'application/json'
		return session

	def create_get_session(self, auth_code):
		session = requests.session()
		#session.auth = (username, passwd)
		session.headers['AuthorizationCode'] = auth_code
		session.headers['Accept'] = 'application/json'
		return session



class Worker(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.queue = queue.Queue()
		self.thread_stop = False

	def run(self):
		q = self.queue
		while not self.thread_stop:
			try:
				task = q.get()
				task.run()
				q.task_done()
			except queue.Empty:
				logging.error("This is empty Exeption!")
				break

	def stop(self):
		self.thread_stop = True

	def add(self, task):
		self.queue.put(task)

	def create_dss_task(self, *args, **kwargs):
		'''
		Create Device Status Statistics Task
		'''
		self.add(DeviceStatusStatistics(*args, **kwargs))

	def create_des_task(self, *args, **kwargs):
		self.add(DeviceEventStatistics(*args, **kwargs))

	def device_event(self, *args, **kwargs):
		self.add(DeviceEvent(*args, **kwargs))


class DeviceStatusStatistics(TaskBase):
	def __init__(self, tsdb_worker, redis_sts, api_srv, owner, auth_code):
		self.tsdb_worker = tsdb_worker
		self.redis_sts = redis_sts
		self.api_srv = api_srv
		self.owner = owner,
		self.auth_code = auth_code

	def run(self):
		session = self.create_get_session(self.auth_code)

		r = session.get(self.api_srv + ".list_devices")
		if r.status_code != 200:
			logging.warning(r.text)
			return
		msg = _dict(r.json())
		if not msg or not msg.message.get('company_devices'):
			logging.warning('Result is not json!!')
			return

		online_count = 0
		offline_count = 0
		company_devices = msg.message.get('company_devices')
		for group in company_devices:
			group = _dict(group)
			for i in range(0, len(group.devices), 64):
				devs = group.devices[i:i+64]
				sts = self.redis_sts.mget(devs)
				for status in sts:
					if status == 'ONLINE':
						online_count = online_count + 1
					else:
						offline_count = offline_count + 1
		self.tsdb_worker.append_statistics('device_status_statistics', self.owner, time.time(), {
			'online': online_count,
			'offline': offline_count
		})



DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S.%f"
DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT

class DeviceEventStatistics(TaskBase):
	def __init__(self, tsdb_client, redis_statistics, api_srv, owner, auth_code):
		self.tsdb_client = tsdb_client
		self.redis_statistics = redis_statistics
		self.api_srv = api_srv
		self.owner = owner,
		self.auth_code = auth_code
		self.time = time.time()

	def run(self):
		start_time = (int(self.time / (60 * 60 * 24)) * 60 * 60 * 24) - ( 30 * 24 * 60 * 60)
		start_time = datetime.date.fromtimestamp(start_time).strftime(DATETIME_FORMAT)
		# end_time = datetime.datetime.fromtimestamp(int(self.time / (60 * 5)) * 60 * 5).strftime(DATETIME_FORMAT)
		end_time = datetime.datetime.fromtimestamp(self.time).strftime(DATETIME_FORMAT)
		session = self.create_get_session(self.auth_code)

		r = session.get(self.api_srv + ".list_devices")
		if r.status_code != 200:
			logging.warning(r.text)
			return
		msg = _dict(r.json())
		if not msg or not msg.message.get('company_devices'):
			logging.warning('Result is not json!!')
			return

		company_devices = msg.message.get('company_devices')
		for group in company_devices:
			group = _dict(group)
			for dev in group.devices:
				val = self.tsdb_client.query_event_count(dev, start_time, end_time, '1d')
				self.redis_statistics.set(dev, json.dumps(val))


class DeviceEvent(TaskBase):
	def __init__(self, sn, event):
		self.sn = sn
		self.event = event

	def run(self, worker):
		session = worker.create_post_session()

		event = json.loads(self.event)
		timestamp = datetime.datetime.utcfromtimestamp(event[2]).strftime(DATETIME_FORMAT)

		data= json.dumps({
			"device": self.sn,
			"source": event[0],
			"level": event[1].get("level") or 0,
			"type": event[1].get("type") or "EVENT",
			"info": event[1].get("info") or "EVENT INFO",
			"data": json.dumps(event[1].get("data")),
			"time": timestamp,
			"wechat_notify": 1,
		})

		r = session.post(worker.get_server_url() + ".add_device_event", data=data)
		if r.status_code != 200:
			logging.warning(r.text)