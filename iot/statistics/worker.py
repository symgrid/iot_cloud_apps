import threading
import queue
import requests
import json
import logging
import datetime


class TaskBase:
	def run(self, woker):
		logging.debug("TaskBase")


class Worker(threading.Thread):
	def __init__(self, api_srv, auth_code):
		threading.Thread.__init__(self)
		self.queue = queue.Queue()
		self.thread_stop = False
		self.api_srv = api_srv
		self.auth_code = auth_code

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

	def create_session(self):
		session = requests.session()
		#session.auth = (username, passwd)
		session.headers['AuthorizationCode'] = self.auth_code
		session.headers['Content-Type'] = 'application/json'
		session.headers['Accept'] = 'application/json'
		return session

	def get_server_url(self):
		return self.api_srv

	def create_dss_task(self, *args, **kwargs):
		'''
		Create Device Status Statistics Task
		'''
		self.add(DeviceStatusStatistics(*args, **kwargs))

	def device_event(self, *args, **kwargs):
		self.add(DeviceEvent(*args, **kwargs))


class DeviceStatusStatistics(TaskBase):
	def __init__(self, auth_code, tsdb_worker):
		self.auth_code = auth_code
		self.tsdb_worker = tsdb_worker

	def run(self, worker):
		session = worker.create_session()

		r = session.post(worker.get_server_url() + ".add_device", data=json.dumps(dev))
		if r.status_code != 200:
			logging.warning(r.text)


DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S.%f"
DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT


class DeviceEvent(TaskBase):
	def __init__(self, auth_code, sn, event):
		self.auth_code = auth_code
		self.sn = sn
		self.event = event

	def run(self, worker):
		session = worker.create_session()

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