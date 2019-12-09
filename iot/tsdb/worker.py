import threading
import queue
import time
import logging
import json
import tsdb.client as tsdb


class Worker(threading.Thread):
	def __init__(self, *args, **kwargs):
		threading.Thread.__init__(self)
		client = tsdb.Client(*args, **kwargs)
		client.connect()
		client.create_database()
		self.client = client
		self.data_queue = queue.Queue(10240)
		self.task_queue = queue.Queue(1024)

	def run(self):
		dq = self.data_queue
		tq = self.task_queue
		client = self.client
		while True:
			time.sleep(0.5)
			# Get data points from data queue
			points = []
			while not dq.empty():
				points.append(dq.get())
				dq.task_done()

			# append points into task queue
			if len(points) > 0:
				if tq.full():
					tq.get()
					tq.task_done()
				tq.put(points)

			# process tasks queue
			while not tq.empty():
				points = tq.get()
				try:
					client.write_data(points)
					tq.task_done()
				except Exception as ex:
					logging.exception(ex)
					#tq.queue.appendleft(points) #TODO: Keep the points writing to influxdb continuely.

	def append_data(self, name, property, device, iot, timestamp, value, quality):
		self.data_queue.put({
			"name": name,
			"property": property,
			"device": device,
			"iot": iot,
			"timestamp": timestamp,
			"value": value,
			"quality": quality,
		})

	def append_event(self, device, iot, timestamp, event, quality):
		self.data_queue.put({
			'event_data': True,
			"name": "iot_device_event",
			"property": "event",
			"device": device,
			"iot": iot,
			"timestamp": timestamp,
			"value": json.dumps(event),
			"quality": quality,
			"level": event.get('level'),
			"type": event.get('type'),
		})

	def append_statistics(self, name, owner, iot, timestamp, fields):
		self.data_queue.put({
			'statistics': True,
			"name": name,
			"owner": owner,
			"iot": iot,
			"timestamp": timestamp,
			"fields": fields,
		})