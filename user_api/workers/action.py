
import threading
import queue
import json
import logging
import time


class ActionBase:
	def doAction(self):
		return True

	def isDone(self):
		return False

	def isTimeout(self):
		return False


class Worker(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.action_queue = queue.Queue()
		self.wait_list = []
		self.thread_stop = False

	def doAction(self):
		actions = self.action_queue
		waits = self.wait_list
		while not actions.empty():
			try:
				task = actions.get()
				try:
					if task.doAction():
						waits.append(task)
				except Exception as ex:
					logging.exception(ex)
					# Continue
				actions.task_done()
			except queue.Empty:
				logging.error("This is empty Exeption!")
				break

	def doAction(self):
		waits = self.wait_list
		next_waits = []
		for i in waits:
			try:
				task = waits[i]
				try:
					if not task.isDone():
						next_waits.append(task)
				except Exception as ex:
					logging.exception(ex)
					# Continue
			except queue.Empty:
				logging.error("This is empty Exeption!")
				break
		self.wait_list = next_waits

	def run(self):
		while not self.thread_stop:
			time.sleep(0.2)
			self.doAction()
			self.doWait()

	def stop(self):
		self.thread_stop = True

	def add(self, task):
		self.queue.put(task)

	def send_output(self, *args, **kwargs):
		self.add(OutputAction(*args, **kwargs))

	def send_command(self, *args, **kwargs):
		self.add(CommandAction(*args, **kwargs))


class OutputAction(ActionBase):
	def __init__(self, ws_client, ws_server, ws_id, auth_code, frappe_api, data, timeout=5):
		self.ws_client = ws_client
		self.ws_server = ws_server
		self.ws_id = ws_id
		self.auth_code = auth_code
		self.frappe_api = frappe_api
		self.data = data
		self.timeout = time.time() + timeout

	def doAction(self):
		logging.warning("Send Output to device", json.dumps(self.data))
		action_id = self.frappe_api.send_output(self.auth_code, self.data)
		if not action_id:
			return False
		self.action_id = action_id
		return True

	def isDone(self):
		'''
		result_example = {
			"message": "Done",
			"timestamp_str": "Wed Aug 29 09:39:08 2018",
			"result": True,
			"timestamp": 1535535548.28,
			"device": "000C296CBED3",
			"id": "605063B4-AB6F-11E8-8C76-00163E06DD4A"
		}
		'''
		r = self.frappe_api.action_result(self.action_id)
		if not r:
			return False
		logging.info("Got action result: " + json.dumps(r))
		if r.id != self.action_id:
			return False

		try:
			self.ws_server.send_message(self.ws_client, json.dumps({
				"id": self.ws_id,
				"code": 'send_output',
				"data": r
			}))
		except Exception as ex:
			logging.exception(ex)

		return True

	def isTimeout(self):
		return time.time() > self.timeout


class CommandAction(ActionBase):
	def __init__(self, ws_client, ws_server, ws_id, auth_code, frappe_api, data, timeout=5):
		self.ws_client = ws_client
		self.ws_server = ws_server
		self.ws_id = ws_id
		self.auth_code = auth_code
		self.frappe_api = frappe_api
		self.data = data
		self.timeout = time.time() + timeout

	def doAction(self):
		logging.warning("Send Command to device", json.dumps(self.data))
		action_id = self.frappe_api.send_command(self.auth_code, self.data)
		if not action_id:
			return False
		self.action_id = action_id
		return True

	def isDone(self):
		'''
		result_example = {
			"message": "Done",
			"timestamp_str": "Wed Aug 29 09:39:08 2018",
			"result": True,
			"timestamp": 1535535548.28,
			"device": "000C296CBED3",
			"id": "605063B4-AB6F-11E8-8C76-00163E06DD4A"
		}
		'''
		r = self.frappe_api.action_result(self.action_id)
		if not r:
			return False
		logging.info("Got action result: " + json.dumps(r))
		if r.id != self.action_id:
			return False

		try:
			self.ws_server.send_message(self.ws_client, json.dumps({
				"id": self.ws_id,
				"code": 'send_output',
				"data": r
			}))
		except Exception as ex:
			logging.exception(ex)

		return True

	def isTimeout(self):
		return time.time() > self.timeout