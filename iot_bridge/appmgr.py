'''
App manager
'''

from __future__ import unicode_literals
import threading
import logging
import time
import os
from utils import _dict
from utils.user_api import UserApi
from workers.mqtt import MQTTBridge


class AppMgr(threading.Thread):
	def __init__(self, config):
		threading.Thread.__init__(self)
		self.thread_stop = False
		self.config = config
		self.api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot."
		self.auth_code = config.get('iot', 'auth_code', fallback='1234567890')
		self.user_api = UserApi(self.api_srv, self.auth_code)
		self.apps = _dict({})

	def run(self):
		try:
			while not self.thread_stop:
				self.update_apps()
				time.sleep(60)
		except Exception as ex:
			logging.exception(ex)
			os._exit(1)

	def list_mqtt_apps(self):
		applist = self.user_api.list_user_apps() or {}
		apps = _dict({})
		for app in applist:
			app = _dict(app)
			if app.device_data == 1 and app.device_data_mqtt_host:
				user = app.device_data_mqtt_username
				if user and len(user) == 0:
					user = None
				password = app.device_data_mqtt_password
				if password and len(password) == 0:
					password = None
				apps[app.name] = _dict({
					'name': app.name,
					'host': app.device_data_mqtt_host,
					'user': user,
					'password': password,
					'modified': app.modified,
					'auth_code': app.auth_code
				})
		return apps

	def update_apps(self):
		logging.info("=== Update MQTT bridge list ===")
		apps = self.list_mqtt_apps()
		new_apps_set = set(apps.keys())
		cur_apps_set = set(self.apps.keys())

		remove_set = cur_apps_set.difference(new_apps_set)
		add_set = new_apps_set.difference(cur_apps_set)
		update_set = cur_apps_set.intersection(new_apps_set)

		self.remove_apps(remove_set)
		self.add_apps({k:apps[k] for k in add_set})
		self.try_update_apps({k:apps[k] for k in update_set})

	def add_apps(self, apps):
		for k in apps.keys():
			logging.info("=== Create MQTT bridge %s ===", k)
			self.apps[k] = apps[k]
			self.apps[k].bridge = MQTTBridge(self.config, apps[k])
			self.apps[k].bridge.start()

	def remove_apps(self, apps):
		for k in apps:
			logging.info("=== Remove MQTT bridge %s ===", k)
			self.apps[k].bridge.stop()
			self.apps.pop(k)

	def try_update_apps(self, apps):
		for k in apps.keys():
			if self.apps[k].modified != apps[k].modified:
				logging.info("=== Update MQTT bridge %s ===", k)

				self.apps[k].bridge.stop()
				self.apps.pop(k)

				self.apps[k] = apps[k]
				self.apps[k].bridge = MQTTBridge(self.config, apps[k])
				self.apps[k].bridge.start()
