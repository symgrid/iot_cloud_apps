
from __future__ import unicode_literals
import requests
from contextlib import closing
import logging
import json


class UserApi():
	def __init__(self, api_srv, auth_code):
		self.api_srv = api_srv
		self.auth_code = auth_code

	def create_get_session(self):
		session = requests.session()
		session.headers['AuthorizationCode'] = self.auth_code
		session.headers['Accept'] = 'application/json'
		return session

	def create_post_session(self):
		session = requests.session()
		#session.auth = (username, passwd)
		session.headers['AuthorizationCode'] = self.auth_code
		session.headers['Content-Type'] = 'application/json'
		session.headers['Accept'] = 'application/json'
		return session

	def list_user_apps(self):
		session = self.create_get_session()
		r = session.get(self.api_srv + "user_api.list_user_apps")
		if r.status_code != 200:
			logging.error(r.text)
			return None
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return msg.get("message")

	def list_devices(self):
		session = self.create_get_session()
		r = session.get(self.api_srv + "user_api.list_devices")
		if r.status_code != 200:
			logging.error(r.text)
			return None
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return msg.get("message")