import threading
import queue
import requests
import json
import logging
import datetime
import time
from utils import _dict


class Query:
	def __init__(self, api_srv, auth_code):
		self.api_srv = api_srv
		self.auth_code = auth_code

	def create_post_session(self):
		session = requests.session()
		#session.auth = (username, passwd)
		session.headers['AuthorizationCode'] = self.auth_code
		session.headers['Content-Type'] = 'application/json'
		session.headers['Accept'] = 'application/json'
		return session

	def create_get_session(self):
		session = requests.session()
		#session.auth = (username, passwd)
		session.headers['AuthorizationCode'] = self.auth_code
		session.headers['Accept'] = 'application/json'
		return session

	def list_companies(self):
		session = self.create_get_session()

		r = session.get(self.api_srv + ".list_statistics_companies")
		if r.status_code != 200:
			logging.warning(r.text)
			return

		msg = _dict(r.json())
		if not msg or not msg.message:
			logging.warning('Result is not json!!')
			return

		return msg.message

	def list_forwards(self, type=None):
		session = self.create_get_session()

		url = self.api_srv + ".list_statistics_companies"
		if type:
			url = url + "?type=" + type

		r = session.get(url)
		if r.status_code != 200:
			logging.warning(r.text)
			return

		msg = _dict(r.json())
		if not msg or not msg.message:
			logging.warning('Result is not json!!')
			return

		return msg.message

