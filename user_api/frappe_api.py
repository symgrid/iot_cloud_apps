import requests
from contextlib import closing
import logging
import json


class FrappeApi():
	def __init__(self, config):
		self.thread_stop = False
		self.api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot.user_api"

	def create_get_request(self, auth_code):
		session = requests.session()
		session.headers['AuthorizationCode'] = auth_code
		session.headers['Accept'] = 'application/json'
		return session

	def create_post_session(self, auth_code):
		session = requests.session()
		#session.auth = (username, passwd)
		session.headers['AuthorizationCode'] = auth_code
		session.headers['Content-Type'] = 'application/json'
		session.headers['Accept'] = 'application/json'
		return session

	def get_user(self, auth_code):
		session = self.create_get_request(auth_code)
		r = session.get(self.api_srv + ".get_user")
		if r.status_code != 200:
			logging.error(r.text)
			return None
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return msg.get("message")

	def get_device(self, auth_code, device_sn):
		session = self.create_get_request(auth_code)
		r = session.get(self.api_srv + ".get_device?sn=" + device_sn)
		if r.status_code != 200:
			logging.error(r.text)
			return None
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return msg.get("message")

	def proxy_get(self, auth_code, url, params=None, data=None):
		session = self.create_get_request(auth_code)

		with closing(
				session.get(self.api_srv + url, params=params, data=data)
		) as resp:
			resp_headers = []
			for name, value in resp.headers.items():
				if name.lower() in ('content-length', 'connection',
									'content-encoding'):
					continue
				resp_headers.append((name, value))
			return json.dumps({
				"content":resp.content,
				"status_code": resp.status_code,
				"headers": resp_headers})

	def proxy_post(self, auth_code, url, params=None, data=None):
		session = self.create_get_request(auth_code)

		with closing(
				session.post(self.api_srv + url, params=params, data=data)
		) as resp:
			resp_headers = []
			for name, value in resp.headers.items():
				if name.lower() in ('content-length', 'connection',
									'content-encoding'):
					continue
				resp_headers.append((name, value))
			return json.dumps({
				"content":resp.content,
				"status_code": resp.status_code,
				"headers": resp_headers})


