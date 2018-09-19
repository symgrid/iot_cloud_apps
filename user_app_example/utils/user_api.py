import requests
from contextlib import closing
import logging
import json


class UserApi():
	def __init__(self, config):
		self.thread_stop = False
		self.api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot."

	def create_get_session(self, auth_code):
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
		session = self.create_get_session(auth_code)
		r = session.get(self.api_srv + "user_api.get_user")
		if r.status_code != 200:
			logging.error(r.text)
			return None
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return msg.get("message")

	def access_device(self, auth_code, device_sn):
		session = self.create_get_session(auth_code)
		r = session.get(self.api_srv + "user_api.access_device?sn=" + device_sn)
		if r.status_code != 200:
			logging.error(r.text)
			return None
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return msg.get("message")

	def proxy_get(self, auth_code, url, params=None, data=None):
		session = self.create_get_session(auth_code)
		if data:
			data = json.dumps(data)

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
		session = self.create_post_session(auth_code)
		if data:
			data = json.dumps(data)

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

	def send_output(self, auth_code, data):
		session = self.create_post_session(auth_code)
		r = session.post(self.api_srv + "device_api.send_output", data=json.dumps(data))
		if r.status_code != 200:
			logging.error(r.text)
			return False, r.text
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return True, msg.get("message")

	def send_command(self, auth_code, data):
		session = self.create_post_session(auth_code)
		r = session.post(self.api_srv + "device_api.send_command", data=json.dumps(data))
		if r.status_code != 200:
			logging.error(r.text)
			return False, r.text
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return True, msg.get("message")

	def action_result(self, auth_code, id):
		session = self.create_get_session(auth_code)
		r = session.get(self.api_srv + "device_api.get_action_result", params={"id": id})
		if r.status_code != 200:
			logging.error(r.text)
			return False, r.text
		msg = r.json()
		# logging.debug('%s\t%s\t%s', str(time.time()), msg)
		return True, msg.get("message")

