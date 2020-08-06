
import requests
import logging
from utils import _dict


class CloudApi:
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

		try:
			r = session.get(self.api_srv + ".list_statistics_companies")
			if r.status_code != 200:
				logging.warning(r.text)
				return

			msg = _dict(r.json())
			if not msg or not msg.message:
				logging.warning('Result is not json!!')
				return

			return msg.message
		except Exception as ex:
			logging.exception(ex)
			return

