
from __future__ import unicode_literals
import sys
import redis
import logging
import json
from configparser import ConfigParser
from websocket_server import WebsocketServer

from workers.sub import SubClient
from workers.action import Worker as ActionWorker
from utils.rtdata import RTData
from utils.user_api import UserApi


console_out = logging.StreamHandler(sys.stdout)
console_out.setLevel(logging.DEBUG)
console_err = logging.StreamHandler(sys.stderr)
console_err.setLevel(logging.ERROR)
logging_handlers = [console_out, console_err]
logging_format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
logging_datefmt = '%a, %d %b %Y %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, format=logging_format, datefmt=logging_datefmt, handlers=logging_handlers)


config = ConfigParser()
config.read('../config.ini')

redis_srv = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')
api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot.user_api"

redis_sts = redis.Redis.from_url(redis_srv + "/9", decode_responses=True) # device status (online or offline)
redis_cfg = redis.Redis.from_url(redis_srv + "/10", decode_responses=True) # device defines
redis_rtdb = redis.Redis.from_url(redis_srv+"/12", decode_responses=True) # device real-time data


client_auth_map = {}


rtdata = RTData(redis_cfg, redis_rtdb)
frappe_api = UserApi(config)
server = WebsocketServer(port=17654, host='0.0.0.0', loglevel=logging.INFO)
sub = SubClient(config, server)
action_worker = ActionWorker()


def new_client(client, server):
	server.send_message(client, json.dumps({
		"id": 0,
		"code": "welcome",
		"data": "Welcome!"
	}))


def client_left(client, server):
	sub.unsubscribe(client)
	handler = client["handler"]
	if client_auth_map.get(handler):
		client_auth_map.pop(handler)


def valid_client(client):
	handler = client["handler"]
	c = client_auth_map.get(handler)
	if not c:
		raise Exception("NOT LOGIN")
	return c.get("auth_code")


def message_received(client, server, message):
	msg = json.loads(message)
	id = msg["id"]
	code = msg["code"]

	if code == 'ping':
		server.send_message(client, json.dumps({
			"id": id,
			"code": code
		}))
		return

	if code == 'login':
		auth_code = msg['data']
		got_user = frappe_api.get_user(auth_code)
		if not got_user:
			raise ("User code incorrect")
		server.send_message(client, json.dumps({
			"id": id,
			"code": code,
			"data": got_user
		}))
		client_auth_map[client["handler"]] = {
			"user": got_user,
			"auth_code": auth_code,
			"client": client
		}
		return

	if code == 'proxy':
		data = msg['data']
		auth_code = data['AuthorizationCode']
		url = data['url']
		method = data['method']
		if method == 'POST':
			r = frappe_api.proxy_get(auth_code, url, method, data.get("params"), data.get('data'))
			server.send_message(client, r)
		else:
			r = frappe_api.proxy_post(auth_code, url, method, data.get("params"), data.get('data'))
			server.send_message(client, r)

	auth_code = valid_client(client)

	if code == 'device_data':
		device = msg['data']
		if not frappe_api.access_device(auth_code, device):
			logging.warning("Not permitted to operation on this device")
			server.send_message(client, json.dumps({
				"id": id,
				"code": code,
				"data": 0,
			}))
			return

		data = rtdata.query(device)
		server.send_message(client, json.dumps({
			"id": id,
			"code": code,
			"data": data
		}))
		return

	if code == 'device_sub':
		device = msg['data']
		if not frappe_api.access_device(auth_code, device):
			logging.warning("Not permitted to operation on this device")
			server.send_message(client, json.dumps({
				"id": id,
				"code": code,
				"data": 0,
			}))
			return

		data = rtdata.query(device)
		server.send_message(client, json.dumps({
			"id": id,
			"code": 'device_data',
			"data": data
		}))
		server.send_message(client, json.dumps({
			"id": id,
			"code": code,
			"data": 1,
		}))
		sub.subscribe(client, device)
		return

	if code == 'device_unsub':
		device = msg['data']
		sub.unsubscribe(client, device)
		server.send_message(client, json.dumps({
			"id": id,
			"code": code
		}))
		return

	if code == 'send_output':
		data = msg['data']
		device = data.get('device')
		if not frappe_api.access_device(auth_code, device):
			logging.warning("Not permitted to operation on this device")
			server.send_message(client, json.dumps({
				"id": id,
				"code": code,
				"data": {
					"message": "Not permitted to operation on this device",
					"result": False,
				}
			}))
			return
		else:
			action_worker.send_output(ws_client=client, ws_server=server, ws_id=id, auth_code=auth_code, frappe_api=frappe_api, data=data)
			server.send_message(client, json.dumps({
				"id": id,
				"code": code,
				"data": {
					"message": "Send output done!",
					"result": True,
				}
			}))
			return

	if code == 'send_command':
		data = msg['data']
		device = data.get('device')
		if not frappe_api.access_device(auth_code, device):
			logging.warning("Not permitted to operation on this device")
			server.send_message(client, json.dumps({
				"id": id,
				"code": code,
				"data": {
					"message": "Not permitted to operation on this device",
					"result": False,
				}
			}))
			return
		else:
			action_worker.send_command(ws_client=client, ws_server=server, ws_id=id, auth_code=auth_code, frappe_api=frappe_api, data=data)
			server.send_message(client, json.dumps({
				"id": id,
				"code": code,
				"data": {
					"message": "Send command done!",
					"result": True,
				},
			}))
			return


server.set_fn_new_client(new_client)
server.set_fn_client_left(client_left)
server.set_fn_message_received(message_received)

action_worker.start()
sub.start()
server.run_forever()
