
from __future__ import unicode_literals
import redis
import logging
import json
from configparser import ConfigParser
from websocket_server import WebsocketServer
from workers.sub import SubClient
from rtdata import RTData
from frappe_api import FrappeApi


logging.basicConfig(level=logging.DEBUG,
					format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
					datefmt='%a, %d %b %Y %H:%M:%S')

config = ConfigParser()
config.read('../config.ini')

redis_srv = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')
api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot.user_api"

redis_sts = redis.Redis.from_url(redis_srv + "/9", decode_responses=True) # device status (online or offline)
redis_cfg = redis.Redis.from_url(redis_srv + "/10", decode_responses=True) # device defines
redis_rel = redis.Redis.from_url(redis_srv + "/11", decode_responses=True) # device relationship
redis_rtdb = redis.Redis.from_url(redis_srv+"/12", decode_responses=True) # device real-time data


device_sub_map = {}
client_auth_map = {}


def add_sub(client, device):
	sub_array = device_sub_map.get(device) or []
	sub_array.append(client)
	device_sub_map[device] = sub_array


def remove_sub(client, device=None):
	if device:
		sub_array = device_sub_map.get(device)
		for d in sub_array:
			if client['handler'] == d['handler']:
				sub_array.remove(d)
		device_sub_map[device] = sub_array
		return

	for device in device_sub_map:
		sub_array = device_sub_map.get(device)
		for d in sub_array:
			if client['handler'] == d['handler']:
				sub_array.remove(d)
		device_sub_map[device] = sub_array


rtdata = RTData(redis_cfg, redis_rtdb)
frappe_api = FrappeApi(config)
server = WebsocketServer(port=17654, host='0.0.0.0', loglevel=logging.INFO)
sub = SubClient(config, server, device_sub_map)


def new_client(client, server):
	server.send_message(client, json.dumps({
		"id": 0,
		"code": "welcome",
		"data": "Welcome!"
	}))


def client_left(client, server):
	remove_sub(client)

	for c in client_auth_map:
		if c == client["handler"]:
			client_auth_map[c] = None


def valid_client(client):
	c = client_auth_map.get(client['handler'])
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
		if not frappe_api.get_device(auth_code, device):
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
		if not frappe_api.get_device(auth_code, device):
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
		add_sub(client, device)
		return

	if code == 'device_unsub':
		device = msg['data']
		remove_sub(client, device)
		server.send_message(client, json.dumps({
			"id": id,
			"code": code
		}))
		return


server.set_fn_new_client(new_client)
server.set_fn_client_left(client_left)
server.set_fn_message_received(message_received)

sub.start()
server.run_forever()
