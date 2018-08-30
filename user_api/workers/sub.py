'''
Publish/Subscribe message broker between Redis and MQTT
'''
import threading
import json
import re
import os
import logging
import zlib
import paho.mqtt.client as mqtt


match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/(.+)$')

redis_result_expire = 60 * 60 * 24 # in seconds  (24 hours)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	logging.info("Sub MQTT Connected with result code "+str(rc))
	client.subscribe("+/data")
	client.subscribe("+/data_gz")
	client.subscribe("+/devices")
	client.subscribe("+/devices_gz")
	client.subscribe("+/status")
	client.subscribe("+/event")


def on_disconnect(client, userdata, rc):
	logging.info("Sub MQTT Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message_raw(client, userdata, msg):
	g = match_topic.match(msg.topic)
	if not g:
		return
	g = g.groups()
	if len(g) < 2:
		return

	devid = g[0]
	topic = g[1]

	if topic == 'data':
		payload = json.loads(msg.payload.decode('utf-8'))
		if not payload:
			logging.warning('Decode DATA JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8'))
			return
		g = match_data_path.match(payload[0])
		if g and msg.retain == 0:
			g = g.groups()
			dev = g[0]
			intput = g[1]
			# pop input key
			payload.pop(0)
			userdata.on_data(dev, intput, payload)
		return

	if topic == 'data_gz':
		try:
			payload = zlib.decompress(msg.payload).decode('utf-8')
			data_list = json.loads(payload)
			if not data_list:
				logging.warning('Decode DATA_GZ JSON Failure: %s/%s\t%s', devid, topic, payload)
				return
			for d in data_list:
				g = match_data_path.match(d[0])
				if g and msg.retain == 0:
					g = g.groups()
					dev = g[0]
					intput = g[1]
					# pop input key
					d.pop(0)
					userdata.on_data(dev, intput, d)

		except Exception as ex:
			logging.exception(ex)
			logging.debug('Catch an exception: %s\t%d\t%d', msg.topic, msg.qos, msg.retain)
		return

	if topic == 'devices' or topic == 'devices_gz':
		data = msg.payload.decode('utf-8') if topic == 'devices' else zlib.decompress(msg.payload).decode('utf-8')
		logging.debug('%s/%s\t%s', devid, topic, data)
		devs = json.loads(data)
		if not devs:
			logging.warning('Decode DEVICE_GZ JSON Failure: %s/%s\t%s', devid, topic, data)
			return

		for dev in devs:
			userdata.on_device(dev, devs[dev])
		return

	if topic == 'status':
		status = msg.payload.decode('utf-8')
		userdata.on_device_status(devid, status)
		return

	if topic == 'event':
		event = msg.payload.decode('utf-8')
		userdata.on_device_event(devid, event)
		return


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	try:
		on_message_raw(client, userdata, msg)
	except Exception as ex:
		logging.exception(ex)


class MQTTClient(threading.Thread):
	def __init__(self, subclient, host="localhost", port=1883, keepalive=60):
		threading.Thread.__init__(self)
		self.subclient = subclient
		self.host = host
		self.port = port
		self.keepalive = keepalive

	def run(self):
		try:
			mqttc = mqtt.Client(userdata=self.subclient, client_id="SYS_MQTT_TO_REDIS.SUB")
			mqttc.username_pw_set("root", "bXF0dF9pb3RfYWRtaW4K")
			self.mqttc = mqttc

			mqttc.on_connect = on_connect
			mqttc.on_disconnect = on_disconnect
			mqttc.on_message = on_message

			logging.debug('MQTT Connect to %s:%d', self.host, self.port)
			mqttc.connect_async(self.host, self.port, self.keepalive)

			mqttc.loop_forever(retry_first_connection=True)
		except Exception as ex:
			logging.exception(ex)
			os._exit(1)

	def publish(self, *args, **kwargs):
		return self.mqttc.publish(*args, **kwargs)


class SubClient:
	def __init__(self, config, ws_server):
		self.config = config
		self.ws_server = ws_server
		self.device_sub_map = {}
		self.id = 0
		self.invalid_client = []

	def subscribe(self, client, device):
		sub_map = self.device_sub_map.get(device) or {}
		sub_map[client['handler']] = client
		self.device_sub_map[device] = sub_map

	def unsubscribe(self, client, device=None):
		if device:
			sub_map = self.device_sub_map.get(device)
			if not sub_map:
				return

			new_map = {}
			for d in sub_map:
				if client['handler'] != d:
					new_map[d] = sub_map[d]
			self.device_sub_map[device] = new_map
			return

		for device in self.device_sub_map:
			sub_map = self.device_sub_map.get(device)
			new_map = {}
			for d in sub_map:
				if client['handler'] != d:
					new_map[d] = sub_map[d]
			self.device_sub_map[device] = new_map
			return

	def start(self):
		host = self.config.get('mqtt', 'host', fallback='127.0.0.1')
		port = self.config.getint('mqtt', 'port', fallback=1883)
		keepalive = self.config.getint('mqtt', 'keepalive', fallback=60)
		mqttc = MQTTClient(self, host=host, port=port, keepalive=keepalive)
		mqttc.start()
		self.mqttc = mqttc

	def send_sub_message(self, client, msg):
		try:
			self.ws_server.send_message(client, json.dumps(msg))
			self.id = self.id + 1
		except Exception as ex:
			logging.exception(ex)
			self.invalid_client.append(client)

	def clean_invalid_client(self):
		for client in self.invalid_client:
			self.unsubscribe(client)
		self.invalid_client = []

	def on_data(self, sn, input, data):
		sub_map = self.device_sub_map.get(sn) or {}
		msg = None
		for client in sub_map.values():
			msg = msg or {
				"id": self.id,
				"code": 'data',
				"data": {
					"device": sn,
					"input": input,
					"value": data
				}
			}
			self.send_sub_message(client, msg)
			self.clean_invalid_client()

	def on_device(self, sn, info):
		sub_map = self.device_sub_map.get(sn) or {}
		msg = None
		for client in sub_map.values():
			msg = msg or {
				"id": self.id,
				"code": 'device',
				"data": {
					"device": sn,
					"info": info
				}
			}
			self.send_sub_message(client, msg)
			self.clean_invalid_client()

	def on_device_status(self, sn, status):
		sub_map = self.device_sub_map.get(sn) or {}
		msg = None
		for client in sub_map.values():
			msg = msg or {
				"id": self.id,
				"code": 'device_status',
				"data": {
					"device": sn,
					"status": status
				}
			}
			self.send_sub_message(client, msg)
			self.clean_invalid_client()

	def on_device_event(self, sn, event):
		sub_map = self.device_sub_map.get(sn) or {}
		msg = None
		for client in sub_map.values():
			msg = msg or {
				"id": self.id,
				"code": 'device_event',
				"data": {
					"device": sn,
					"event": event
				}
			}
			self.send_sub_message(client, msg)
			self.clean_invalid_client()
