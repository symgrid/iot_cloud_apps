'''
MQTT Message subscribuer
'''
from __future__ import unicode_literals
import threading
import json
import re
import os
import logging
import zlib
import time
import paho.mqtt.client as mqtt
from urllib.parse import urlparse
from utils import _dict
from utils.user_api import UserApi


match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/(.+)$')

redis_result_expire = 60 * 60 * 24 # in seconds  (24 hours)

topics = ["data", "data_gz", "devices", "devices_gz", "status", "event"]


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	userdata.on_connect(client, flags, rc)


def on_disconnect(client, userdata, rc):
	userdata.on_disconnect(client, rc)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	try:
		userdata.on_message(client, msg)
	except Exception as ex:
		logging.exception(ex)


class MQTTClient(threading.Thread):
	def __init__(self, host, port, keepalive=60, clientid="", user=None, password=None):
		threading.Thread.__init__(self)
		self.host = host
		self.port = port
		self.clientid = clientid
		self.user = user
		self.password = password
		self.keepalive = keepalive
		self.sub_devs = set()

	def stop(self):
		self.sub_devs = set()
		self.mqttc.disconnect()

	def run(self):
		try:
			mqttc = mqtt.Client(userdata=self, client_id=self.clientid)
			if self.user:
				mqttc.username_pw_set(self.user, self.password)

			self.mqttc = mqttc

			mqttc.on_connect = on_connect
			mqttc.on_disconnect = on_disconnect
			mqttc.on_message = on_message

			logging.debug('MQTT Connect to %s:%d cid: %s user: %s passwd: %s', self.host, self.port, self.clientid, self.user or "", self.password or "")
			mqttc.connect_async(self.host, self.port, self.keepalive)

			mqttc.loop_forever(retry_first_connection=True)
		except Exception as ex:
			logging.exception(ex)
			os._exit(1)

	def on_connect(self, client, flags, rc):
		logging.info("MQTT %s connected return %d", self.host, rc)

		if rc != 0:
			return

		logging.info("MQTT %s MQTT Subscribe topics", self.host)
		for device in self.sub_devs:
			for topic in topics:
				self.mqttc.subscribe(device + "/" + topic)

	def on_disconnect(self, client, rc):
		logging.info("MQTT %s disconnect return %d", self.host, rc)

	def on_message(self, client, msg):
		logging.info("MQTT %s message recevied topic %s", self.host, msg.topic)

	def publish(self, *args, **kwargs):
		return self.mqttc.publish(*args, **kwargs)

	def subscribe(self, device):
		logging.info('Subscribe device %s', device)
		self.sub_devs.add(device)
		for topic in topics:
			self.mqttc.subscribe(device + "/" + topic)

	def unsubscribe(self, device):
		logging.info('Unsubscribe device %s', device)
		self.sub_devs.remove(device)
		for topic in topics:
			self.mqttc.unsubscribe(device + "/" + topic)


class IOTMQTTClient(MQTTClient):
	def __init__(self, subclient, host, port, keepalive=60, clientid="", user=None, password=None):
		self.subclient = subclient
		MQTTClient.__init__(self, host, port, keepalive, clientid, user, password)

	def on_message(self, client, msg):
		g = match_topic.match(msg.topic)
		if not g:
			return
		g = g.groups()
		if len(g) < 2:
			return

		devid = g[0]
		topic = g[1]

		if topic == 'data':
			payload = json.loads(msg.payload.decode('utf-8', 'surrogatepass'))
			if not payload:
				logging.warning('Decode DATA JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8', 'surrogatepass'))
				return
			g = match_data_path.match(payload[0])
			if g and msg.retain == 0:
				g = g.groups()
				dev = g[0]
				intput = g[1]
				# pop input key
				payload.pop(0)
				self.subclient.on_data(devid, dev, intput, payload)
			return

		if topic == 'data_gz':
			try:
				payload = zlib.decompress(msg.payload).decode('utf-8', 'surrogatepass')
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
						self.subclient.on_data(devid, dev, intput, d)

			except Exception as ex:
				logging.exception(ex)
				logging.debug('Catch an exception: %s\t%d\t%d', msg.topic, msg.qos, msg.retain)
			return

		if topic == 'devices' or topic == 'devices_gz':
			data = msg.payload.decode('utf-8', 'surrogatepass') if topic == 'devices' else zlib.decompress(msg.payload).decode('utf-8', 'surrogatepass')
			logging.debug('%s/%s\t%s', devid, topic, data)
			devs = json.loads(data)
			if not devs:
				logging.warning('Decode DEVICE_GZ JSON Failure: %s/%s\t%s', devid, topic, data)
				return

			for dev in devs:
				self.subclient.on_device(devid, dev, devs[dev])
			return

		if topic == 'status':
			status = msg.payload.decode('utf-8', 'surrogatepass')
			self.subclient.on_device_status(devid, status)
			return

		if topic == 'event':
			event = msg.payload.decode('utf-8', 'surrogatepass')
			self.subclient.on_device_event(devid, event)
			return


class MQTTBridge(threading.Thread):
	def __init__(self, config, remote):
		threading.Thread.__init__(self)
		self.thread_stop = False
		self.config = config
		self.api_srv = config.get('iot', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot."
		self.user_api = UserApi(self.api_srv, remote.auth_code)
		self.remote = remote
		self.device_sub_map = {}
		self.id = 0
		self.devices = set()

	def list_devices(self):
		devices = _dict(self.user_api.list_devices())
		results = set(devices.private_devices)
		for g in devices.shared_devices:
			results = results.union(set(_dict(g).devices))
		for g in devices.company_devices:
			results = results.union(set(_dict(g).devices))
		return results


	def update_devices(self):
		devices = self.list_devices()
		to_be_remove = self.devices.difference(devices)
		for device in to_be_remove:
			self.mqttc.unsubscribe(device)

		self.devices.difference_update(to_be_remove)

		to_be_add = devices.difference(self.devices)
		for device in to_be_add:
			self.mqttc.subscribe(device)

		self.devices.update(to_be_add)


	def start(self):
		host = self.config.get('mqtt', 'host', fallback='127.0.0.1')
		port = self.config.getint('mqtt', 'port', fallback=1883)
		keepalive = self.config.getint('mqtt', 'keepalive', fallback=60)
		clientid = self.config.get('mqtt', 'clientid_base', fallback="IOT_CLOUD_APPS") + ".BRIDGE." + self.remote.name
		user = self.config.get('mqtt', 'user', fallback="root")
		password = self.config.get('mqtt', 'password', fallback="bXF0dF9pb3RfYWRtaW4K")
		mqttc = IOTMQTTClient(subclient=self, host=host, port=port, keepalive=keepalive, clientid=clientid, user=user, password=password)
		mqttc.start()
		self.mqttc = mqttc

		remote = _dict(self.remote)
		bridge_host = remote.host
		bridge_port = 1883
		bridge_clientid = 'THINGSROOT_MQTT_BRIDGE'
		uri = urlparse(remote.host)
		if uri:
			bridge_host = uri.hostname or remote.host
			bridge_port = uri.port or 1883
			bridge_clientid = uri.username or 'THINGSROOT_MQTT_BRIDGE'
		mqtt_bridge = MQTTClient(host=bridge_host, port=bridge_port, clientid=bridge_clientid, user=remote.user, password=remote.password)
		mqtt_bridge.start()
		self.mqtt_bridge = mqtt_bridge

		threading.Thread.start(self)

	def stop(self):
		self.thread_stop = True
		self.mqttc.stop()
		self.mqtt_bridge.stop()
		self.join()

	def run(self):
		try:
			while not self.thread_stop:
				self.update_devices()
				time.sleep(60)
		except Exception as ex:
			logging.exception(ex)

	def on_data(self, gate, sn, input, data):
		self.mqtt_bridge.publish(topic=sn+'/data', payload=json.dumps({
			'input': input,
			'data': data
		}))

	def on_device(self, gate, sn, info):
		self.mqtt_bridge.publish(topic=sn+'/device', payload=json.dumps({
			"gate": gate,
			"device": sn,
			"info": info
		}), qos=1, retain=True)

	def on_device_status(self, gate, status):
		self.mqtt_bridge.publish(topic=gate+'/status', payload=json.dumps({
			"gate": gate,
			"status": status
		}), qos=1, retain=True)

	def on_device_event(self, gate, event):
		self.mqtt_bridge.publish(topic=gate+'/event', payload=json.dumps({
			"gate": gate,
			"event": event
		}), qos=1)

