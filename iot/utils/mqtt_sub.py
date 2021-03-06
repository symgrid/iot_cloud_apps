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

	if rc != 0:
		return

	logging.info("Sub MQTT Subscribe topics")
	client.subscribe("+/data")
	client.subscribe("+/data_gz")
	client.subscribe("+/devices")
	client.subscribe("+/devices_gz")
	client.subscribe("+/device")
	client.subscribe("+/device_gz")
	client.subscribe("+/status")
	client.subscribe("+/event")


def on_disconnect(client, userdata, rc):
	logging.error("Sub MQTT Disconnect with result code "+str(rc))


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
			userdata.on_data(dev, intput, payload)
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
					userdata.on_data(dev, intput, d)

		except Exception as ex:
			logging.exception(ex)
			logging.debug('Catch an exception: %s\t%d\t%d', msg.topic, msg.qos, msg.retain)
		return

	if topic == 'devices' or topic == 'devices_gz':
		data = msg.payload.decode('utf-8', 'surrogatepass') if topic == 'devices' else zlib.decompress(msg.payload).decode('utf-8', 'surrogatepass')
		logging.debug('%s/%s\t%s', devid, topic, data)
		devs = json.loads(data)
		if not devs:
			logging.warning('Decode DEVICES_GZ JSON Failure: %s/%s\t%s', devid, topic, data)
			return

		for dev in devs:
			userdata.on_device(dev, devs[dev])
		return

	if topic == 'device' or topic == 'device_gz':
		data = msg.payload.decode('utf-8', 'surrogatepass') if topic == 'device' else zlib.decompress(msg.payload).decode('utf-8', 'surrogatepass')
		logging.debug('%s/%s\t%s', devid, topic, data)
		dev = json.loads(data)
		if not dev:
			logging.warning('Decode DEVICE_GZ JSON Failure: %s/%s\t%s', devid, topic, data)
			return


		action = dev.get('action')
		dev_sn = dev.get('sn')
		if not action or not dev_sn:
			logging.warning('Invalid DEVICE data: %s/%s\t%s', devid, topic, data)
			return

		if action == 'add':
			userdata.on_device(dev_sn, dev.get('props'))
		elif action == 'mod':
			userdata.on_device(dev_sn, dev.get('props'))
		elif action == 'del':
			# TODO: userdata.on_device(dev_sn, {})
			pass
		else:
			logging.warning('Unknown Device Action!!')
		return

	if topic == 'status':
		status = msg.payload.decode('utf-8', 'surrogatepass')
		userdata.on_device_status(devid, status)
		return

	if topic == 'event':
		event = msg.payload.decode('utf-8', 'surrogatepass')
		userdata.on_device_event(devid, event)
		return


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	try:
		on_message_raw(client, userdata, msg)
	except Exception as ex:
		logging.exception(ex)


class MQTTClient(threading.Thread):
	def __init__(self, subclient, host, port, keepalive, clientid, user, password):
		threading.Thread.__init__(self)
		self.subclient = subclient
		self.host = host
		self.port = port
		self.clientid = clientid
		self.user = user
		self.password = password
		self.keepalive = keepalive
		self.mqttc = None

	def run(self):
		try:
			mqttc = mqtt.Client(userdata=self.subclient, client_id=self.clientid)
			mqttc.username_pw_set(self.user, self.password)
			self.mqttc = mqttc

			mqttc.on_connect = on_connect
			mqttc.on_disconnect = on_disconnect
			mqttc.on_message = on_message

			logging.debug('MQTT Connect to %s:%d', self.host, self.port)
			mqttc.connect_async(self.host, self.port, self.keepalive)

			mqttc.loop_forever(retry_first_connection=True)
		except Exception as ex:
			logging.exception(ex)
			self.mqttc = None

	def disconnect(self):
		self.mqttc.disconnect()

	def publish(self, *args, **kwargs):
		return self.mqttc.publish(*args, **kwargs)


class SubClient:
	def __init__(self, host, port, keepalive=60, clientid=None, user=None, password=None):
		self.host = host
		self.port = port
		self.clientid = clientid
		self.user = user
		self.password = password
		self.keepalive = keepalive
		self.mqttc = None

	def start(self):
		mqttc = MQTTClient(self, host=self.host, port=self.port, keepalive=self.keepalive, clientid=self.clientid, user=self.user, password=self.password)
		mqttc.start()
		self.mqttc = mqttc

	def stop(self):
		if (self.mqttc):
			self.mqttc.disconnect()
