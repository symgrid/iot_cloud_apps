
from __future__ import unicode_literals
import re
import os
import json
import redis
import logging
import zlib
from collections import deque
import paho.mqtt.client as mqtt


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S')


redis_srv = 'redis://:Pa88word@m.symgrid.com:6379'
mqtt_host = 'm.symgrid.com'
mqtt_port = 1883
mqtt_keepalive = 60


redis_sts = redis.Redis.from_url(redis_srv+"/9") # device status (online or offline)
redis_cfg = redis.Redis.from_url(redis_srv+"/10") # device defines
redis_rel = redis.Redis.from_url(redis_srv+"/11") # device relationship
redis_rtdb = redis.Redis.from_url(redis_srv+"/12") # device real-time data

''' Set all data be expired after device offline '''
redis_offline_expire = 3600 * 24 * 7

data_queue = deque()
match_topic = re.compile(r'^([^/]+)/(.+)$')


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	logging.info("Main MQTT Connected with result code "+str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	client.subscribe("+/data")
	client.subscribe("+/device")
	client.subscribe("+/status")
	client.subscribe("+/event")


def on_disconnect(client, userdata, rc):
	logging.info("Main MQTT Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	g = match_topic.match(msg.topic)
	if not g:
		return
	g = g.groups()
	if len(g) < 2:
		return

	devid = g[0]
	topic = g[1]

	if topic == 'data':
		if msg.retain != 0:
			return
		payload = json.loads(msg.payload.decode('utf-8'))
		if not payload:
			logging.warning('Decode DATA JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8'))
			return

		# logging.debug('device: %s\tInput: %s\t Value: %s', g[0], g[1], json.dumps(payload))
		r = redis_rtdb.hmset(devid, {
			payload['input']: json.dumps(payload['data'])
		})
		return

	if topic == 'device':
		data = msg.payload.decode('utf-8')
		logging.debug('%s/%s\t%s', devid, topic, data)
		info = json.loads(data)
		if not info:
			logging.warning('Decode JSON Failure: %s/%s\t%s', devid, topic, data)
			return

		gateid = info['gate']
		devkeys = set(redis_rel.lrange(gateid, 0, 1000))
		redis_rel.ltrim(gateid, 0, -1000)
		devkeys.add(devid)
		for key in devkeys:
			redis_rel.lpush(gateid, key)

		redis_rtdb.persist(devid)
		redis_cfg.persist(devid)
		redis_cfg.set(devid, json.dumps(info['info']))
		redis_rel.persist('PARENT_{0}'.format(devid))
		redis_rel.set('PARENT_{0}'.format(devid), gateid)

		return

	if topic == 'status':
		gateid = devid
		data = msg.payload.decode('utf-8')
		data = json.loads(data)
		if not data:
			logging.warning('Decode JSON Failure: %s/%s\t%s', devid, topic, data)
			return

		status = data['status']
		redis_sts.set(gateid, status)
		if status == 'OFFLINE':
			redis_sts.expire(gateid, redis_offline_expire)
			redis_rel.expire(gateid, redis_offline_expire)
			devkeys = redis_rel.lrange(gateid, 0, 1000)
			for dev in devkeys:
				redis_cfg.expire(dev, redis_offline_expire)
				redis_rtdb.expire(dev, redis_offline_expire)
				redis_rel.expire('PARENT_{0}'.format(dev), redis_offline_expire)
		else:
			redis_sts.persist(gateid)
			redis_rel.persist(gateid)

		return

	if topic == 'event':
		event = msg.payload.decode('utf-8')
		return


# Listen on MQTT forwarding real-time data into redis, and forwarding configuration to frappe.
client = mqtt.Client(client_id="THINGSROOT_MQTT_TO_REDIS")
client.username_pw_set("root", "root")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

try:
	logging.debug('MQTT Connect to %s:%d', mqtt_host, mqtt_port)
	client.connect_async(mqtt_host, mqtt_port, mqtt_keepalive)

	# Blocking call that processes network traffic, dispatches callbacks and
	# handles reconnecting.
	# Other loop*() functions are available that give a threaded interface and a
	# manual interface.
	client.loop_forever(retry_first_connection=True)
except Exception as ex:
	logging.exception(ex)
	os._exit(1)
