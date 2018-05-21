
from __future__ import unicode_literals
import influxdb
import json
from influxdb.exceptions import InfluxDBClientError
import logging
import tzlocal

class Client:
	def __init__(self, host, port, username, password, database):
		self.host = host
		self.port = port
		self.username = username
		self.password = password
		self.database = database
		self._client = None

	def connect(self):
		self._client = influxdb.InfluxDBClient( host=self.host,
												port=self.port,
												username=self.username,
												password=self.password,
												database=self.database )

	def write_data(self, data_list):
		points = []
		for data in data_list:
			if data.get('event_data') is True and data['name'] == 'iot_device_event':
				points.append({
					"measurement": data['name'],
					"tags": {
						"device": data['device'],
						"iot": data['iot']
					},
					"time": int(data['timestamp'] * 1000),
					"fields": {
						data['property']: data['value'],
						"quality": data['quality'],
						"level": data['level']
					}
				})
			elif data.get('statistics') is True:
				points.append({
					"measurement": data['name'],
					"tags": {
						"owner": data['owner'],
					},
					"time": int(data['timestamp'] * 1000),
					"fields": data['fields']
				})
			else:
				points.append({
					"measurement": data['name'],
					"tags": {
						"device": data['device'],
						"iot": data['iot']
					},
					"time": int(data['timestamp'] * 1000),
					"fields": {
						data['property']: data['value'],
						"quality": data['quality'],
					}
				})
		try:
			self._client.write_points(points, time_precision='ms')
		except InfluxDBClientError as ex:
			if ex.code == 400:
				logging.exception('Catch an exception.')
				return

	def create_database(self):
		try:
			self._client.create_database(self.database)
		except Exception as ex:
			logging.exception('Catch an exception.')

	def query_event_count(self, iot, startime, endtime, group_by):
		time_cond = 'time >= \'' + startime + '\' AND time < \'' + endtime +'\''
		dev_cond = '"iot"=\'' + iot + '\' AND "device"=\'' + iot + '\''
		group_by = 'GROUP BY time(' + group_by + ') FILL(null)'
		query = 'select count("event") from iot_device_event where ' + dev_cond + ' AND ' + time_cond + ' ' + group_by
		query = query + ' tz(\'' + tzlocal.get_localzone().zone + '\')'

		try:
			val = self._client.query(query)

			value = []
			for p in val.get_points('iot_device_event'):
				value.append(p)

			return value
		except Exception as ex:
			return []

	def query_event_type_count(self, iot, startime, endtime):
		time_cond = 'time >= \'' + startime + '\' AND time < \'' + endtime +'\''
		dev_cond = '"iot"=\'' + iot + '\''
		query = 'select event from iot_device_event where ' + dev_cond + ' AND ' + time_cond
		query = query + ' tz(\'' + tzlocal.get_localzone().zone + '\')'

		try:
			val = self._client.query(query)

			value = {}
			for p in val.get_points('iot_device_event'):
				data = json.loads(p.get('event'))
				v = value.get(data.get('type')) or 0
				v = v + 1
				value[data.get('type')] = v

			return value
		except Exception as ex:
			return []

	def query_device_status(self, iot, startime, endtime):
		time_cond = 'time >= \'' + startime + '\' AND time < \'' + endtime +'\''
		dev_cond = '"iot"=\'' + iot + '\' AND "device"=\'' + iot + '\''
		query = 'select online from device_status where ' + dev_cond + ' AND ' + time_cond
		query = query + ' tz(\'' + tzlocal.get_localzone().zone + '\')'

		try:
			val = self._client.query(query)

			value = []
			for p in val.get_points('device_status'):
				value.append(p)

			return value
		except Exception as ex:
			return []