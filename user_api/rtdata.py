import logging
import json


class RTData:
	def __init__(self, redis_cfg, redis_rtdb):
		self.redis_cfg = redis_cfg
		self.redis_rtdb = redis_rtdb

	def query(self, device_sn):
		cfg = json.loads(self.redis_cfg.get(device_sn) or "{}")
		hs = self.redis_rtdb.hgetall(device_sn)
		data = {}
		inputs = cfg.get("inputs") or []
		for input in inputs:
			input_name = input.get('name')
			s = hs.get(input_name + "/value")
			if s:
				val = json.loads(s)
				data[input_name] = {"PV": val[1], "TM": val[0], "Q": val[2]}
		return data