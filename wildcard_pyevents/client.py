# -*- coding: utf-8 -*-
"""
Python client for Wildcard Events
"""
import json
import types
import redis
import datetime
from influxdb import InfluxDBClient


class WildcardPyEventsError(Exception):
    "Raised when an error occurs in the request"
    def __init__(self, content, code):
        super(WildcardPyEventsError, self).__init__(
            "{0}: {1}".format(code, content)
        )
        self.content = content
        self.code = code


class WildcardPyEventsClient(object):
    def __init__(self,
                 host_name,
                 environment,
                 influx_host='localhost',
                 influx_port=8086,
                 influx_username='app',
                 influx_password='',
                 influx_database='developer-metrics',
                 influx_udp_port=-1,
                 logstash_redis_host='localhost',
                 logstash_redis_port=6379,
                 logstash_redis_queue='logstash'
                 ):
        influx_use_udp = False
        if host_name is None or environment is None:
            raise RuntimeError('Need to specify host_name, environment')
        if influx_udp_port > -1:
            influx_use_udp = True
        self.influxdb_client = InfluxDBClient(
            host=influx_host,
            port=influx_port,
            username=influx_username,
            password=influx_password,
            database=influx_database,
            ssl=False,
            use_udp=influx_use_udp,
            udp_port=influx_udp_port,
        )
        self.redis_client = redis.StrictRedis(
            host=logstash_redis_host,
            port=logstash_redis_port,
            db=0,
        )
        self.logstash_redis_queue = logstash_redis_queue
        self.host_name = host_name
        self.environment = environment

    def send(self, event_name, payload, type=None):
        if not isinstance(payload, types.ListType):
            payload = [payload]

        for event in payload:
            event["environment"] = self.environment
            event["host"] = self.host_name
            if type is not None:
                event["type"] = type

        try:
            self.send_to_influx(event_name, payload)
        except Exception, e:
            print("Could not send events to influx: " + json.dumps(payload))
            print(e)

        try:
            self.send_to_logstash(event_name, payload)
        except Exception, e:
            print("Could not send events to logstash: " + json.dumps(payload))
            print(e)

    def send_to_influx(self, event_name, events):
        if not isinstance(events, types.ListType):
            raise RuntimeError('Need a list of events')

        # create a timestamp in influx compatible format (ms since epoch, utc)
        epoch = datetime.datetime.utcfromtimestamp(0)
        delta = datetime.datetime.utcnow() - epoch
        timestamp = int(delta.total_seconds()*1000.0 +
                        delta.microseconds/1000.0)

        keys = events[0].keys()
        points = []
        for event in events:
            event_keys = event.keys()
            if (event_keys != keys):
                raise RuntimeError('Events have to have the same structure.')

            if "time" not in event:
                event["time"] = timestamp

            vals = map((lambda x: event[x]), keys)
            points.append(vals)

        json_body = {"columns": keys, "name": event_name, "points": points}
        # print json_body
        self.influxdb_client.write_points(json.dumps([json_body]))

    def send_to_logstash(self, event_name, events):
        if not isinstance(events, types.ListType):
            raise RuntimeError('Need a list of events')

        # create a timestamp in python-beaver compatible format
        now = datetime.datetime.utcnow()
        timestamp = (now.strftime("%Y-%m-%dT%H:%M:%S") +
                     ".%03d" % (now.microsecond / 1000) + "Z")
        for event in events:
            if "@timestamp" not in event:
                event["@timestamp"] = timestamp
            event["eventName"] = event_name

        events_payload = map((lambda x: json.dumps(x)), events)

        self.redis_client.rpush(self.logstash_redis_queue, *events_payload)
