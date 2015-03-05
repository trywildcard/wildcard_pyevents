# -*- coding: utf-8 -*-
"""
Python client for Wildcard Events
"""
import json
import types
import redis
from influxdb import InfluxDBClient

class WildcardPyEventsError(Exception):
    "Raised when an error occurs in the request"
    def __init__(self, content, code):
        super(WildcardPyEventError, self).__init__(
            "{0}: {1}".format(code, content))
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
        influx_use_udp = False;
        if (host_name == None or environment == None):
            raise RuntimeError('Need to specify host_name, environment')
        if (influx_udp_port > -1):
            influx_use_udp = True
        self.influxdb_client = InfluxDBClient(host=influx_host, port=influx_port, username=influx_username, password=influx_password, database=influx_database, ssl=False, use_udp=influx_use_udp, udp_port=influx_udp_port)
        self.redis_client = redis.StrictRedis(host=logstash_redis_host, port=logstash_redis_port, db=0)
        self.logstash_redis_queue = logstash_redis_queue
        self.host_name = host_name
        self.environment = environment

    def send(self, event_name, payload, type=None):
        if (isinstance(payload, types.ListType)):
            for event in payload:
                payload["environment"] = self.environment
                payload["host"] = self.host_name
                if(type != None):
                    payload["type"] = type
        else:
            payload["host"] = self.host_name
            payload["environment"] = self.environment
            if(type != None):
                payload["type"] = type

        try:
            if (isinstance(payload, types.ListType)):
                self.send_to_influx(event_name, payload)
                self.send_to_logstash(event_name, payload)
            else:
                self.send_to_influx(event_name, [payload])
                self.send_to_logstash(event_name, [payload])
        except Exception, e:
            print "Could not send events: " + json.dumps(payload)


    def send_to_influx(self, event_name, events):
        if (isinstance(events, types.ListType) == False):
            raise RuntimeError('Need a list of events')

        keys = events[0].keys()
        points = []
        for event in events: 
            event_keys = event.keys()
            if (event_keys != keys):
                raise RuntimeError('Events have to have the same structure.')

            vals = map((lambda x: event[x]), keys)
            points.append(vals)

        json_body = {"columns": keys, "name": event_name, "points": points}
        # print json_body
        self.influxdb_client.write_points(json.dumps([json_body]))

    def send_to_logstash(self, event_name, events):
        if (isinstance(events, types.ListType) == False):
            raise RuntimeError('Need a list of events')

        for event in events:
            event["eventName"] = event_name

        events_payload = map((lambda x: json.dumps(x)), events)

        self.redis_client.lpush(self.logstash_redis_queue, *events_payload)
