# -*- coding: utf-8 -*-
"""
Python client for Wildcard Events
"""
import json
import redis
import logging
import datetime
from influxdb import InfluxDBClient

log = logging.getLogger(__name__)


class WildcardPyEventsClient(object):
    def __init__(self,
                 host_name,
                 environment,
                 influx_host='localhost',
                 influx_port=8086,
                 influx_username='app',
                 influx_password='',
                 influx_database='developer-metrics',
                 influx_ssl=False,
                 influx_udp_port=-1,
                 logstash_redis_host='localhost',
                 logstash_redis_port=6379,
                 logstash_redis_db=0,
                 logstash_redis_queue='logstash',
                 force_event_namespace=None,
                 static_data=None,
                 event_name_fmt=None,
                 ):
        self.host_name = host_name
        self.environment = environment
        self.influxdb_client = InfluxDBClient(
            host=influx_host,
            port=influx_port,
            username=influx_username,
            password=influx_password,
            database=influx_database,
            ssl=influx_ssl,
            use_udp=influx_udp_port > -1,
            udp_port=influx_udp_port,
        )
        self.redis_client = redis.StrictRedis(
            host=logstash_redis_host,
            port=logstash_redis_port,
            db=logstash_redis_db,
        )
        self.logstash_redis_queue = logstash_redis_queue
        self.force_event_namespace = force_event_namespace
        self.static_data = static_data if static_data is not None else {}
        self.event_name_fmt = event_name_fmt

    def send(self, event_name, payload):
        self.send_to_influx(event_name, payload)
        self.send_to_logstash(event_name, payload)

    def normalize_payload(self, payload):
        if not isinstance(payload, list):
            payload = [payload]

        for event in payload:
            if self.force_event_namespace:
                for k in event.keys():
                    # skip some keys
                    if k in ('environment', 'host'):
                        continue

                    if not k.startswith(self.force_event_namespace):
                        event[self.force_event_namespace+k] = event.pop(k)

            event.update(self.static_data)
            event.update({'environment': self.environment,
                          'host': self.host_name})

        return payload

    def send_to_influx(self, event_name, events):
        try:
            self._send_to_influx(event_name, events)
        except Exception as e:
            log.warning('Could not send events to influx.\n'
                        'Events: {}\nReason: {}'.format(events, e))

    def send_to_logstash(self, event_name, events):
        try:
            self._send_to_logstash(event_name, events)
        except Exception as e:
            log.warning('Could not send events to logstash.\n'
                        'Events: {}\nReason: {}'.format(events, e))

    def _send_to_influx(self, event_name, events):
        events = self.normalize_payload(events)
        if self.event_name_fmt:
            event_name = self.event_name_fmt.format(event_name)

        # create a timestamp in influx compatible format (ms since epoch, utc)
        epoch = datetime.datetime.utcfromtimestamp(0)
        delta = datetime.datetime.utcnow() - epoch
        timestamp = int(delta.total_seconds()*1000.0 +
                        delta.microseconds/1000.0)

        keys = events[0].keys()
        points = []
        for event in events:
            if (event.keys() != keys):
                raise RuntimeError('Events have to have the same structure.')

            if "time" not in event:
                event["time"] = timestamp

            vals = map(lambda x: event[x], keys)
            points.append(vals)

        json_body = {"columns": keys, "name": event_name, "points": points}
        self.influxdb_client.write_points(json.dumps([json_body]))

    def _send_to_logstash(self, event_name, events):
        events = self.normalize_payload(events)
        if self.event_name_fmt:
            event_name = self.event_name_fmt.format(event_name)

        # create a timestamp in python-beaver compatible format
        now = datetime.datetime.utcnow()
        timestamp = '{}.{:03d}Z'.format(now.strftime('%Y-%m-%dT%H:%M:%S'),
                                        now.microsecond / 1000)
        for event in events:
            if "@timestamp" not in event:
                event["@timestamp"] = timestamp
            event["eventName"] = event_name

        events_payload = map(json.dumps, events)
        self.redis_client.rpush(self.logstash_redis_queue, *events_payload)
