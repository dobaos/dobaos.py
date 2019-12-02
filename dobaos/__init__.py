import json
import time
import redis
from random import random

class Dobaos:
    def get_cast(self):
        msg = self.sub_cast.get_message()
        if msg:
            if msg['type'] == 'message':
                payload = json.loads(msg['data'])
                return payload
    def common_request(self, channel, method, payload):
        msg = {}
        msg['method'] = method
        msg['payload'] = payload
        msg['response_channel'] = self.response_channel.replace("*", str(random()))
        self.pub.publish(channel, json.dumps(msg))
        res = None
        resolved = False
        while not resolved:
            time.sleep(0.01)
            res = self.sub_res.get_message()
            if res:
                if res['type'] == 'pmessage' and res['channel'] == msg['response_channel']:
                    resolved = True
                    parsed = json.loads(res['data'])
                    if parsed['method'] == 'error':
                        raise Exception(parsed['payload'])
                    elif parsed['method'] == 'success':
                        return parsed['payload']                        
    def get_description(self, payload):
        return self.common_request(self.request_channel, 'get description', payload)
    def get_value(self, payload):
        return self.common_request(self.request_channel, 'get value', payload)
    def set_value(self, payload):
        return self.common_request(self.request_channel, 'set value', payload)
    def read_value(self, payload):
        return self.common_request(self.request_channel, 'read value', payload)
    def get_progmode(self):
        return self.common_request(self.request_channel, 'get programming mode', None)
    def set_progmode(self, payload):
        return self.common_request(self.request_channel, 'set programming mode', payload)
    def get_server_items(self):
        return self.common_request(self.request_channel, 'get server items', None)
    def get_version(self):
        return self.common_request(self.service_channel, 'version', None)
    def reset(self):
        return self.common_request(self.service_channel, 'reset', None)
    def __init__(self, host='localhost', port=6379,
                 request_channel='dobaos_req', 
                 service_channel='dobaos_service', 
                 broadcast_channel='dobaos_cast',
                 response_channel='dobapy_res_*'):
        self.request_channel = request_channel
        self.service_channel = service_channel
        self.broadcast_channel = broadcast_channel
        self.response_channel = response_channel

        self.pub = redis.Redis(host=host, port=port)
        self.redisClient = redis.Redis(host=host, port=port)

        self.sub_cast = self.redisClient.pubsub()
        self.sub_res = self.redisClient.pubsub()

        self.sub_cast.subscribe(self.broadcast_channel)
        castSubscribed = False
        while not castSubscribed:
            time.sleep(0.01)
            res = self.sub_cast.get_message()
            if res:
                castSubscribed = res['type'] == 'subscribe'

        self.sub_res.psubscribe(self.response_channel)
        resSubscribed = False
        while not resSubscribed:
            time.sleep(0.01)
            res = self.sub_res.get_message()
            if res:
                resSubscribed = res['type'] == 'psubscribe'
