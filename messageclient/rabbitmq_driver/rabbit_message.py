#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: rabbit_message.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Thu May 12 11:21:19 HKT 2016
#########################################################################

import pika
import uuid
import json


class Message(object):

    def __init__(self, transport, target, msg_body):
        self.transport = transport
        self.target = target
        self.channel = self.transport.channel
        self.correlation_id = None
        self.body = msg_body
        self.response = None

    def send_rpc(self):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.queue_declare(queue=self.target.queue, durable=True)     # queue durable
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(self.on_send_rpc, queue=self.callback_queue)

        properties = pika.BasicProperties(reply_to=self.callback_queue,
                                          correlation_id=self.correlation_id,
                                          content_type='application/json',
                                          delivery_mode=2)          # make message persistent
        self.channel.basic_publish(exchange='',
                                   routing_key=self.target.queue,
                                   properties=properties,
                                   body=json.dumps(self.body))
        while self.response is None:
            self.transport.connection.process_data_events()
        return self.response

    def on_send_rpc(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = json.loads(body)

    def notify(self):
        self.channel.basic_publish(exchange='amq.fanout',
                                   routing_key='',
                                   properties=None,
                                   body=json.dumps(self.body))
        return None


