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

    def __init__(self, transport, target, header={}, body={}):
        self.transport = transport
        self.target = target
        self.channel = self.transport.channel
        self.correlation_id = None
        self.header = header
        self.body = body
        self.data = dict()
        self.data['body'] = self.body
        self.data['header'] = self.header
        self.response = None
        self.callback_queue = '%s-callback' % self.target.queue

    def send_rpc(self):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        self.channel.queue_declare(queue=self.target.queue, durable=True)     # queue durable
        self.channel.queue_declare(queue=self.callback_queue, durable=True)
        self.channel.basic_consume(self.on_send_rpc, queue=self.callback_queue)

        properties = pika.BasicProperties(reply_to=self.callback_queue,
                                          correlation_id=self.correlation_id,
                                          content_type='application/json',
                                          delivery_mode=2)          # make message persistent
        self.channel.basic_publish(exchange='',
                                   routing_key=self.target.queue,
                                   properties=properties,
                                   body=json.dumps(self.data))
        while self.response is None:
            self.transport.connection.process_data_events()
        # self.transport.connection.close()
        return self.response

    def on_send_rpc(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = json.loads(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def notify(self):
        self.channel.basic_publish(exchange='amq.fanout',
                                   routing_key='',
                                   properties=None,
                                   body=json.dumps(self.data))
        return None

    def send_request(self):
        self.channel.queue_declare(queue=self.target.queue, durable=True)  # queue durable
        callback_queue = '%s-callback' % self.target.queue
        self.channel.queue_declare(queue=callback_queue, durable=True)
        correlation_id = str(uuid.uuid4())
        properties = pika.BasicProperties(reply_to=callback_queue,
                                          correlation_id=correlation_id,
                                          content_type='application/json',
                                          delivery_mode=2)  # make message persistent

        self.channel.basic_publish(exchange='',
                                   routing_key=self.target.queue,
                                   properties=properties,
                                   body=json.dumps(self.data))


